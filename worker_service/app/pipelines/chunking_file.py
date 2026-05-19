
from logger.logging import setup_logging
import pyarrow.parquet as pq # PyArrow = a Python library to work with columnar data (including Parquet)
from common.config import get_settings
from common.storage_azure import get_blob,get_azure_fs
from celery import signature
from common.exceptions import TooManystreamErrors

settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME
logger = setup_logging()

CHUNK_SIZE = 500
MAX_RETRIES = 1


def create_streaming_tasks(job_id, object_name, user_selected_columns , final_enc, parquet_file_path):
        '''
        👉 The file is too big to process at once
        👉 So we cut it into smaller pieces (chunks)
        👉 Store each chunk back in S3
        👉 Create tasks so multiple workers (Celery) can process them in parallel
        '''
        final_tasks = []

        try:
            file_type = "csv" if object_name.endswith(".csv") else "parquet"

            if file_type == "csv":
                logger.info("File type is CSV")
                csv_tasks = _process_csv_streaming(object_name, final_enc, job_id, user_selected_columns)
                final_tasks.extend(csv_tasks)

            else:

                logger.info("File type is parquet")
                parquet_tasks = (_process_parquet_streaming(parquet_file_path, job_id, user_selected_columns))
                final_tasks.extend(parquet_tasks)

            return final_tasks
        
        except (TooManystreamErrors):
            raise

        except Exception:
            logger.exception(f"Error occured while connectinng to Azure to read and write files")
            raise
    

# -----------------------------
# CSV STREAMING
# -----------------------------
def _process_csv_streaming(object_name, final_enc, job_id, user_selected_columns):
            try:
                csv_tasks = [] 
                logger.info("Inside function: process csv streaming for splitting csv file into chunks for processing")
                #response = await s3.get_object(Bucket=BUCKET, Key=object_name) # only metadata + stream handle # here we send request to S3 so need await
                #stream = response["Body"] # This line is just assigning a stream object. No data is being read yet. No network transfer happens here
                # This just gives you a stream object (like a file handle)
                # No network call happens here
                # No awiat needed
                
                stream = get_blob(object_name)
                buffer = []
                header = None
                partial_line = ""

                stream_error_count = 0
                row_error_count = 0
                total_stream_attempts = 0
                chunk_count = 0
           
                for chunk in stream.chunks():
                    try:
                        total_stream_attempts += 1

                        text = chunk.decode(final_enc)
                    
                        lines = (partial_line + text).split("\n")
                        partial_line = lines.pop() #last incomplete line

                        for line in lines:
                            try:
                                if header is None:
                                    header = line
                                    continue

                                buffer.append(line)

                                if len(buffer) >= CHUNK_SIZE:
                                    
                                    chunk_count += 1
                                    
                                    chunk_id = f"{job_id}_chunk_{chunk_count}_csv"
                                    
                                    result = {
                                                "header": header,
                                                "rows": buffer,
                                                "enc"  : final_enc,
                                                "chunk_id": chunk_id,
                                                "source": "csv"
                                            }
                                    
                               
                                    csv_tasks.append(
                                            signature(
                                                "worker_service.app.pipelines.column_normalization_nlp_processing.process_chunk_nlp_processing",
                                                args=[result, user_selected_columns, job_id],
                                                queue="cpu"
                                            )
                                            )
                                
                                    buffer = []

                                
                            except Exception:
                                row_error_count += 1
                                logger.exception(f"[JOB={job_id}] Row error (skipped)")
                                continue

                    except Exception:
                        stream_error_count += 1 
                        logger.exception(f"[JOB={job_id}] Stream chunk error or decoding error (skipped)")
                        result = {
                                    "status": "failed",
                                    "source": "csv",
                                    "stream_id": total_stream_attempts}
                          
              
                        csv_tasks.append(
                                            signature(
                                                "worker_service.app.pipelines.column_normalization_nlp_processing.process_chunk_nlp_processing",
                                                args=[result, user_selected_columns, job_id],   
                                                queue="cpu"
                                            )
                                            )

                        if (total_stream_attempts >= 5 and (stream_error_count / total_stream_attempts) > 0.5):
                            raise TooManystreamErrors("More than 50% chunk failed while reading and decoding. Aborting job.")

                        continue
                                 

                # flush last partial line
                if partial_line:
                     buffer.append(partial_line)

                # flush remaining - “If some rows are left (not exactly 5000)”
                if buffer:

                    chunk_count += 1
            
                    chunk_id = f"{job_id}_chunk_{chunk_count}_csv"

                    result = {
                                "header": header,
                                "rows": buffer,
                                "enc"  : final_enc,
                                "chunk_id" : chunk_id,
                                "source": "csv",
                            }

        
                    csv_tasks.append(
                                            signature(
                                                "worker_service.app.pipelines.column_normalization_nlp_processing.process_chunk_nlp_processing",
                                                args=[result, user_selected_columns, job_id],
                                                queue="cpu"
                                            )
                                            )


                logger.info(
                    f"[JOB={job_id}] CSV chunking completed | "
                    f"Row Errors={row_error_count}, "
                    f"Total stream Attempts={total_stream_attempts},"
                    f"Stream Error count={stream_error_count}"
        
                )

                return csv_tasks
            
    
            except TooManystreamErrors:
                logger.exception(f"[JOB={job_id}] Too many stream errors. File likely corrupted")
                raise

            except Exception:
                logger.exception(f"[JOB={job_id}] CSV streaming failed completely")
                raise 

        
# --------------------------------------------------
# PARQUET BATCH (ROW GROUPS)
# --------------------------------------------------

def _process_parquet_streaming(parquet_file_path, job_id, user_selected_columns):
            try:
                logger.info("Inside finction: process parquet streaming for splitting parquet file into chunks for processing")
                # Step 1: Get stream from Azure
                # stream = get_blob(object_name)
                
                #response = await s3.get_object(Bucket=BUCKET, Key=object_name) # only metadata + stream handle # here we send request to S3 so need await
                #stream = response["Body"] # Get strem handle. # Get a pipe connect to S3

                # data = await response["Body"].read() # → downloads entire file .Involves  network + I/O. so await
                # parquet_file = pq.ParquetFile(io.BytesIO(data))
                # Use FULL READ when:
                # File is structured/binary (Parquet, images)
                # File is small enough

                fs = get_azure_fs()
                parquet_file = pq.ParquetFile(f"{CONTAINER_NAME}/{parquet_file_path}",filesystem=fs)

                # It does NOT load the whole file into memory
                # It just: Reads file structure
                # Knows where row groups are
                # Keeps a reference to the file
                # why threading cpu heavy

                num_groups = parquet_file.num_row_groups

                logger.info(f"Parquet file has row groups of -> {num_groups}")

                logger.info("passing each row group to function 'process_row_groups_adlfs' to create a seperate path inblob and pass it to text cleaning process")

                parquet_tasks = []

                
                
                parquet_tasks = [
                                    signature(
                                        "worker_service.app.pipelines.column_normalization_nlp_processing.process_chunk_nlp_processing",
                                        args=[
                                            {
                                                "parquet_file_path": parquet_file_path,
                                                "row_group": i,
                                                "chunk_id": f"{job_id}_rg_{i}",
                                                "source": "parquet"
                                            },
                                            user_selected_columns,
                                            job_id
                                        ],
                                        queue="cpu",
                                    )
                                    for i in range(num_groups)
                                    ]
                    
        
                logger.info(f"[JOB={job_id}] Created {len(parquet_tasks)} tasks")

                return parquet_tasks
                
            except Exception as e:
                logger.exception(f"[JOB={job_id}] Failed preparing parquet tasks: {e}")
                raise



'''
                                if len(buffer) >= CHUNK_SIZE: #Keep collecting rows until chunk is full
                                    #total_chunk_attempts += 1
                                    #try:
                                        #azure_key = upload_CSV_chunk_to_azure_blob(header, buffer, job_id, final_enc, total_chunk_attempts) # “Take 5000 rows and save them as a small file in S3.”
                    
                                        #logger.info(f"[JOB={job_id}] Uploaded chunk → {azure_key}")

                                    result = {
                                                "status": "success",
                                                "header": header,
                                                "rows": buffer,
                                                "chunk_id": total_chunk_attempts,
                                                "source": "csv",
                                                "enc"  : final_enc
                                                }
                                
                                        csv_tasks.append(process_chunk_nlp_processing.s(result, user_selected_columns,job_id))
                                     
                                    except Exception:
                                        chunk_error_count += 1
                                        logger.exception(f"[JOB={job_id}] Chunk upload failed")
                                        result = {
                                                "status": "failed",
                                                "source": "csv",
                                                "chunk_id": total_chunk_attempts,
                                            }

                                        csv_tasks.append(process_chunk_nlp_processing.s(result, user_selected_columns, job_id))


                                        if (total_chunk_attempts >= 10 and (chunk_error_count / total_chunk_attempts) > 0.5):
                                            logger.exception(f"[JOB={job_id}] >50% chunk upload failures. Aborting job")
                                            raise TooManyuploadErrors("More than 50% chunk failures. Aborting job.")
                                
                                        
                                    finally:
                                         buffer = []
                                '''


# --------------------------- One more method using temp file-----------------------------
'''
Use if youre using temp file
# Step 2: Stream to temp file
with tempfile.NamedTemporaryFile(delete = False) as tmp:
    while True:
        chunk = await stream.read(1024 * 1024)  # 1MB at a time
        # 👉 Use STREAM when:
        # File is large
        # File is text-based (CSV, logs)
        if not chunk:
            break
        tmp.write(chunk)

    temp_path = tmp.name  #store where the file is saved


        
finally:
    if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

            
            
        
# --------------------------- One more method-----------------------------

for i in range(parquet_file.num_row_groups):  # A Parquet file is already internally split into chunks called row groups. ❌ instead of reading whole file , you read one chunk (row group) at a t
    table = parquet_file.read_row_group(i) # PyArrow reads that row group from disk → into RAM. # table ->  It is a PyArrow Table (not pandas yet)
    if table.num_rows == 0:
            continue

    logger.info(f"Row group {i}, rows={table.num_rows}")

    # df = table.to_pandas() # Converts Arrow format → Pandas DataFrame

    # if you want to split further use this:-
        for start in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[start:start + CHUNK_SIZE]

                buffer = io.StringIO() #create in memeory file
                chunk.to_parquet(buffer, index=False) # convert chunk to csv and writes csv data to buffer
                buffer.seek(0) # if you forget this you will send empty data 

                s3_key = await upload_buffer_async(s3,buffer,f"chunks/{job_id}/{uuid.uuid4().hex}.csv")

                tasks.append(process_chunk.s(s3_key, columns))


    df.to_parquet(buffer, index=False)

    buffer = io.BytesIO() # creates a binary buffer file in RAM becuase parquet in bainry format
    pq.write_table(table, buffer,compression="snappy") # Taking that chunk and saving it as a new Parquet file”
    buffer.seek(0) # if you forget this you will send empty data 

    s3_key = await upload_buffer_async(s3,buffer,f"chunks/{job_id}/{uuid.uuid4().hex}.parquet")

    tasks.append(process_chunk.s(s3_key, columns))
    os.remove(temp_path)                  


------------------------------------Using file system----------------------------
         

 This is one more method not to use temp file

import pyarrow.dataset as ds
import pyarrow.fs as fs

# For MinIO
s3_fs = fs.S3FileSystem(
access_key="minioadmin",
secret_key="minioadmin",
endpoint_override="localhost:9000",
scheme="http"
)

dataset = ds.dataset(
f"{BUCKET}/{object_name}",
filesystem=s3_fs,
format="parquet"
)

for batch in dataset.to_batches(batch_size=CHUNK_SIZE):
df = batch.to_pandas()

buffer = io.StringIO()
df.to_csv(buffer, index=False)
buffer.seek(0)

s3_key = await upload_buffer_async(
    s3,
    buffer,
    f"chunks/{job_id}/{uuid.uuid4().hex}.csv"
)

tasks.append(process_chunk.s(s3_key, columns))


#---------------------Using local disk-------------------------------------
 
try:
    logger.info("Inside function reading csv/excel file and converting them to data frame")
    chunk_size = 5000
    tasks = [] # Used to accumalate the tasks

            # Convert Excel → CSV
    if file_path.endswith(".xlsx"):
        csv_path = file_path.replace(".xlsx", ".csv")
        Xlsx2csv(file_path).convert(csv_path)
        file_path = csv_path


    for chunk in pd.read_csv(file_path, chunksize=chunk_size, encoding=final_enc, na_values=["NA", "Unknown", "-", " "],
                                    sep=None, skipinitialspace=True, skip_blank_lines=True, on_bad_lines="skip",engine="python"):
        try:
            
            # chunk becomes Dataframe of 5000 rows
            chunk_id = uuid.uuid4().hex

            # step 1 - Save locally
            local_path = f"/tmp/{chunk_id}.csv"
            chunk.to_csv(local_path,index = False)
            
            
            # Upload to S3
            s3_path = upload_file(local_path)

            os.remove(local_path)
        

            # return celery signature
            tasks.append(process_chunk.s(job_id, s3_path, columns))  # This creates a Celery signature object.
            # Here you are not sending task. You are creating task blue print
            # tasks = [
                # process_chunk(job1,chunk1),
                # process_chunk(job1,chunk2),
                # process_chunk(job1,chunk3)
                # ]

            # Celery workflows like: a. group b. chord c. chain need task signatures, not running tasks.
            # Example: from celery import chord
            # chord(tasks)(aggregate_chunks.s(job_id))
            # Celery will then:
            # start all tasks
            # wait for them to finish
            # then run aggregation

            # task = process_chunk.delay(job_id, chunk_path, columns) # You send the task immediately to redis queue where celelry Worker picks tasks from redis queue and  starts processing it. Celery returns a task ID.
        except Exception:
            logger.exception(f"Error encountered while processing file {file_path}")
            continue


    return tasks
                
except Exception:
    logger.exception(f"Error while processing file {file_path}")
    raise
''' 