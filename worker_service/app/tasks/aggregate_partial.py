from worker_service.app.celery_worker_agg import celery_app
from logger.logging import setup_logging
import tempfile, os
import pyarrow.parquet as pq
from common.config import get_settings
from common.storage_azure import get_blob_client, get_azure_fs
from azure.core.exceptions import AzureError
from functools import lru_cache
import hashlib, time
from celery import group,chord
from worker_service.app.tasks.aggregate_final import aggregate_final_
from common.exceptions import NoValidChunksError, TooManyFailedChunksError
from data_layer.repositories.job_repository import JobRepository
from data_layer.database import SessionLocal
from worker_service.app.tasks.zipping_final_path_uploading import zip_upload
from celery import group, chord,chain

settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME
logger = setup_logging()

celery_app.config_from_object("worker_service.app.celery_worker_agg")

db = None
max_retry = 3
retry = 0

@lru_cache(maxsize=50)
def get_parquet_file(path):
    fs = get_azure_fs()
    return pq.ParquetFile(f"{CONTAINER_NAME}/{path}",filesystem=fs)

@celery_app.task(name="worker_service.app.tasks.aggregate.aggregate_partial_", bind=True, autoretry_for=(ConnectionError, TimeoutError,AzureError), retry_backoff=True, retry_kwargs={"max_retries": 1})
def aggregate_partial_(self, chunk_batch, job_id):

    logger.info(f"[JOB={job_id}] Starting partial aggregation for {len(chunk_batch)} chunks")

    TEMP_DIR = "D:\\aggregation_tmp"
    os.makedirs(TEMP_DIR, exist_ok=True)

    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=TEMP_DIR)
    temp_path = temp_file.name
    temp_file.close()

    writer = None
    base_schema = None

    try:
        for chunk in chunk_batch:
            try:
                parquet_file = get_parquet_file(chunk)

                for rg in range(parquet_file.num_row_groups):
                    try:
                        table = parquet_file.read_row_group(rg)

                        if writer is None:
                            base_schema = table.schema
                            writer = pq.ParquetWriter(
                                temp_path,
                                base_schema,
                                version="2.6",
                                compression="snappy"
                            )
                        else:
                            if table.schema != base_schema:
                                raise ValueError(f"Schema mismatch in {chunk}")
                            
                          # ✅ write only if everything is successful
                        writer.write_table(table)
                            
                    except Exception:
                        logger.exception(f"Error during partial aggregation for {chunk} for row group {rg}")
                        continue

            except Exception:
                logger.warning(f"[JOB={job_id}] Error during partial aggregation: {chunk}")
                continue

        if writer:
            writer.close()
            

        # Upload partial result
        hash_val = hashlib.md5("".join(sorted(chunk_batch)).encode()).hexdigest()

        partial_key = f"partial/{job_id}/{hash_val}.parquet"

        blob_client = get_blob_client(partial_key)

        if blob_client.exists():
            logger.info(f"[JOB={job_id}] Partial_aggregation file already exists")
            return {"status": "success", "key": partial_key}
        
        for attempt in range(1, max_retry + 1):
            try:
                with open(temp_path, "rb") as f:
                    blob_client.upload_blob(f, overwrite=False, content_type="application/x-parquet", timeout=120)
                    logger.info(f"[JOB={job_id}] Partial aggregation completed → {partial_key}")
                    return {"status": "success", "key": partial_key}
                
            except Exception as e:
                logger.warning(f"[JOB={job_id}] Upload attempt {attempt} failed for {partial_key}")

                if blob_client.exists():
                    logger.info(f"[JOB={job_id}] Partial aggregation completed already by another worker")
                    return {"status": "success", "key": partial_key}
       
    
                if attempt == max_retry:
                    logger.exception(f"[JOB={job_id}] Upload failed after retries for {partial_key}")
                    return {"status": "failed"}
                    
        
                time.sleep(2 ** attempt)

    except ValueError as e:
        logger.error(str(e))
        return {"status": "failed"}

    except (ConnectionError, TimeoutError, AzureError):
        logger.warning(f"[JOB={job_id}] Retryable error during partial aggregation")

        if self.request.retries >= self.max_retries:
            logger.exception(f"[JOB={job_id}] Partial_Aggregation_Failed for {chunk_batch}")
            return {"status": "failed"}

        raise
                 
    except Exception as e:
        logger.exception(f"[JOB={job_id}] Partial aggregation failed for {chunk_batch}: {e}")
        return {"status": "failed"}
       

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


@celery_app.task(name="worker_service.app.tasks.aggregate.aggregate_chunks",bind=True, retry_backoff=True)
def aggregate_chunks(self, processed_chunk_path, job_id): 
    
    try:
        logger.info("Inside Aggregate_chunks_function")

        valid_paths = []
        failed = 0

        for item in processed_chunk_path:
            try:
                if isinstance(item,dict):
                    if item.get("status") == "success":
                        valid_paths.append(item["key"])
                    else:
                        failed += 1

                else:
                    valid_paths.append(item)

            except Exception:
                logger.exception(f"Item processing failed {item}")
                continue


        total = len(processed_chunk_path) 

        success_count = len(valid_paths)

        logger.info( f"[JOB={job_id}] Aggregation input → success={success_count}, failed={failed}, total={total}" )

        
        if total > 0: 
            failure_rate = failed / total 
            if failure_rate > 0.5: 
                raise TooManyFailedChunksError( f"[JOB={job_id}] Too many failed chunks ({failure_rate*100:.2f}%)")
            
        if not valid_paths:
            logger.warning(f"[JOB={job_id}] No valid chunks to aggregate")
            raise NoValidChunksError(f"[JOB={job_id}] No valid chunks")
            

        def split_chunks(valid_paths, batch_size):
            for i in range(0, len(valid_paths), batch_size):
                yield valid_paths[i:i + batch_size]

        batch_size = 5

        batches = list(split_chunks(valid_paths, batch_size))


        chord(
            group(aggregate_partial_.s(batch, job_id) for batch in batches),
            chain(
                aggregate_final_.s(job_id=job_id),
                zip_upload.s(job_id=job_id)
            )
            ).apply_async()

        return {"status": "started", "batches": len(batches)}
    

    except TooManyFailedChunksError as e:
        logger.error(str(e))
        db = SessionLocal()
        try:
            JobRepository.update_job_status(db, job_id, "Aggregation_Failed")
        finally:
            db.close()

        raise
    
    except NoValidChunksError as e:
        logger.error(str(e))
        db = SessionLocal()
        try:
            JobRepository.update_job_status(db, job_id, "Aggregation_Failed")
        finally:
            db.close()

        raise
    
    except Exception:
        logger.exception("Chunks_splitting for aggrrgation failed")
        db = SessionLocal()
        try:
            JobRepository.update_job_status(db, job_id, "Aggregation_Failed")
        finally:
            db.close()

        raise


   
    
'''
async def _aggregate_chunks_streaming(results,job_id):
    async with aioboto3.client("s3") as s3:
        TEMP_DIR = "/mnt/aggregation_tmp"
        temp_file = tempfile.NamedTemporaryFile(delete=False,dir=TEMP_DIR)
        logger.info(f"Temp file size: {os.path.getsize(temp_path)}")
        temp_path = temp_file.name
        temp_file.close()

        writer = None

        try:
            for idx, key in enumerate(results):
                #-----READ CHUNK---------------
                # response = await s3.get_object(Bucket = BUCKET, Key = key)
                # data = await response["Body"].read()
                parquet_file = pq.ParquetFile(f"s3://{BUCKET}/{key}",filesystem=fs)


                for rg in range(parquet_file.num_row_groups):
                    #table = pq.read_table(io.BytesIO(data)) # converting raw chunks into a structured data

                    table = parquet_file.read_row_group(rg)

                    #------INIT WRITER------------(ONCE)

                    if writer is None:
                        writer = pq.ParquetWriter(temp_path,table.schema,compression="snappy") # creates final file structure

                    #---------Append Data---------------
                    writer.write_table(table)   # appends each chunk into final file

                await s3.delete_object(BUCKET = BUCKET, Key = key)


            # 6. CLOSE WRITER
            # -----------------------------
            if writer:
                writer.close()



            # -----------------------------
            # 7. UPLOAD FINAL FILE
            # -----------------------------
            with open(temp_path, "rb") as f: 
                # rb becuse parquet is binary file
                # temp_path = address of the file 
                # open(...) = actually opening the file 
                # f = giving S3 access to read it 

                final_key = f"final/{job_id}/{uuid.uuid4().hex}.parquet"

                await s3.put_object(
                    Bucket=BUCKET,
                    Key=final_key,
                    Body=f
                )

            # -----------------------------
            # 8. UPDATE DB
            # -----------------------------
            db = SessionLocal()
            JobRepository.update_job_status(db, job_id, "completed")
            db.close()

            return final_key

        except Exception:
            logger.exception("Streaming aggregation failed")
            raise

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


''' 




#-----------------------------FOR CSV FILE streaming dorectly from S3--------------------

                    
# Below code is used when you want to merge large CSV files directly in S3 using streaming, without creating any local 
# file — unlike earlier code which builds the file locally first.

''' 
import aioboto3

    BUCKET = "test" 
    PART_SIZE = 5 * 1024 * 1024  # 5MB (S3 minimum)    # Each upload part must be at least 5MB (S3 rule)


    async def _aggregate_chunks_async(self, results, job_id):

    async with aioboto3.client("s3") as s3: # Open connection to S3 (async)

    final_key = f"final/{job_id}.csv" # Where final merged file will be stored

    # -----------------------------
    # INIT MULTIPART UPLOAD
    # -----------------------------
    mp = await s3.create_multipart_upload(  # Tell S3: “I’m going to upload this file in multiple parts
    Bucket=BUCKET,
    Key=final_key,
    ContentType="text/csv"
    )

    upload_id = mp["UploadId"] # S3 gives you an ID to track this upload

    parts = []  # track  each uploaded part
    part_number = 1 # order of parts

    buffer = []  # Temporary storage: collect lines before uploading
    buffer_size = 0 # track size

    first_chunk = True

    try:
    # -----------------------------
    # LOOP OVER CHUNKS
    # -----------------------------
    for idx, s3_key in enumerate(results):

        response = await s3.get_object(Bucket=BUCKET, Key=s3_key) # Get file from S3
        stream = response["Body"] # Don’t download fully — stream it

        line_index = 0 # Used to detect header row

        async for line in stream.iter_lines(): # Read CSV row-by-row (memory efficient)
            # here line is NOT a string, it is: b"A,10" . i.e., bytes, not text

            # skip header for all except first chunk
            if not first_chunk and line_index == 0:
                line_index += 1
                continue

            buffer.append(line + b"\n") # \n is new line. The b means “bytes”. So this is newline in binary form
            # “Take this row and add a newline at the end, then store it”
            # suppose line = b"A,10", then line + b"\n" becomes b"A,10\n"
            # Why we need this? 
            # Because when you later do: Body = b"".join(buffer) You want:
            # A,10, B,20
            # not A,10B,20C,30

            buffer_size += len(buffer[-1])  # “Keep track of how much data (in bytes) we have collected so far”
            # len(buffer[-1]) - gives size of last bites

            line_index += 1

            # -----------------------------
            # UPLOAD PART
            # -----------------------------
            if buffer_size >= PART_SIZE:

                part = await s3.upload_part(
                    Bucket=BUCKET,
                    Key=final_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=b"".join(buffer)
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": part["ETag"]
                })

                part_number += 1
                buffer = []
                buffer_size = 0

        first_chunk = False

        # -----------------------------
        # PROGRESS UPDATE
        # -----------------------------
        progress = int((idx + 1) / len(results) * 100)

        self.update_state(
            state="PROGRESS",
            meta={
                "progress": progress,
                "processed_chunks": idx + 1,
                "total_chunks": len(results)
            }
        )

    # -----------------------------
    # FINAL BUFFER FLUSH
    # -----------------------------
    if buffer:
        part = await s3.upload_part(
            Bucket=BUCKET,
            Key=final_key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=b"".join(buffer)
        )

        parts.append({
            "PartNumber": part_number,
            "ETag": part["ETag"]
        })

    # -----------------------------
    # COMPLETE UPLOAD
    # -----------------------------
    # “Hey S3, I’ve uploaded all parts — now combine them into ONE final file”
    await s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=final_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts}
    )
    # 👉 S3 will:
    # Take all parts
    # Arrange them in order (PartNumber)
    # Stitch them together
    # Create the final file

    # -----------------------------
    # MARK JOB COMPLETE
    # -----------------------------
    db = SessionLocal()
    JobRepository.update_job_status(db, job_id, "completed")
    db.close()

    return final_key

    except Exception:

    # -----------------------------
    # ABORT ON FAILURE
    # -----------------------------
    await s3.abort_multipart_upload(
        Bucket=BUCKET,
        Key=final_key,
        UploadId=upload_id
    )

    raise

'''