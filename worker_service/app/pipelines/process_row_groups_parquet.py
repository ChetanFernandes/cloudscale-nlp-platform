import pyarrow.parquet as pq
from logger.logging import setup_logging
from common.config import get_settings
from worker_service.app.celery_worker_io import celery_app
from azure.core.exceptions import AzureError



logger = setup_logging()
settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME


@celery_app.task(name ="worker_service.app.pipelines.process_row_groups_parquet.process_row_group_adlfs", bind = True, autoretry_for = (ConnectionError, TimeoutError, AzureError,), retry_backoff = True, retry_backoff_max = 600, retry_kwargs = {"max_retries": 1})
def process_row_group_adlfs(self,i,parquet_file_path,job_id):
        try:
            
            #parquet_file = get_parquet_file(parquet_file_path)

            #table = parquet_file.read_row_group(i)

            #if table.num_rows == 0:
                #return {"status": "empty", "row_group": i}
            

            chunk_id = f"{job_id}_rowgroup_{i}_pq"

            return {
                        "status": "success",
                        "parquet_file_path": parquet_file_path,
                        "row_group": i,
                        "chunk_id": chunk_id,
                        "source": "parquet",
                    }

           
        except Exception as e:
            logger.exception(f"[JOB={job_id}] Row group {i} processing failed: {e}")
            return {"status": "failed", "row_group": i}
         
            '''
            chunk_hash = hashlib.md5(f"{parquet_file_path}_{i}".encode()).hexdigest()
            chunk_key = f"chunks_to_be_nlp_processed/{job_id}/{chunk_hash}.parquet"

            blob_client = get_blob_client(chunk_key)

            if blob_client.exists(): 
                 return {"status": "success", "key": chunk_key, "row_group": i}
            
            
            buffer = io.BytesIO() # Acts like a file in RAM

            # Write parquet directly (no pandas)
            pq.write_table(table, buffer,  row_group_size=None, version="2.6", compression="snappy")  # writes the data (table) into a Parquet file format and stores it in buffer

            # pq.write_table → “ convert tabel to  → parquet binary format , Create a parquet file in memeory and write everything at once”


            # 🔥 Manual retry ONLY for upload
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    buffer.seek(0)
                    blob_client.upload_blob(buffer, overwrite=False, # “Only one writer can create the blob” 
                                            content_type="application/x-parquet",timeout=60)

                    logger.info(f"[JOB={job_id}] Row group {i} uploaded successfully")
                    return {"status": "success", "key": chunk_key, "row_group": i}
            
                
                except AzureError:

                    # 🔥 STEP 3: Handle race condition
                    if blob_client.exists():
                         logger.info(f"[JOB={job_id}] Chunk already uploaded by another worker: {i}")
                         return {"status": "success", "key": chunk_key, "row_group": i}

                    logger.exception(f"[JOB={job_id}] Upload retry {attempt}/{MAX_RETRIES} for row group {i}")
            
                    if attempt == MAX_RETRIES:
                        logger.exception(f"[JOB={job_id}] Upload failed after retries for row group {i}")
                    
                        return {"status": "failed", "row_group": i}
                
                    time.sleep(2 ** attempt)  # exponential backoff
            '''
        
        '''
        except (ConnectionError, TimeoutError, AzureError):
            logger.warning(f"[JOB={job_id}]  Upload retry for row group {i}")

            if self.request.retries >= self.max_retries:
                  logger.exception(f"[JOB={job_id}] Final failure after retries for row group {i}")
                  return {"status": "failed", "row_group": i}
            raise
        '''
        

          
   

            