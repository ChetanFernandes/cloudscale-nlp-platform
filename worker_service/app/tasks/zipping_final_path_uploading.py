from data_layer.database import SessionLocal
from worker_service.app.celery_worker_agg import celery_app
import os
import zipfile
import pyarrow.parquet as pq
from common.storage_azure import get_blob_client, get_azure_fs
from logger.logging import setup_logging
from common.config import get_settings
logger = setup_logging()
from data_layer.repositories.job_repository import JobRepository
from azure.core.exceptions import AzureError
import pyarrow as pa
import pyarrow.csv as pacsv 
from datetime import datetime, timezone


TEMP_DIR = "D:\\zip_tmp"
os.makedirs(TEMP_DIR, exist_ok = True)

settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME
logger = setup_logging()


celery_app.config_from_object("worker_service.app.celery_worker_agg")

db, job = None, None
zip_path = None


'''
def convert_lists_to_string(table):
    new_columns = []

    for col_name in table.schema.names:
        column = table[col_name]

        if pa.types.is_list(column.type):
            values = column.to_pylist()
            cleaned_values = []
            for v in values:
                if isinstance(v, list):
                    # 🔥 flatten nested lists if needed
                    if any(isinstance(i, list) for i in v):
                        v = [item for sublist in v for item in (sublist if isinstance(sublist, list) else [sublist])]

                    cleaned_values.append(" , ".join(map(str, v)) if v else "No data found")

                elif v is None:
                    cleaned_values.append("")

                else:
                    cleaned_values.append(str(v))

            column = pa.array(values, type=pa.string())

        new_columns.append(column)

    return pa.Table.from_arrays(new_columns, names=table.schema.names)
'''


@celery_app.task(name = "worker_service.app.tasks.zipping_final_path_uploading.zip_upload",bind=True, 
                 autoretry_for=(ConnectionError, TimeoutError, AzureError,), retry_backoff=True, retry_kwargs={"max_retries": 1})
def zip_upload(self, result, job_id):

    logger.info("Inside Zip function")

    if isinstance(result, dict):
        final_path = result.get("final_key")
        #processed_now = result.get("processed_chunks", [])
    else:
        final_path = result["final_key"]
        #processed_now = []
        
    try:

        db = SessionLocal()
        job = JobRepository.get_job(db,job_id)

        if not job:
            logger.error(f"[JOB={job_id}] Job not found ")
            return
        
        if job.status in ["Zipping_failed","final_zip_file_uploaded_Azure"]:
             logger.info(f"[JOB={job_id}] Zipping process already completed. skipping")
             return
           

        zip_path = os.path.join(TEMP_DIR, f"{job_id}.zip")

        fs = get_azure_fs()  # Connect to Azure Parquet. Returns filesystem object (likely adlfs or similar)
        parquet_file = pq.ParquetFile(f"{CONTAINER_NAME}/{final_path}", filesystem=fs) # Opens Parquet file without loading it fully
        
        batches = parquet_file.iter_batches(batch_size=50000) # Creates generator: Reads Parquet chunk-by-chunk
        
        first_batch = next(batches, None)

        if first_batch is None:
            raise ValueError("Parquet file is empty")

        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as z: # Opens ZIP file for writing
            with z.open("output.csv","w") as zip_file: # creates a file inside ZIP
                #schema = parquet_file.schema_arrow  # Gets column structure (names + types)
                #writer = pc.CSVWriter(zip_file, schema) # Creates CSV writer: Writes directly into ZIP stream. Uses schema for column order
                #writer = None
                # write first batch
                
           
                table = pa.Table.from_batches([first_batch])
                #table = convert_lists_to_string(table)

                writer = pacsv.CSVWriter(zip_file, table.schema)
                writer.write_table(table)
                
        
                for batch in batches:
                    table = pa.Table.from_batches([batch])  # Converts batch → Arrow Table → writes to CSV
                    #table = convert_lists_to_string(table)
                    writer.write_table(table)  # Written to CSV inside ZIP
                writer.close() # Flushes remaining buffer. Ensures CSV is complete

        zip_key = f"final_zip_to_be_downloaded/{job_id}/final.zip"
        
        blob_client = get_blob_client(zip_key)

        if blob_client.exists():
            logger.info("Zip already exists, skipping creation")
            job.status = "final_zip_file_uploaded_Azure"
            job.final_zip_path = zip_key
            if job.status == "final_zip_file_uploaded_Azure":
                job.completed_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(job)
            return

        with open(zip_path, "rb") as f: # Open ZIP file in binary mode
            try:
                blob_client.upload_blob(
                    f,
                    overwrite=False,
                    content_type="application/zip",
                    timeout=120,
                )
    
            except AzureError:
                if blob_client.exists():
                    logger.info(f"[JOB={job_id}] final Zip file already uploaded by another worker")   
                else:
                    raise

        # -----------------------------
        # 8. UPDATE DB
        # -----------------------------
        job.status = "final_zip_file_uploaded_Azure"
        job.final_zip_path = zip_key
        
        job.completed_at = datetime.now(timezone.utc)

        db.flush()

        if job.created_at and job.completed_at:
            job.total_time_taken = round((job.completed_at - job.created_at).total_seconds()/60, 2 )
    
        db.commit()

        db.refresh(job)

        logger.info(f"File zipped and uploaded in Azure - {zip_key}")

 
    except (ConnectionError, TimeoutError, AzureError):
        logger.warning(f"[JOB={job_id}] Zipping failed for {final_path}. Retrtying")

        if self.request.retries >= self.max_retries:
            logger.exception(f"[JOB={job_id}] Zipping failed for {final_path}")
            JobRepository.update_job_status(db, job_id,"Zipping_failed")
        raise 


    except Exception:
        logger.exception(f"[JOB={job_id}] Zipping failed for {final_path}")
        JobRepository.update_job_status(db, job_id,"Zipping_failed")
        raise 


    finally:
        if db:
            db.close()

        if zip_path and os.path.exists(zip_path):
            try:
                os.remove(zip_path)
            except:
                pass

  

