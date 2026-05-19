# Create First Celery Task

from worker_service.app.celery_worker_io import celery_app
from data_layer.database import SessionLocal
from worker_service.app.processor.convert_excel_parquet import convert_excel_to_parquet
from data_layer.repositories.job_repository import JobRepository
from logger.logging import setup_logging
from common.config import get_settings
from common.exceptions import HeaderError

logger = setup_logging()
settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME



@celery_app.task(name ="worker_service.app.tasks.process_file.process_file_task", bind = True, 
                autoretry_for=(ConnectionError, TimeoutError), 
                retry_backoff = True, retry_backoff_max = 600, retry_kwargs = {"max_retries": 1})
def process_file_task(self,job_id:str, object_name:str):
    """Inside worker_service function"""
    
    db = SessionLocal() # Think of it like opening a temporary conversation with the database. Worker → "Hello Postgres, I want to read/write data"
  
    try:
        logger.info(f"[JOB={job_id}] -  Worker started")
        logger.info(f"[JOB={job_id}]  - If file is excel, it will be converted to parquet")

        job = JobRepository.get_job(db, job_id)

        if not job:
            JobRepository.update_job_status(db, job_id, "job_not_found")
            raise ValueError(f"Job {job_id} not found")
        
 
        if job.status in ["Invalid_file","Excel_parquet_conversion_failed","job_not_found","columns_extracted","no_columns_found"]:
            logger.info(f"[JOB={job_id}] - Already completed. Skipping.")
            return

        # 1 Mark job as processing
        job.status = "job_picked_worker_column_extraction"
        db.commit()

        
    # 1 ------------------ 2. Coverting excel file to parqet. If file is csv keep as it is----------
       
        logger.info(f"[JOB={job_id}]  -  Checking if file : {object_name} is excel or not")

        if not object_name:
            raise ValueError("object_name is required for file processing")

        if object_name.endswith((".xlsx", ".xls")):

            logger.info(f" [JOB={job_id}] - File is excel. Convert to parquet")

            excel_to_parqet_path = convert_excel_to_parquet(object_name,job_id)
            
            logger.info(f"Reading parquet path: {excel_to_parqet_path}")

            return {
                "job_id": job_id,
                "file_type": "parquet",
                "file_path": excel_to_parqet_path
            }
        
        else:
            logger.info(f"[JOB={job_id}] CSV detected")

            return {
                "job_id": job_id,
                "file_type": "csv",
                "file_path": object_name
            }


    except HeaderError:
        logger.exception(f"[JOB={job_id}] No Header found. Invalid excel file")
        JobRepository.update_job_status(db, job_id, "Invalid_file")
        return None

    except (ConnectionError, TimeoutError) as e:
        logger.warning(f"[JOB={job_id}] Retryable error: {e}")

        if self.request.retries >= self.max_retries:
            logger.exception(f"[JOB={job_id}] Final failure after retries: {e}")
            JobRepository.update_job_status(db, job_id, "Excel_parquet_conversion_failed")

        raise 
    
    except Exception as e:
        logger.exception(f"[JOB={job_id}] Non-retryable failure: {e}")
        JobRepository.update_job_status(db, job_id, "Excel_parquet_conversion_failed")
        raise

    finally:
        if db:
            db.close()
    


'''

    rows = []
    with open(file_path) as f: 
        for line in f:
            rows.append(int(line.strip()))

    chunk_size = 1000

    chunks = [rows [i:i+chunk_size] for i in range(0, len(rows), chunk_size)]

    print(f"Created {len(chunks)} chunks")


    header = [process_chunk.s(chunk) for chunk in chunks]

    callback = aggregate_results.s(job_id) # This defines what should run after all chunks finish. Define final task

    chord(header)(callback) # Execute workflow

'''
''' 

bind=True

This gives access to the task instance (self)

Celery needs this internally for retries.
-----------------------------
autoretry_for=(Exception,)

If any exception occurs:

Celery automatically retries the task
-------------------------------
retry_backoff=True

This enables exponential backoff.

Retry delay grows automatically.
-----------------------------------
retry_backoff_max=600

Maximum wait time:

600 seconds = 10 minutes
--------------------------

max_retries=5

Celery will retry maximum 5 times.

If still failing:

task = failed permanently
'''