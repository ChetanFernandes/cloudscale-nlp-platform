from worker_service.app.celery_worker_io import celery_app
from data_layer.database import SessionLocal
from celery import group, chord,chain
from worker_service.app.pipelines.chunking_file import create_streaming_tasks
from data_layer.repositories.job_repository import JobRepository
#from  worker_service.app.tasks.aggregate_partial import aggregate_chunks
from celery import signature
from logger.logging import setup_logging
import pandas as pd
#from worker_service.app.tasks.zipping_final_path_uploading import zip_upload
from azure.core.exceptions import AzureError
from common.exceptions import TooManyuploadErrors,TooManystreamErrors
logger =  setup_logging()


@celery_app.task(bind = True, autoretry_for=(ConnectionError, TimeoutError, AzureError), retry_backoff = True, 
                 retry_backoff_max = 600, retry_kwargs = {"max_retries": 1},
                 dont_autoretry_for=(TooManyuploadErrors,TooManystreamErrors))
def process_selected_columns(self, job_id, object_name, user_selected_columns, final_enc, parquet_file_path):
    db = SessionLocal()
    try:
        logger.info("Inside function 'process_selected_column' for nlp processing")
        job = JobRepository.get_job(db, job_id)

        if job.status in ["nlp_processing_failed_upload_errors","nlp_processing_failed_stream_errors","system_failed"]:
            logger.info(f"[JOB={job_id}] Already processed or in progress. Skipping.")
            return

        tasks = create_streaming_tasks(job_id, object_name, user_selected_columns , final_enc, parquet_file_path)

            
        logger.info(f"Created {len(tasks)} tasks for object {object_name}")

    # 4 ----------------------chord (MAP -> Reduce) -> process chunks then aggregate--------------------------------"
        # This defines what should run after all chunks finish. After chunk finish run function aggregate_chunks
        logger.info("Inside Map and reduce section - Here celery send tasks to redis queue. Celery worker picks tasks from redis")
        
        if tasks:

            header = group(tasks)

            callback = signature("worker_service.app.tasks.aggregate.aggregate_chunks",kwargs={"job_id": job_id}, queue="aggregation")

            chord(header, callback).apply_async()

        else:

            chain(
                    signature(
                        "worker_service.app.tasks.aggregate.aggregate_chunks",
                        kwargs={"job_id": job_id}, queue="aggregation"
                    ),
                    signature(
                        "worker_service.app.tasks.zipping_final_path_uploading.zip_upload",
                        kwargs={"job_id": job_id}, queue="aggregation"
                    )
                ).delay()


    
        # Here celery send tasks to redis queue. Celery worker picks tasks from redis
        # chord(header_tasks)(callback_task) - header tasks → list of task signatures - callback task → also a task signature
        # Chord will run all tasks in parallel and once its done the aggregate_chunks will run
        # Clelery will automatically pass results of each header_tasks to callback_task
        # aggregate_chunks(["chunk1.csv","chunk2.csv","chunk3.csv"], job.id)
        

    except TooManystreamErrors:
        job = JobRepository.update_job_status(db, job_id,"nlp_processing_failed_stream_errors")
        raise

    except (ConnectionError, TimeoutError, AzureError) as e:
        logger.warning(f"[JOB={job_id}] Retryable error: {e}")

        if self.request.retries >= self.max_retries:
            logger.exception(f"[JOB={job_id}] Final failure after retries. Column processing failed: {e}")
            job = JobRepository.update_job_status(db, job_id,"system_failed")


        raise 

    except Exception:
        logger.exception(f"[JOB={job_id}] Column processing failed")
        job = JobRepository.update_job_status(db, job_id,"system_failed")
        raise

    finally:
        if db:
            db.close()