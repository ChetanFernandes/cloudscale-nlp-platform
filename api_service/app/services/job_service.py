# Now we connect this repository to the Service Layer.

 #We will use this to send tasks to Redis.
from sqlalchemy.orm import Session
from data_layer.repositories.job_repository import JobRepository
from api_service.app.core.celery_app import celery_app 
from logger.logging import setup_logging
logger = setup_logging()
from typing import Optional
from celery import chain, signature


class JobService:

    @staticmethod
    def create_job(db:Session, object_name : Optional [str] = None, text: Optional[str] = None , idempotency_key: str = None): # Service asks repository to create a job.
        """ The job service function calls job repository to create job in DB """
   
        logger.info("Inside job service function which call job repositiry to create job in DB")

        # check existing job
        if idempotency_key:
            logger.info("Check if job is existing")
            existing_job = JobRepository.get_by_idempotency_key(db,idempotency_key)

            if existing_job:
                logger.info(f"Job already exists -> {existing_job}")
                return existing_job


        logger.info(f"Job doesnt exists. Create new job in Database")

        # Create job in database
        job = JobRepository.create_job(db, object_name, text, idempotency_key)

        logger.info(f"Job id is {job.id}")
        # here job is object, represneting each row


        # 2️⃣ send task to worker through Redis

        logger.info(f"Sending job to Redis Queue")

        if object_name:
            chain(
                    celery_app.signature(
                                "worker_service.app.tasks.process_file.process_file_task",
                                 args=[job.id, object_name],queue="io"),

                    celery_app.signature(
                              "worker_service.app.tasks.column_extraction.extract_columns_task", 
                                queue="io")

                    ).apply_async()
        else:
            celery_app.send_task("worker_service.app.tasks.process_text.text_nlp_processing",args=[job.id, text],queue="cpu")

        return job
        
       

    
    @staticmethod
    def get_job_status(db:Session,job_id:str):  # Returns job status in a clean format.

        """ The job service function calls job repository to get job status"""
        
        logger.info("Inside job service function which call job repositiry to status of job")

        job = JobRepository.get_job(db,job_id) 

        if not job:
            return None
        
        
        return {
            "job_id": job.id,
            "status": job.status,
            "created_at": job.created_at,
            "completed_at": job.completed_at,
        }
        

    @staticmethod
    def get_job_by_id(db: Session, job_id: str):
        """ The job service function calls job repository to fetch the job"""

        logger.info("The job service function calls job repository to fetch the job")

        return JobRepository.get_job(db, job_id)
    
    
    @staticmethod
    def start_job_processing(db:Session, job_id:str, user_selected_columns:list):
        """ The job service function calls job repository to store user selected column in DB and start processing nlp_pipeline"""

        logger.info("Inside job service function which call job repositiry to store user selected column")

        job =  JobRepository.job_processing(db, job_id, user_selected_columns)

        if not job:
            return None

        logger.info(f"Sending job to Redis Queue with user selected columns")
        #process_selected_columns.delay(job.id, job.file_name, user_selected_columns,job.encoding, job.parquet_file_path)
        celery_app.send_task("worker_service.app.tasks.task_distribution.process_selected_columns", args=[job.id, job.file_name, user_selected_columns, job.encoding, job.parquet_file_path], queue="io")

        return job


   
    
   
        
   

    
# celery_app.send_task(...)
# This does not run the task.
# Instead it sends a message to Redis queue.
# Inside Redis a message appears like: process_file(job_id, file_name)
# Worker will pick it up.