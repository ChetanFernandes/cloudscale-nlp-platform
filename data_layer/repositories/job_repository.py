# Think of repository as a database manager.
# API → Repository → DB
# Worker → Repository → DB

from data_layer.models.job_model import Job
from sqlalchemy.orm import Session
from logger.logging import setup_logging
from typing import Optional

logger = setup_logging()


class JobRepository:

    @staticmethod
    def create_job(db:Session,object_name:Optional[str] = None, text: Optional[str] = None, idempotency_key: str = None):
        ''' This is job repository fucntion. This function will create new job in postgress DB'''
        try:
            logger.info("Inside job repository function which create job in DB")
            job = Job(file_name = object_name, text = text, status = "job_created_DB", idempotency_key = idempotency_key)
            db.add(job) # "Prepare this object to be inserted into the database"
            db.commit() # Now the row is stored in the database.
            db.refresh(job) # This reloads the object from the database.
            return job
        
        except Exception:
            logger.exception("Error while creating job in repository layer")
            db.rollback()
            raise

    
    @staticmethod
    def get_job(db:Session,job_id:str): # Used by API when user checks job status.
        ''' This is job repository fucntion. This function will check user job status'''
        try:
            
            logger.info("Inside job repository function checking job status")
            return db.query(Job).filter(Job.id == job_id).one_or_none()
        
        except Exception:
            logger.exception("Error while checking job status in repository layer")
            raise
    

    @staticmethod 
    def update_job_status(db: Session, job_id: str, status: str): # Worker service will call this when processing:
        try:
            job = db.query(Job).filter(Job.id == job_id).one_or_none()

            if not job:
                logger.warning(f"Job not found: {job_id}")
                raise ValueError("Job not found")

            if job.status == status:
                return job
            
           
            job.status = status

            db.commit()
            db.refresh(job)
           
            return job
        
        except Exception:
            logger.exception("Error while updating job status in repository layer")
            db.rollback()
            raise
       
        
    
    @staticmethod
    def get_by_idempotency_key(db:Session, idempotency_key:str):
        ''' Checks for Existing JOb'''
        try:
    
            return db.query(Job).filter(Job.idempotency_key == idempotency_key).one_or_none()
        
        except Exception:
            logger.exception("Error while checking idempotency key")
            raise


    @staticmethod
    def job_processing(db:Session, job_id:str, user_selected_columns:list):
        ''' This function stores user selected column in DB'''
        try:
            logger.info("Inside job repository function to stores user selected column in DB")
            
            job =  db.query(Job).filter(Job.id == job_id).one_or_none()

            if not job:
                logger.warning(f"Job not found: {job_id}")
                return None

            job.user_selected_columns = user_selected_columns
            job.status = "processing_columns_received"
            db.commit()
            db.refresh(job)
            return job
        
        except Exception:
            logger.exception("Error while updating user selected columns in DB")
            db.rollback()
            raise
    
