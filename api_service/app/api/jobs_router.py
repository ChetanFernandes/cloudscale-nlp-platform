from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from data_layer.database import get_db
from api_service.app.services.job_service import JobService
from api_service.app.core.rate_limiter import rate_limiter
from api_service.app.schemas.column_request import ColumnRequest
from common.storage_azure import generate_SAS_url
from typing import Optional
import redis, json, gzip
import os

from logger.logging import setup_logging
logger = setup_logging()



router = APIRouter(prefix = "/jobs",tags = ["Jobs"])

@router.post("/")
def create_job(object_name: Optional[str] = None, text:Optional[str] = None, db:Session = Depends(get_db), idempotency_key: str | None = Header(None, alias="Idempotency-Key"), _:None = Depends(rate_limiter)):
    """ This job router function calls job service to create job in DB"""
   
    logger.info("Inside job router function which call job service to create job in DB")

    job = JobService.create_job(db, object_name, text, idempotency_key)

    logger.info(f"Job created in DB. Job details: job_id -> {job.id} and job status {job.status} and File_name:{job.file_name}")

    return {"job_id":job.id, "status":job.status, "File_name":job.file_name}


@router.get("/{job_id}")
def get_job_status(job_id:str,db:Session = Depends(get_db)):
     
    """ This job router function calls job service get status of job"""


    logger.info("Inside job router function which call job service get status of job")

    job_details = JobService.get_job_status(db,job_id)

    logger.info(f" Job Status-> {job_details}")

    if not job_details:
        raise HTTPException(status_code=404, detail="Job not found")

    return {"job_details": job_details}

@router.get("/{job_id}/columns")
def get_columns(job_id:str, db:Session = Depends(get_db)):

    """ This job router function extracts columns from the given file"""

    logger.info("Inside job router function which call job service get columns")

    job = JobService.get_job_by_id(db, job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    logger.info(f"Updates on job - {job.status}")
    logger.info(f"Updates on columns - {job.extracted_columns}")

    return {
        "status": job.status,
        "columns": job.extracted_columns
    }



@router.post("/nlp_processing")
def start_job(request : ColumnRequest, db:Session = Depends(get_db)):
    
    """ This job router function calls job service to store user selected column in DB and start processing nlp_pipeline"""
   
    logger.info("Inside job router function calls job service to store user selected column in DB and start processing nlp_pipeline")

    job = JobService.start_job_processing(db, request.job_id, request.user_selected_columns)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    logger.info(f"Status of jobs post user selecting columns - {job.status}")
    logger.info(f"User_selected columns - {job.user_selected_columns}")

    return {
        "status": job.status,
    }

@router.get("/{job_id}/download_url")
def get_download_url(job_id:str, db:Session = Depends(get_db)):

    logger.info("Called download URL in backend")

    job = JobService.get_job_by_id(db, job_id)

    logger.info(f"job detiails - > {job}")

    sas_url = generate_SAS_url(job.final_zip_path)
    logger.info(f"Download url -{sas_url}")

    return {
        "url": sas_url,
    }

# this is only for local dev
#from dotenv import load_dotenv 
#load_dotenv()



def get_redis():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(redis_url)


@router.get("/text_result/{job_id}")
def get_result(job_id: str, db:Session = Depends(get_db)):

    logger.info("Calling job service function to get job")

  
    db.expire_all()  # force fresh read from DB

    job = JobService.get_job_by_id(db, job_id)
    
    logger.info(f"job details {job}")

    if not job:
        return {"status": "not_found"}
    

    
    status = job.status

    if status not in ["Text_processing_completed", "Text_processing_failed"]:
        return {"status": "processing"}
    
    if status == "Text_processing_failed":
        return {"status": "failed"}
    
    r = get_redis()
    data = r.get(f"job_result:{job_id}")

 
    if not data:
        # Redis expired or failed
        return {"status": "processing"}   # or "not_available"

    try:
        decompressed = gzip.decompress(data)
        result = json.loads(decompressed.decode())
    except Exception:
        logger.error(f"Error decoding result")
        return {"status": "failed"}

    return {"status": "completed", "data": result}

    

















'''
Note -  If there was exception handler mentioned in FAST API then we have to write try an catch block in below format
@router.post("/")
def create_job(object_name:str, db:Session = Depends(get_db), idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
                _:None = Depends(rate_limiter)):
    """ This function creates job in postgress DB. This job router function calls job service to create job in DB"""
    try:
        logger.info("Inside job router function which call job service to create job in DB")

        job = JobService.create_job(db,object_name,idempotency_key)

        logger.info(f"Job created in DB. Job details: job_id -> {job.id} and job status {job.status}")

        return {"job_id":job.id, "satus":job.status}
    
    except Exception:
        logger.exception("Error while creating job in router  layer")
        raise
'''