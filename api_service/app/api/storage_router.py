from fastapi import APIRouter
import uuid
from common.storage_azure import generate_SAS_url
from logger.logging import setup_logging
from fastapi import HTTPException
logger = setup_logging()

router = APIRouter(prefix="/azure_storage", tags=["Storage"])

@router.get("/upload-url")
def get_upload_url(object_name:str):
    try:
        '''Generates URL to store file in S3 bucket'''
        
        object_name = f"{uuid.uuid4()}_{object_name}"

        url = generate_SAS_url(object_name)

        logger.info(f"[UPLOAD_URL] object={object_name}")

        return {
            "upload_url": url,
            "object_name": object_name
        }
    except Exception:
        logger.exception("Error while generating upload URL")
        raise HTTPException(status_code=500, detail="Failed to generate upload URL")




        

