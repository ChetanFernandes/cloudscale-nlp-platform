
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta, timezone
from logger.logging import setup_logging
from common.config import get_settings
from azure.storage.blob import BlobServiceClient
from typing import Optional
from adlfs import AzureBlobFileSystem

logger = setup_logging()
settings = get_settings()


STORAGE_ACCOUNT_NAME = settings.storage_account_name
CONTAINER_NAME = settings.CONTAINER_NAME
ACCOUNT_KEY = settings.STORAGE_ACCOUNT_KEY
CONNECTION_STRING = settings.STORAGE_CONNECTION_STRING

# 🔹 global holder (but controlled)
blob_service_client: Optional[BlobServiceClient] = None


# 🔹 getter (used everywhere)
def get_blob_service_client() -> BlobServiceClient:
    global blob_service_client

    if blob_service_client is None:
        logger.info("Initializing BlobServiceClient...")

        if not CONNECTION_STRING:
            raise RuntimeError("STORAGE_CONNECTION_STRING is missing")

        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    return blob_service_client



def generate_SAS_url(object_name: str):
    try:
        ''' This fucntion creates  a temporary, secure upload link (SAS URL). So instead of uploading via backend, you:
            Generate URL ✅
            Client uploads file directly to Azure ✅'''
        
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=CONTAINER_NAME,
            blob_name=object_name,
            account_key=ACCOUNT_KEY,
            permission=BlobSasPermissions(write=True, create=True, read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1) # Link works for 1 hour only
        )
        '''   
        Allow access to:
        Storage account → ACCOUNT_NAME
        Container → test
        File → object_name
        With permissions:
        BlobSasPermissions(write=True, create=True)
        create=True → can create new file
        write=True → can upload data
        
        '''
        url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{object_name}?{sas_token}"

        return url

    except Exception:
        logger.exception("Error in generating upload URL")
        raise 




# helper: download blob
def get_blob(object_name: str):
    client = get_blob_service_client()
    blob_client = client.get_blob_client(container=CONTAINER_NAME,blob=object_name)
    stream = blob_client.download_blob()
    return stream


# helper: upload blob
def get_blob_client(blob_name: str):
    client = get_blob_service_client()
    return client.get_blob_client(container=CONTAINER_NAME,blob=blob_name)


# It reads only metadata + requested row groups
def get_azure_fs():
    return AzureBlobFileSystem(
        account_name=STORAGE_ACCOUNT_NAME,
        account_key=ACCOUNT_KEY
    )


