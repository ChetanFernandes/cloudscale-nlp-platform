
# Your code is used to generate a secure temporary upload URL for MinIO (S3 storage). 
# This is a very common pattern in modern cloud architectures because it allows clients to upload files directly to object storage 
# without passing through your API server.

import boto3
from botocore.client import Config
import os
from logger.logging import setup_logging
logger = setup_logging()
import aioboto3
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

from common.config import get_settings
settings = get_settings()
BUCKET = settings.BUCKET_NAME
'''
boto3

Official AWS SDK for Python

Used to interact with S3-compatible storage

Even though you are using MinIO, it supports the S3 API, so boto3 works perfectly.

Config

Used to configure request signing and protocol behavior.
'''


# This creates a connection client to MinIO.
def get_s3_client():
    return boto3.client(
    "s3", # Specifies the service.
    endpoint_url="http://localhost:9000", # Normally boto3 connects to: https://s3.amazonaws.com. But since you're using MinIO locally, you override the endpoint.
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    use_ssl=False,
    config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}   # ✅ FIX
        ), # S3 requires request signing for security. # Signature V4 is the modern standard. 

    # it ensures URL cannot be tampered with
        # URL expires
        # Request is authenticated
    region_name="us-east-1",
)

def get_async_s3_client():
    session = aioboto3.Session()
    return session.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}   # ✅ FIX
        ),
        use_ssl=False,
        region_name="us-east-1",
    )

s3_client = get_s3_client()


def generate_upload_url(object_name: str):
    try:
        '''This function creates a temporary upload link.'''

        url = s3_client.generate_presigned_url(
            "put_object",  
            # Tells S3 Generate a temporary URL that allows uploading an object'''
            Params={
                "Bucket": BUCKET,
                "Key": object_name,
            },
            ExpiresIn=3600
        )

        return url
    except Exception:
        logger.exception("Error in generating upload URL")
        raise 

def upload_file(file_path,s3_client):

    filename = os.path.basename(file_path)

    s3_client.fput_object(
        BUCKET,
        filename,
        file_path
    )

    return filename

'''
This pattern is used to avoid sending large files through your API server. 

Without Presigned URL ❌

    Client
    ↓
    FastAPI server
    ↓
    MinIO

    Problem:
    1. API server handles huge files
    2. high memory usage
    3. slow

With Presigned URL ✅

    Client
    ↓
    FastAPI (generate URL)
    ↓
    Client uploads directly
    ↓
    MinIO

Flow:

1️⃣ Client requests upload URL

POST /upload-url

2️⃣ FastAPI generates presigned URL

generate_upload_url("file.csv")

3️⃣ Client uploads directly to MinIO

PUT presigned_url

4️⃣ File stored in bucket

8️⃣ Example Client Upload



This architecture is used by:

AWS

Google Cloud

Azure

Netflix

Uber

Benefits:

🚀 Faster uploads

File goes directly to storage

🔒 Secure

URL expires automatically.

⚡ Scalable

API server doesn't handle large files.

💰 Cost efficient

Less compute usage.
'''