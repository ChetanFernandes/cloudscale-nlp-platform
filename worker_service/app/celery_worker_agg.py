
# Create Celery Worker File
from dotenv import load_dotenv
load_dotenv()

from celery import Celery
from celery.signals import setup_logging
import os
from logger.logging import setup_logging as custom_setup_logging

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
print(redis_url)


# ✅ STEP 1: Create Celery app FIRST
celery_app = Celery(
    "worker_agg",
    broker=redis_url,
    backend=redis_url,
    include=[
        "worker_service.app.tasks.aggregate_final",
        "worker_service.app.tasks.aggregate_partial",
        "worker_service.app.tasks.zipping_final_path_uploading",
    ]
)


# ✅ STEP 2: Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_default_queue="aggregation",  
    worker_prefetch_multiplier=1, 
    task_acks_late = True,
    task_routes = {


# Aggregation Queeue
    "worker_service.app.tasks.aggregate_final.aggregate_final_": {"queue": "aggregation"},
    "worker_service.app.tasks.aggregate_partial.aggregate_partial_": {"queue": "aggregation"},
    "worker_service.app.tasks.aggregate_partial.aggregate_chunks": {"queue": "aggregation"},
    "worker_service.app.tasks.zipping_final_path_uploading.zip_upload": {"queue": "aggregation"}

},

)


@setup_logging.connect
def configure_celery_logging(**kwargs):
    custom_setup_logging()