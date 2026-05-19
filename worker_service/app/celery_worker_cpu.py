
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
    "worker_cpu",
    broker=redis_url,
    backend=redis_url,
    include=[
        "worker_service.app.tasks.process_text",
        "worker_service.app.pipelines.column_normalization_nlp_processing",
    ]
)


# ✅ STEP 2: Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_default_queue="cpu",  
    worker_prefetch_multiplier=1, 
    task_acks_late = True,
    task_routes = {


# CPU queue 
    "worker_service.app.tasks.process_text.text_nlp_processing": {"queue": "cpu"},
    "worker_service.app.pipelines.column_normalization_nlp_processing.process_chunk_nlp_processing": {"queue": "cpu"},



},

)


@setup_logging.connect
def configure_celery_logging(**kwargs):
    custom_setup_logging()




''' 
celery_app.autodiscover_tasks(["worker_service.app.tasks"])

| Feature          | Prefork    | Threads | Gevent |
| ---------------- | ---------- | ------- | ------ |
| True parallelism | ✅          | ❌       | ❌      |
| CPU tasks        | ✅ BEST     | ❌       | ❌      |
| I/O tasks        | ✅          | ✅       | ✅ BEST |
| Memory usage     | ❌ High     | ✅ Low   | ✅ Low  |
| Stability        | ✅          | ✅       | ⚠️     |
| Windows support  | ⚠️ (spawn) | ✅       | ⚠️     |
'''