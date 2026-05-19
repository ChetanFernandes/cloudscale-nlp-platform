# Create Celery Configuration


from celery import Celery
from common.config import get_settings

settings = get_settings()

celery_app = Celery("file_processor", broker = settings.redis_url, backend = settings.redis_url)
celery_app.conf.update(task_serializer = "json",accept_content = ["json"],result_serializer  = "json", timezone = "utc")

# Celery() - Creates Celery application. Think of it as task manager.
# broker = settings.redis_url - Redis acts as message queue/ Tasks are stored here.
# backend = settings.redis_url - Stores task results.
