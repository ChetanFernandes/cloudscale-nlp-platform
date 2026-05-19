FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app

# Copy worker requirements
COPY worker_service/requirements_base.txt .

RUN pip install --no-cache-dir -r requirements_base.txt

# Download models at build time (important)


# Copy worker + utility
COPY worker_service/ ./worker_service/
COPY data_layer/ ./data_layer/
COPY logger/ ./logger/
COPY common/ ./common
COPY utility/ ./utility/

CMD ["celery", "-A", "worker_service.app.celery_worker_io.celery_app", "worker", "-Q", "io", "--loglevel=info", "--concurrency=4"]
