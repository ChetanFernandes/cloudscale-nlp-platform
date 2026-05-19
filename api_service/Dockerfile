FROM python:3.12-slim

WORKDIR /app

# Copy only API requirements
COPY api_service/requirements_api.txt .

RUN pip install --no-cache-dir -r requirements_api.txt

# Copy API code + utility (shared code)
COPY api_service/ ./api_service/
COPY logger/ ./logger/
COPY common/ ./common
COPY data_layer/ ./data_layer/

CMD ["uvicorn", "api_service.app.main:app", "--host", "0.0.0.0", "--port", "8000"]