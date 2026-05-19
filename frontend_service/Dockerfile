FROM python:3.12-slim

WORKDIR /app

# Copy only API requirements
COPY frontend_service/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN python -m spacy download en_core_web_sm

# Copy API code + utility (shared code)

COPY frontend_service/ .
COPY logger/ ./logger/

CMD ["streamlit", "run", "streamlit_app_google_style.py", "--server.port=8501", "--server.address=0.0.0.0"]