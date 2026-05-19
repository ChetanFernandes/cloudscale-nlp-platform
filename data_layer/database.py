
'''
Connect to Postgres
Manage connection pool
Provide DB sessions to API routes
'''

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from common.config import get_settings
settings = get_settings()

# create Database engine
engine = create_engine(settings.database_url,pool_pre_ping=True,pool_size=10,max_overflow=20,pool_recycle=1800)
# This creates the database connection manager.
# Instead of opening a new connection every time, it maintains a pool of connections.

# Session Factory
SessionLocal = sessionmaker(autocommit = False, autoflush=False, bind = engine)
# A session is like a conversation with the database.
# sessionmaker → Session generator
# SessionLocal() → actual session
# autocommit=False -> Changes are NOT automatically saved to the database.
# autoflush=False -> Send pending changes to the database before executing queries




# Base class for models

Base = declarative_base()
# This will be used later when we create database models.

# Dependency for Fast API
# Whenever an API endpoint needs database access, FastAPI will:
# Open a session -> Give it to the endpoint -> Close it automatically -> Very safe and clean.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

#from api_service.app.models.job_model import Job  # This ensures SQLAlchemy knows about the model.