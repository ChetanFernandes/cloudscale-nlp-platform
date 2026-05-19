from sqlalchemy import Column, String, DateTime, JSON,Float
from sqlalchemy.sql import func
import uuid
from data_layer.database import Base


class Job(Base):

    __tablename__ = 'jobs' # This tells SQLAlchemy: create a table called jobs

    id = Column(String, primary_key=True, default = lambda:str(uuid.uuid4())) # Means this uniquely identifies every job.

    file_name = Column(String, nullable=True) # Stores uploaded file_name

    text = Column(String, nullable=True)

    status = Column(String, nullable=False, default="pending") 

    idempotency_key = Column(String, unique=True, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())  # Automatically stores time when job is created. 
    # We use func.now() which means database sets time

    completed_at = Column(DateTime(timezone=True), nullable=True) # Stores when job finished.

    extracted_columns = Column(JSON, default=list, nullable = True)

    encoding = Column(String, nullable=True) 

    parquet_file_path = Column(String, nullable=True)

    user_selected_columns = Column(JSON, default=list,nullable=True) 

    aggregation_progress = Column(JSON, default=lambda: {"processed_chunks": [], "status": "in_progress"}, nullable=True)
    # from sqlalchemy.orm.attributes import flag_modified
    # flag_modified(job, "aggregation_progress")

    # Lambda = “give me a fresh copy every time, not a shared one”

    final_zip_path = Column(String, nullable=True) 

    total_time_taken = Column(Float, nullable=True)



'''
Think of it like this:

Models = blueprint of a house

Database = land where house will be built

engine = address of the land

create_all() = construction process

'''
'''

Because SQLAlchemy works using Object Relational Mapping (ORM).

ORM means:

Python Class  ↔  Database Table
Python Object ↔  Table Row

So:

Job class → jobs table
job object → row in jobs table

That is why we create:

job = Job(...)



'''