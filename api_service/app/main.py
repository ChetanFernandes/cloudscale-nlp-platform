from fastapi import FastAPI,Request
from common.config import get_settings
from logger.logging import setup_logging
from data_layer.database import engine, Base
from contextlib import asynccontextmanager
from api_service.app.api.jobs_router import router as jobs_router
from api_service.app.api.health_router import router as health_router
from api_service.app.api.storage_router import router as storage_router
from fastapi.responses import JSONResponse



settings = get_settings()
logger = setup_logging() # Connect logging to app


@asynccontextmanager
async def lifespan(app:FastAPI):
    logger.info("Starting application") 
    Base.metadata.create_all(bind=engine)# Look at all the models defined in Python and create those tables in the database." It only creates tables if they do not already exist.)
    yield

app = FastAPI(lifespan = lifespan, title = settings.app_name) # This initializes the FastAPI application object.

# GLOBAL EXCEPTION HANDLER 
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """ If any error occurs in: Repository , Service , Router , Celery call , Database -> FastAPI will automatically run: global_exception_handler()"""
    logger.exception(f"Unhandled exception: {exc}")

    return JSONResponse(
        status_code = 500,
        content={"message": "Internal server error"}
    )



logger.info("Handling routers")
# add all the routes defined in jobs_router to my main application.”
app.include_router(health_router) # Register health check-up
app.include_router(storage_router)  
app.include_router(jobs_router)



'''
create_all() tells SQLAlchemy:

“Look at all the models and create the corresponding tables in the database.”

So it converts Python models → actual database tables
'''
''' 
Why do we write bind=engine?

The question SQLAlchemy needs answered is:

Which database should I create the tables in?

Because your app could theoretically connect to multiple databases.

That is why we pass the engine.

'''


@app.get("/")
def root():
    logger.info("Root endpoint accessed")
    return {
        "app":settings.app_name,
        "environment":settings.app_env
    }