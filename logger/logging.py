import logging
import sys
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv
load_dotenv()
import os,sys

def setup_logging():

    #settings = get_settings()
    log_level = os.getenv("LOG_LEVEL","INFO")

    logger = logging.getLogger() # Gets global logger.
    logger.setLevel(log_level)

    # Remove default handlers
    logger.handlers = []

#---------------Write in console------------------
    console_log_handler = logging.StreamHandler(sys.stdout) # Sends logs to console.
    
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    console_log_handler.setFormatter(formatter)

    logger.addHandler(console_log_handler)

# --------------- write in file ----------------------
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "log.txt")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    '''
    # Add extra context
    logger = logging.LoggerAdapter(
        logger,
        {
            "service": "api-service",
            "environment": settings.app_env
        }
    )
    '''

    return logger
