
from logger.logging import setup_logging
logger = setup_logging()
from typing import List


def extracting_columns(sample_df,object_name:str) -> List [str]:
    '''Column extraction function'''
    try:
        logger.info(f"Inside column extraction function to extract column of file -> {object_name}")

        logger.info("Selecting columns only of type object and string")
        
        text_cols = sample_df.select_dtypes(include=["object", "string"]).columns.tolist()

        valid_text_cols = [col for col in text_cols if str(col).strip() != ""]
  
        logger.info(f"Columns extracted -> {valid_text_cols}")

        return valid_text_cols
    
    except Exception:
        logger.exception(f"Error while extracting columns failed for file {object_name}")
        raise