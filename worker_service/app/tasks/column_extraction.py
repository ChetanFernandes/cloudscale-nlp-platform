from worker_service.app.celery_worker_io import celery_app
from data_layer.database import SessionLocal
from worker_service.app.processor.column_extraction_helper import extracting_columns
from data_layer.repositories.job_repository import JobRepository
import  io
import pyarrow.parquet as pq
from common.storage_azure import get_blob, get_azure_fs
from logger.logging import setup_logging
from common.config import get_settings
import pandas as pd
from azure.core.exceptions import AzureError
from common.exceptions import HeaderError
from utility.csv_cleaning import is_valid_header,is_key_value_pattern


logger = setup_logging()
settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME

sample_df = None

@celery_app.task(name ="worker_service.app.tasks.column_extraction.extract_columns_task", bind=True, autoretry_for=(ConnectionError, TimeoutError, AzureError), retry_backoff=True, retry_kwargs={"max_retries": 2})
def extract_columns_task(self, payload: dict):
    try:
        
        db = SessionLocal()

        if not payload:
            logger.error("Invalid payload received: None or empty")
            return None
          
        file_type = payload.get("file_type")
        job_id = payload.get("job_id")
        object_name = payload.get("file_path")

        if not file_type or not job_id or not object_name:
            logger.error("Invalid payload received")

            if job_id:
                JobRepository.update_job_status(db, job_id, "column_extraction_failed")
            return
        
        job = JobRepository.get_job(db, job_id)

        if not job:
            JobRepository.update_job_status(db, job_id, "job_not_found")
            raise ValueError(f"Job {job_id} not found")
        

        if job.status in ["Invalid_file","Column_extraction_failed","columns_extracted","no_columns_found","job_not_found"]:
            logger.info(f"[JOB={job_id}] - Already completed. Skipping.")
            return
        
        if file_type == "csv":

            #-------------CSV columns extraction-----------------

            logger.info(f"[JOB={job_id}] - File is csv. Will go ahead for column extraction")

            encodings = ["utf-8","cp1252","latin1"]
            
            sample_df = None
            final_enc = None

            for enc in encodings:
                try:
                    stream = get_blob(object_name)
                    header_sample = stream.read(1024 * 50) # read first 50KB only
                    sample_df = pd.read_csv(io.BytesIO(header_sample), nrows = 50, encoding = enc)
                    # io.BytesIO - Converts raw bytes → file-like object in memory beacuse pandas.read_csv() expects a file or file-like object
                    final_enc = enc
                    logger.info(f"Encoding detected: {enc}")
                    break

                except UnicodeDecodeError:
                    continue

                except pd.errors.ParserError:
                    # Retry with bigger sample (but MUST re-fetch object)
                    logger.warning(f" [JOB={job_id}] - Retrying with larger sample for encoding {enc}")
                    stream = get_blob(object_name) # you need to call stream again here as pointer will be moving forward from first read
                    header_sample_large = stream.read(1024 * 200)

                    try:
                        sample_df = pd.read_csv(io.BytesIO(header_sample_large), nrows=100, encoding = enc)
                        final_enc = enc
                        logger.info(f"Encoding detected with larger sample: {enc}")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception:
                        continue

            

            if final_enc is None:
                logger.exception(f"[JOB={job_id}] - Failed to detect encoding")
                JobRepository.update_job_status(db, job_id, "column_extraction_failed")
                raise ValueError(f"Could not detect encoding for file {object_name}")
            

            # Read CSV raw
            stream = get_blob(object_name)
            df_raw = pd.read_csv(stream,header= None, encoding = final_enc, nrows = 20)

            # Detect header row

            header_candidates = []

            for i, row in df_raw.iterrows():
                row_list = list(row) # convert row to list

                #Trim leading emmpty cells

                for j, val in enumerate(row_list): #loop through each cell
                    if pd.notna(val) and str(val).strip() != "":
                        trimmed = row_list[j:]
                        break
                else:
                    continue

                if is_valid_header(trimmed):
                    header_candidates.append(i)

            if len(header_candidates) == 0:
                raise HeaderError(f"[JOB={job_id}] Invalid file: No table found")


            if len(header_candidates) > 1:
                raise HeaderError(f"[JOB={job_id}] Invalid file: Multiple tables detected")
            

            header_row = header_candidates[0]

            logger.info(f"[JOB={job_id}] Header detected at row {header_row}")


            # 🔹 Step 4: Re-read with header
            stream = get_blob(object_name)
            df = pd.read_csv(stream, header=header_row, encoding=final_enc, nrows = 50)

            df.columns = df.columns.astype(str).str.strip()


            # ---------------------------------
            # 🔹 Step 6: Reject key-value format
            # ---------------------------------
            if is_key_value_pattern(df):
                logger.exception(f"Invalid csv file detected - {object_name}. Key value format deetected")
                JobRepository.update_job_status(db, job_id, "Invalid_file")
                return 

            # ---------------------------------
            # 🔹 Step 7: Validate structure consistency
            # ---------------------------------
            col_count = len(df.columns)

            valid_rows = 0
            total_rows = 0

            for _, row in df.head(50).iterrows():
                values = [v for v in row if pd.notna(v) and str(v).strip() != ""]
                total_rows += 1

                if len(values) >= col_count * 0.5:
                    valid_rows += 1

            if valid_rows < total_rows * 0.6:
                raise HeaderError(f"[JOB={job_id}] Invalid file: Not a structured table.")

            # ---------------------------------
            # 🔹 Step 8: Remove empty/junk rows
            # ---------------------------------
            df = df[df.iloc[:, 0].astype(str).str.strip() != ""] # select frst column removes blank rows
            df = df.reset_index(drop=True)

            logger.info(f"[JOB={job_id}] CSV processing successful")

            if df.shape[0] < 2:
                raise HeaderError(f"[JOB={job_id}] Invalid file: Not enough data rows")

            sample_df = df.head(100)
        
        else:

            
            #-------------Excel columns extraction-----------------

            logger.info(f"[JOB={job_id}] - File is excel. Will process for column extraction")
            fs = get_azure_fs()

            excel_to_parqet_path = payload.get("file_path", [])
            
            if not excel_to_parqet_path:
                 logger.exception(f"Parquet path missing for file {object_name}")
                 JobRepository.update_job_status(db, job_id, "Coloumn_extraction_failed")
                 raise ValueError("Parquet path missing")
            

            parquet_file = pq.ParquetFile(f"{CONTAINER_NAME}/{excel_to_parqet_path}",filesystem=fs)
            # Only fetches metadata (footer)
            # No full file download

            # columns = parquet_file.schema.names Can directly fetch columns
            # Read only first row group (efficient)
            table = parquet_file.read_row_group(0)

            '''
            “Open the first chapter of the book”
                The file is huge
                Instead of reading everything
                You read just one chunk (row group)
            That chunk may contain thousands of rows, not just one
            '''

            sample_df = table.to_pandas()  # PyArrow format → converted into pandas DataFrame
        
            sample_df = sample_df.head(100)

            logger.info(f"[JOB={job_id}] - Sample preview: {sample_df.head(3)}")


        # ================= VALIDATION =================    
        if sample_df is None:
            logger.error(f"columns extraction failed for:  {object_name}")
            JobRepository.update_job_status(db, job_id, "Column_extraction_failed")
            raise ValueError(f"Could not decode file: {object_name}")
        
        logger.info(f"Extracted sample - {sample_df.head(10)} for columns extraction from file -> {object_name}")

    # 2 --------------Column extraction and update DB ----------------------------#
        text_cols = extracting_columns(sample_df,object_name)
        
        job = JobRepository.get_job(db, job_id)

        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        job.extracted_columns = text_cols or []

        logger.info(f" Saved extracted columns to DB -> {text_cols}")
        
        job = JobRepository.get_job(db, job_id)
        job.status = "columns_extracted" if text_cols else "no_columns_found"
        job.encoding = final_enc if file_type == "csv" else "parquet"
        job.parquet_file_path = excel_to_parqet_path if file_type == "parquet" else "csv"

        db.commit()
        db.refresh(job)

        logger.info(f"Job updated -> status={job.status}, columns={job.extracted_columns}, encoding - {job.encoding}")
    

    except HeaderError:
        logger.exception(f"[JOB={job_id}] No Header found. Invalid CSV file")
        JobRepository.update_job_status(db, job_id, "Invalid_file")
        return

    except (ConnectionError, TimeoutError, AzureError) as e:
        logger.warning(f"[JOB={job_id}] Retryable error: {e}")

        if self.request.retries >= self.max_retries:
            logger.exception(f"[JOB={job_id}] Final failure after retries: {e}")
            JobRepository.update_job_status(db, job_id, "Column_extraction_failed")
        raise 
    
    except Exception as e:
        logger.exception(f"[JOB={job_id}] Non-retryable failure: {e}")
        JobRepository.update_job_status(db, job_id, "Column_extraction_failed")
        raise 

    finally:
        if db:
            db.close()



