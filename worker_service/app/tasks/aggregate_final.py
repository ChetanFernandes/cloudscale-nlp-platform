from worker_service.app.celery_worker_agg import celery_app
from data_layer.repositories.job_repository import JobRepository
from data_layer.database import SessionLocal
from logger.logging import setup_logging
import tempfile, os, time
import pyarrow.parquet as pq
from common.config import get_settings
from common.storage_azure import get_blob_client, get_azure_fs
from sqlalchemy.orm.attributes import flag_modified
from azure.core.exceptions import AzureError
from functools import lru_cache
from common.exceptions import Azurepatherror
import hashlib

@lru_cache(maxsize=50)
def get_parquet_file(path):
    fs = get_azure_fs()
    return pq.ParquetFile(f"{CONTAINER_NAME}/{path}",filesystem=fs)

settings = get_settings()
CONTAINER_NAME = settings.CONTAINER_NAME
logger = setup_logging()

celery_app.config_from_object("worker_service.app.celery_worker_agg")

max_retry = 3
retry = 0

@celery_app.task(name="worker_service.app.tasks.aggregate_final.aggregate_final_",bind=True, autoretry_for=(ConnectionError, TimeoutError,AzureError), retry_backoff=True, retry_kwargs={"max_retries": 3})
def aggregate_final_(self, partial_results, job_id):

    db = SessionLocal()

    job = JobRepository.get_job(db, job_id)

    if job.status in ["aggregation_process(map_reduce)_completed", "aggregation_process_failed"]:
            logger.info(f"[JOB={job_id}] Aggregation already completed. Skipping.")
            return
    
    processed = set(job.aggregation_progress.get("processed_chunks", []))
    processed_now = []

    logger.info(f"Processed chunks -> {processed}")

    logger.info(f"[JOB={job_id}] Starting FINAL aggregation")

    TEMP_DIR = "D:\\aggregation_tmp"
    os.makedirs(TEMP_DIR, exist_ok=True)

    temp_file = tempfile.NamedTemporaryFile(delete=False, dir=TEMP_DIR)
    temp_path = temp_file.name
    temp_file.close()

    writer = None
    base_schema = None

    try:
        valid_keys = [r["key"] for r in partial_results if isinstance(r, dict) and r.get("status") == "success"]
        
        for azure_path in valid_keys:
            try:
                parquet_file = get_parquet_file(azure_path)
                total_row_group = parquet_file.num_row_groups
                
                row_errors = 0

                for rg in range(parquet_file.num_row_groups):
                    try:
                        table = parquet_file.read_row_group(rg)

                        if writer is None:
                            base_schema = table.schema
                            writer = pq.ParquetWriter(temp_path, base_schema, version="2.6", compression="snappy")

                        else:

                            if table.schema != base_schema:
                                raise ValueError(f"Schema mismatch in {azure_path}")

                        writer.write_table(table)

                    except Exception:
                        row_errors += 1
                        logger.info(f"Error while reading row {rg}")
                        continue

                    processed.add(azure_path)
                    processed_now.append(azure_path)

                    if len(processed_now) % 5 == 0: #  “After every 5 processed chunks, save progress to DB”
                        progress = job.aggregation_progress or {}
                        progress["processed_chunks"] = list(processed)

                        job.aggregation_progress = progress
                        flag_modified(job, "aggregation_progress")
                        db.commit()
                    
                    if total_row_group > 0 and (row_errors / total_row_group) > 0.5:
                        logger.error(f"[JOB={job_id}] > 50% row group failure in {azure_path}, skipping file")
                        raise Azurepatherror(f"Failed at row group {rg}")
                    
            except Azurepatherror:
                logger.warning(f"[JOB={job_id}] Skipping corrupted parquet_file_chunk {azure_path}")
                continue
        
            except Exception as e:
                logger.exception(f"[JOB={job_id}] Failed aggrergating chunk {azure_path}: {e}")
                continue
                            

        if writer is None:
            raise ValueError("No data written in final aggregation")

        writer.close()

        progress = job.aggregation_progress or {}
        progress["processed_chunks"] = list(processed)
        progress["status"] = "completed"   

        db.commit()

        # Final upload

        hash_input = "|".join(sorted(valid_keys))

        hash_val = hashlib.md5(f"{job_id}_{hash_input}".encode()).hexdigest()

        final_key = f"Aggregation_done_passing_to_zip/{job_id}/{hash_val}.parquet"

    
        blob_client = get_blob_client(final_key)

        if blob_client.exists():
            logger.info(f"[JOB={job_id}] Final aggregation file already exists")
            job.status = "aggregation_process(map_reduce)_completed"
            db.commit()
            db.refresh(job)
            return {"status": "success", "final_key": final_key}

        for attempt in range(1, max_retry + 1):
            try:
                with open(temp_path, "rb") as f:
                        blob_client.upload_blob(f, overwrite=False, content_type="application/x-parquet", timeout=120)

                logger.info(f"[JOB={job_id}] FINAL aggregation completed → {final_key}")
                job.status = "aggregation_process(map_reduce)_completed"
                db.commit()

                return {"status": "success", "final_key": final_key}
            
            except Exception as e:
                logger.warning(f"[JOB={job_id}] Upload attempt {attempt} failed")

                if blob_client.exists():
                    logger.info(f"[JOB={job_id}] File already uploaded by another worker")
                    job.status = "aggregation_process(map_reduce)_completed"
                    db.commit()
                    
                if attempt == max_retry:
                    logger.exception(f"[JOB={job_id}] Upload failed after retries")
                    raise

                time.sleep(2 ** attempt)
                
    except (ConnectionError, TimeoutError, AzureError):
        logger.warning(f"[JOB={job_id}] Retryable error during aggregation")

        if self.request.retries >= self.max_retries:
            logger.exception(f"[JOB={job_id}] Aggregation failed")
            JobRepository.update_job_status(db, job_id,"aggregation_process_failed")
        raise 

    except Exception:
        logger.exception(f"[JOB={job_id}] Aggregation failed")
        JobRepository.update_job_status(db, job_id,"aggregation_process_failed")
        raise


    finally:
        if db:
            db.close()

        if writer:
            try:
                writer.close()
            except:
                pass

        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass

 
        del processed_now, processed

       
       
