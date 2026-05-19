
from worker_service.app.celery_worker_cpu import celery_app
import pandas as pd
from utility.chat_words import chat_conversion
from logger.logging import setup_logging
from utility.aspect_col import aspect_extraction_col
import io,  re
from common.config import get_settings
from common.storage_azure import get_blob_client
from azure.core.exceptions import AzureError
import hashlib
from utility.nlp import get_nlp, get_model
import time
import numpy as np
from common.storage_azure import get_azure_fs
from functools import lru_cache
import pyarrow.parquet as pq

logger = setup_logging()
settings = get_settings()

CONTAINER_NAME = settings.CONTAINER_NAME

URL_pattern = re.compile(r'https?://\S+|www\.\S+')
HTML_pattern = re.compile('<.*?>')


MAX_UPLOAD_RETRIES = 3

_nlp = None
_model = None

def get_cached_nlp():
    global _nlp
    if _nlp is None:
        _nlp = get_nlp()
    return _nlp

def get_cached_model():
    global _model
    if _model is None:
        _model = get_model()
    return _model

MAX_RETRIES = 1

@lru_cache(maxsize=50)
def get_parquet_file(path):
    fs = get_azure_fs()
    return pq.ParquetFile(f"{CONTAINER_NAME}/{path}",filesystem=fs)


@celery_app.task(name="worker_service.app.pipelines.column_normalization_nlp_processing.process_chunk_nlp_processing", bind = True, autoretry_for=(ConnectionError, TimeoutError,AzureError), retry_backoff = True, retry_backoff_max = 600, retry_kwargs = {"max_retries": 1})
def process_chunk_nlp_processing(self,payload, columns:list,job_id:str):
        
        df = None
        chunk_id = payload.get("chunk_id", "unknown")  
        source_file = payload.get("source", "unknown")
       

        logger.info(f"[JOB={job_id}] Processing chunk {chunk_id} (source={source_file})")

        try:
            if source_file == "csv":
                 
                header = payload.get("header")
                if not header:
                    logger.exception(f"Header not found for {chunk_id}")
                    return {"status": "failed", "row_group": row}
         
                 
                rows = payload.get("rows")
                if not header:
                    logger.exception(f"row not found for {chunk_id}")
                    return {"status": "failed", "row_group": row}

                csv_content = header + "\n" + "\n".join(rows)
                 
                df = pd.read_csv(io.StringIO(csv_content))
            
            else:

                parquet_file_path = payload.get("parquet_file_path")

                if not parquet_file_path:
                    logger.exception(f"Parquet file not found {chunk_id}")
                    return {"status": "failed", "row_group": row}

                row = payload.get("row_group")

                if row is None:
                    raise ValueError(f"[JOB={job_id}] Missing row_group in payload")

                parquet_file = get_parquet_file(parquet_file_path)
                
                table = parquet_file.read_row_group(row)

                if table.num_rows == 0:
                    return {"status": "empty", "row_group": row}
                
                df = table.to_pandas()
            
        
        
            # -----------------------------
            # 3. CLEAN DATA (NLP)
            # -----------------------------

            df = clean_dataframe(df, columns, job_id,chunk_id)

            df = nlp_process(df,columns, job_id,chunk_id)

            # --------------------------------------
            # 4. Upload CLEANED CHUNK
            # --------------------------------------

            chunk_hash = hashlib.md5(chunk_id.encode()).hexdigest()

            chunk_key = f"chunks_post_nlp_processed_to_be_aggregated/{job_id}/{chunk_hash}.parquet"

            blob_client = get_blob_client(chunk_key)

            if blob_client.exists():
                logger.info(f"[JOB={job_id}] Already processed → {chunk_key}")
                return {"status": "success", "key": chunk_key}

            buffer = io.BytesIO() # parquet binary format 

            df.to_parquet(buffer, index = False)

            buffer.seek(0)
            
            for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                try:

                    buffer.seek(0)
                    blob_client.upload_blob(buffer, overwrite=False,content_type="application/x-parquet", timeout=60)
                    logger.info(f"[JOB={job_id}] Nlp successafully processed. Passing to aggregation - {chunk_id}")
                    return {"status": "success", "key": chunk_key}
        

                except AzureError:

                    if blob_client.exists():
                        logger.info(f"[JOB={job_id}] Chunk {chunk_id} already sucessfully NLP processed and uploaded in Azure")
                        return {"status": "success", "key": chunk_key}

                    logger.warning(f"[JOB={job_id}] Upload retry {attempt}")

                    if attempt == MAX_UPLOAD_RETRIES:
                        logger.exception(f"[JOB={job_id}] Upload failed for {chunk_id}")
                        return {"status": "failed", "key": chunk_id}

                    time.sleep(2 ** attempt)


        
        except (ConnectionError, TimeoutError, AzureError):
            logger.exception(f"[JOB={job_id}] Retryable error")

            if self.request.retries >= self.max_retries:
                return {"status": "failed", "key": chunk_id}
            raise
        
        except Exception as e:
            logger.exception(f"[JOB={job_id}] Nlp_processing_failed: {chunk_id}")
            return {"status": "failed", "key": chunk_id}
            
def contraction(texts,df,col):
        import contractions
        processed = [contractions.fix(x) for x in texts]
        df[col] = processed
        return df

def clean_dataframe(df, columns,job_id,chunk_id):

    import emoji, contractions

    emoji_cache = {}
    chat_cache = {}

    def normalize_text(x):
        try: 
            # ---------------- Chat + contractions cache ----------------
            if x not in chat_cache:
                temp = contractions.fix(x)
                temp = chat_conversion(temp)
                chat_cache[x] = temp

            x = chat_cache[x]

            if x not in emoji_cache:
                emoji_cache[x] = emoji.demojize(x).replace(":", " ").replace("_", " ")
           

            return emoji_cache[x]
        
        except Exception:
            return x
    try:
        for col in columns:  
            try:
                if col not in df.columns:
                     logger.warning(f"[JOB={job_id}] Column {col} not found")
                     continue
                
                df[col] = (
                            df[col]
                            .fillna("")
                            .astype(str)
                            .str.lower()
                            .str.replace(HTML_pattern, "", regex=True)
                            .str.replace(URL_pattern, "", regex=True))
                
                texts = df[col].tolist()
                processed = [normalize_text(x) for x in texts]
                df[col] = processed
                
                df[col] = (
                    df[col]
                    .str.strip()
                    .str.replace('"', '', regex=False)
                    .str.replace("'", '', regex=False)
                    .str.split()
                    .str.join(" ")
                )


            except Exception:
                logger.exception(f"[JOB={job_id}] Error while cleaning column {col} for file {chunk_id}")
                continue
        return df
    
    except Exception:
        logger.exception(f"Error while Data cleaning for file {chunk_id}")
        raise

def nlp_process(df, columns, job_id, chunk_id):
    try: 
        logger.info(f"[JOB={job_id}]  Inside nlp_process function")

        nlp = get_cached_nlp()
        model = get_cached_model()

        for col in columns:
            try:
                pos_results, dependency_parsing, ner, key_phrase = [], [] , [], []
            
                texts = df[col].astype(str)
                
                 # Full docs list
                unique_texts = texts.drop_duplicates().tolist()

                docs_unique = list(nlp.pipe(unique_texts, batch_size=512))
                doc_cache = dict(zip(unique_texts, docs_unique))

                embeddings = model.encode(unique_texts, batch_size=128)

                embedding_cache = dict(zip(unique_texts, embeddings))


                docs = [doc_cache[text] for text in texts]
                doc_embeddings = [embedding_cache[text] for text in texts]

                phrase_embedding_cache = {}

                for i, doc in enumerate(docs):
                    try:
               
                        # ---------------- POS ----------------
                        tagged_pos_text = " | ".join(f"{t.text} -> {t.pos_}" for t in doc if t.is_alpha)
                        
                        # ---------------- Dependency ----------------
                        tagged_dependency_text = " | ".join(f"{t.text} ->{t.pos_}->{t.dep_} -> {t.head.text}" for t in doc if t.is_alpha)
                        
                        # ---------------- NER ----------------
                        raw = [(e.text, e.label_) for e in doc.ents if len(e.text.strip()) > 0]
                        tagged_ner_text = " | ".join(f"{ent_text} -> {label}" for ent_text, label in raw) if raw else None

                        # ---------------- Keyphrase ----------------
                        embedding = doc_embeddings[i] 
                    
                        embedding = np.array(doc_embeddings[i]).reshape(1, -1)
                        
                        top_phrases = aspect_extraction_col(doc, model, embedding, phrase_embedding_cache)
            
        
                    #--------------------Appending--------------

                        pos_results.append(tagged_pos_text if tagged_pos_text else "No pos found" )
                        
                        dependency_parsing.append(tagged_dependency_text if tagged_dependency_text else "No dependency parsing found for given row")
                        
                        ner.append(tagged_ner_text if tagged_ner_text else "No entity relation found for given row")
                        
                        key_phrase.append(top_phrases if top_phrases else "No key phrase found for given row")

                    except Exception:
                        logger.exception(f"[JOB={job_id}] Row NLP error for file {chunk_id}")
                        pos_results.append(None)
                        dependency_parsing.append(None)
                        ner.append(None)
                        key_phrase.append(None)

                df[f"{col}__pos"] = pos_results
                df[f"{col}__dependency"] = dependency_parsing
                df[f"{col}__ner"] = ner
                df[f"{col}__key_phrase"] = key_phrase

            except Exception:
                logger.exception(f"[JOB={job_id}] Column NLP error for {col} in file {chunk_id}")
                df[col + "__error"] = "Processing failed"
                continue
        
        logger.info(f"[JOB={job_id}] NLP successfully done")
        return df
    
    except Exception:
        logger.exception(f"[JOB={job_id}] File-level NLP error for file {chunk_id}")
        raise

    


            









'''' 
import aioboto3
import pandas as pd
import io
import uuid

BUCKET = "test"
CHUNK_SIZE = 5000


async def _process_chunk_async(s3_path, columns):

    async with aioboto3.client("s3") as s3:

        response = await s3.get_object(Bucket=BUCKET, Key=s3_path)
        stream = response["Body"]

        output_buffer = io.StringIO()
        first_chunk = True

        rows = []
        header = None

        async for line in stream.iter_lines():

            decoded = line.decode("utf-8")

            if header is None:
                header = decoded.split(",")
                continue

            rows.append(decoded)

            if len(rows) >= CHUNK_SIZE:

                df = build_dataframe(header, rows)

                df = clean_dataframe(df, columns)

                write_chunk(output_buffer, df, first_chunk)

                first_chunk = False
                rows = []

        # flush remaining
        if rows:
            df = build_dataframe_safe(header, rows)
            df = clean_dataframe(df, columns)
            write_chunk(output_buffer, df, first_chunk)

        # upload result
        output_buffer.seek(0)

        output_key = f"processed/{uuid.uuid4().hex}.csv"

        await s3.put_object(
            Bucket=BUCKET,
            Key=output_key,
            Body=output_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )

        return output_key
def build_dataframe_safe(header, rows):

    import io
    import pandas as pd

    csv_buffer = io.StringIO()

    # reconstruct valid CSV
    csv_buffer.write(",".join(header) + "\n")

    for row in rows:
        csv_buffer.write(row + "\n")

    csv_buffer.seek(0)

    df = pd.read_csv(csv_buffer,engine="python)

    return d

def write_chunk(buffer, df, first_chunk):

    df.to_csv(
        buffer,
        index=False,
        header=first_chunk,
        mode="w" if first_chunk else "a"
    )
'''












  

    


