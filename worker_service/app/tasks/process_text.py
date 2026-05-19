import os,json, redis,gzip
from worker_service.app.celery_worker_cpu import celery_app
from utility.nlp import get_nlp, get_model
from worker_service.app.pipelines.text_normalization_nlp_processing import TextProcessing
from data_layer.database import SessionLocal
from data_layer.repositories.job_repository import JobRepository
from datetime import datetime, timezone

from logger.logging import setup_logging
logger = setup_logging()

from dotenv import load_dotenv
load_dotenv()


redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(redis_url)

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

db = None
job = None

@celery_app.task(name="worker_service.app.tasks.process_text.text_nlp_processing", bind=True, autoretry_for=(ConnectionError, TimeoutError), retry_backoff=True, retry_kwargs={"max_retries": 1})
def text_nlp_processing(self,job_id,text):
    try:
        logger.info(f"Inside text processing function for jobb with id {job_id}")
        db = SessionLocal()
        job = JobRepository.get_job(db, job_id)

        if not job:
            logger.error(f"[JOB={job_id}] Job not found while marking Text processing")
            return
        
        job.status = "Text_processing"
        logger.info("DB updated as : Text_processing")
        db.commit()

        r.setex(f"job_status:{job_id}", 3600, "processing")

        status = r.get(f"job_status:{job_id}")

        logger.info(f"Update Redis status: {status.decode() if status else None}")

        logger.info("Cleaning Text")
        clean_text = TextProcessing.pre_process_text(text)
        logger.info(f"Cleaned text -> {clean_text}")

        nlp = get_cached_nlp()
        model = get_cached_model()

        doc = nlp(clean_text)

        logger.info("Calling class TextProcessing")
        Tagged_pos_text,POS_weightage  = TextProcessing.pos_tagging(doc)
    
        logger.info("POS tagging completed")
        logger.info(f"Extracted {len(Tagged_pos_text)} tokens")

        dependencyparsing = TextProcessing.dependency_parsing(doc)
        logger.info("Dependency parsing completed")

        negationdetection = TextProcessing.negation_detection(doc)
        logger.info("negationdetection completed")

        keywordstopword_ratio = TextProcessing.keyword_stopword_ratio(doc)
        logger.info(f"keywordstopword_ratio completed")

        readabilitymetrics = TextProcessing.readability_metrics(doc)
        logger.info(f"readability_metrics-> {readabilitymetrics}")

        named_entity_recognition = TextProcessing.NER(doc)
        logger.info(f"named_entity_recognition-> {named_entity_recognition}")

        lemmatized  = TextProcessing.lemmatize_texts(doc)
        logger.info(f"Lemmatized_text -> {lemmatized}")
        
        doc_embedding = model.encode([text])
        keyphrase_extraction  = TextProcessing.key_phrase_extraction(doc, model)
        logger.info(f"key_phrase-extraction -> {keyphrase_extraction}")

        morph_analysis = TextProcessing.Morphological_analysis(doc)
        logger.info(f"Morphological_analysis -> {morph_analysis}")


        result =  {
            "text": {
                "clean": clean_text,
                "lemmatized": lemmatized
            },
            "linguistics": {
                "pos": Tagged_pos_text,
                "POS_weightage":POS_weightage,
                "dependency": dependencyparsing,
                "morphology": morph_analysis,
                "negation": negationdetection
            },
            "metrics": {
                "keyword_stopword_ratio": keywordstopword_ratio,
                "readability": readabilitymetrics
            },
            "entities": named_entity_recognition,
            "keyphrases": keyphrase_extraction
        } 

        compressed = gzip.compress(json.dumps(result, default=str).encode())
     
        job.status = "Text_processing_completed"
        logger.info("DB updated as Text_processing_completed")

        job.completed_at = datetime.now(timezone.utc)


        if job.created_at and job.completed_at:
            job.total_time_taken = round((job.completed_at - job.created_at).total_seconds()/60, 2 )

        db.commit()
        db.refresh(job)

        # Redis AFTER DB success
        try:
            r.setex(f"job_result:{job_id}", 3600, compressed)
            r.setex(f"job_status:{job_id}", 3600, "completed")
            status = r.get(f"job_status:{job_id}")
            logger.info(f"Update Redis status: {status.decode() if status else None}")

        except Exception:
            logger.warning(f"[JOB={job_id}] Redis write failed, but processing completed")
        
        return result

    except Exception:
        try:

            r.setex(f"job_status:{job_id}", 3600, "failed")
            status = r.get(f"job_status:{job_id}")
            logger.info(f"Update Redis status: {status.decode() if status else None}")

        except Exception:
            logger.warning("Failed to update Redis status")

        logger.exception(f"Text processing failed - {text}")

        if db:
            try:
                if job:
                    job.status = "Text_processing_failed"
                    db.commit()
                    db.refresh(job)
                    logger.info("DB updated as Text_processing_failed")
            except Exception:
                logger.exception("Failed to update DB status")
        raise
        
    finally:
        if db:
            db.close()