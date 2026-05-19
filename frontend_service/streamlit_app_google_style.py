import sys, os, time, uuid, requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import streamlit as st
from dataclasses import dataclass
from logger.logging import setup_logging
logger = setup_logging()

from dotenv import load_dotenv
load_dotenv()

# -------------------- CONFIG -------------------- #
BASE_URL = os.getenv("API_URL", "http://api_service:8000")
SHORT_TIMEOUT = 5
LONG_TIMEOUT = 60
MAX_FILE_SIZE_MB = 100


# ---------------- SESSION STATE ---------------- #
if "jobs" not in st.session_state:
    st.session_state.jobs = []

if "processed_files" not in st.session_state:
    st.session_state.processed_files = set()

if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0

if "poll_interval" not in st.session_state:
    st.session_state.poll_interval = 5

if "column_retry_count" not in st.session_state:
    st.session_state.column_retry_count = 0

#if "extracting_columns" not in st.session_state:
    #st.session_state.extracting_columns = True


# -------------------- SERVICE LAYER -------------------- #
@dataclass
class JobService:
    upload_url: str = None
    job_id: str = None
    object_name: str = None

    def generate_upload_url(self, file_name):
        try:
            res = requests.get(f"{BASE_URL}/azure_storage/upload-url", params={"object_name": file_name}, timeout = LONG_TIMEOUT)
            res.raise_for_status()
            data = res.json()
            self.upload_url = data.get("upload_url")
            self.object_name = data.get("object_name")

            if not self.upload_url or not self.object_name:
                logger.error("Invalid upload URL response")
                return False

            return True
        
        except requests.exceptions.RequestException:
            logger.exception("API URL rendering failed")
            st.error("Failed to connect to backend.")
            return 
                
        except ValueError:
            logger.exception("Invalid JSON response from backend")
            if res is not None:
                    logger.error(f"Response content : {res.text}")
            st.error("Backend returned invalid response.")
            return

    def upload_file(self, file):

        try:
            file.seek(0)
            headers = {"x-ms-blob-type": "BlockBlob"}
            res = requests.put(self.upload_url,data=file,headers=headers,timeout=LONG_TIMEOUT)
            res.raise_for_status()

            logger.info("File Successfully  uploaded")
            st.success("File uploaded")
            return True
        
        except requests.exceptions.RequestException:
            logger.exception("File upload failed")
            st.error("File upload failed")
            return # Same as return None

    def create_job(self):
        try:
            # Here we are sendimg data using params
            res = requests.post(f"{BASE_URL}/jobs/", params = {"object_name": self.object_name}, headers={"Idempotency-Key": str(uuid.uuid4())}, timeout=LONG_TIMEOUT)
            res.raise_for_status()
            data = res.json()
            self.job_id = data.get("job_id", "None")
            logger.info(f'job successfully created with id -> {self.job_id}')
            return self.job_id
        
        except requests.exceptions.RequestException:
            logger.exception("Create job failed")
            st.error("Job creation failed")
            return 

        except ValueError:
            logger.exception("Invalid JSON response from backend")
            if res is not None:
                    logger.error(f"Response content : {res.text}")
            st.error("Backend returned invalid response.")
            return 

    def get_columns(self, job_id):
        try:
            res = requests.get(f"{BASE_URL}/jobs/{job_id}/columns",timeout=LONG_TIMEOUT)
            res.raise_for_status()
            return res.json()
        
        except requests.exceptions.RequestException:
            logger.exception("Columns Extraction failed")
            return {"status": "column_extraction_failed", "columns": []}
            
        except ValueError:
            logger.exception("Invalid JSON response from backend")
            if res is not None:
                    logger.error(f"Response content : {res.text}")
            return {"status": "column_extraction_failed", "columns": []}


    def process_columns(self, job_id, columns):
        try:
            # Here we are sending data using json
            res = requests.post(f"{BASE_URL}/jobs/nlp_processing", json={"job_id": job_id, "user_selected_columns": columns},timeout=LONG_TIMEOUT)
            res.raise_for_status()
            return res.json().get("status")
        
        except requests.exceptions.RequestException:
            logger.exception("Column processing failed")
            st.error("Failed to process selected columns.")
            return None

        except ValueError:
            logger.exception("Column processing failed")
            if res is not None:
                    logger.error(f"Response content : {res.text}")
            st.error("Failed to process selected columns")
            return None

    def get_job_status(self, job_id):
        try:
            res = requests.get(f"{BASE_URL}/jobs/{job_id}",timeout=LONG_TIMEOUT)
            res.raise_for_status()
            return res.json().get("job_details")
       
        except requests.exceptions.RequestException:
            logger.exception("Checking job status failed")
            st.error("Checking job status failed")
            return {"status": "failed"}

        except ValueError:
            logger.exception("Invalid JSON response from backend")
            if res is not None:
                    logger.error(f"Response content : {res.text}")
            st.error("Backend returned invalid response.")
            return None
        

    def fetch_download_url(self,job_id):
        try:
            res = requests.get(f"{BASE_URL}/jobs/{job_id}/download_url", timeout=LONG_TIMEOUT)
            res.raise_for_status()
            return res.json().get("url")
        except Exception:
            st.error(f"Failed to get download URL for job {job_id}")
            return None
                
              
service = JobService()

# -------------------- STREAMLIT UI -------------------- #
st.set_page_config(page_title="Linguistic Intelligence", layout="wide")

# -------------------- UI -------------------- #
st.set_page_config(page_title="Multi File NLP", layout="wide")
st.title("📊 Multi-File NLP Processing")


# ---------------- RESET BUTTON ---------------- #
if st.button("🔄 Start New Upload Session"):
    st.session_state.jobs = []
    st.session_state.processed_files = set()
    st.session_state.uploader_key += 1
    st.rerun()



# -------------------- FILE UPLOAD -------------------- #
uploaded_files = st.file_uploader("Upload file(s)", type=["xlsx", "csv"], accept_multiple_files=True, key=f"uploader_{st.session_state.uploader_key}")

if uploaded_files:
    for file in uploaded_files:
     
        if file.name in st.session_state.processed_files:
            continue

        if file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
            st.error(f"❌ {file.name} is too large")
            continue


        with st.spinner(f"Uploading {file.name}..."):

            if not service.generate_upload_url(file.name):
                st.error(f"Failed to get upload URL: {file.name}")
                continue

            if not service.upload_file(file):
                st.error(f"Upload failed: {file.name}")
                continue


            job_id = service.create_job()

            if not job_id:
                st.error(f"Job creation failed: {file.name}")
                continue

                # Store job
        st.session_state.jobs.append({
            "file_name": file.name,
            "job_id": job_id,
            "columns": None,
            "selected_columns": None,
            "status": "uploaded",
            "download_url": None,
            "backend_statue":None,
        })

        st.session_state.processed_files.add(file.name)
        st.success(f"✅ Job created: {file.name}")


# ---------------- UI ---------------- #
st.subheader("📁 Uploaded Files")

for job in st.session_state.jobs:
    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])

    col1.write(job["file_name"])
    


# -------------------- COLUMN EXTRACTION -------------------- #
    if job["status"] == "uploaded":
        
        if col3.button("Extract Columns", key=f"extract_{job['job_id']}"):
            st.session_state["extracting_columns"] = True
            st.session_state.column_retry_count = 0

    if st.session_state.get("extracting_columns"):

            result = service.get_columns(job["job_id"])
            status = result["status"]
            columns = result["columns"]

            if status == "columns_extracted":
                st.success("Columns extracted successfully.Select columns to move to next step")
                job["columns"] = columns
                job["status"] = "columns_extracted"

                st.session_state["extracting_columns"] = False
                st.session_state.column_retry_count = 0
                st.rerun()

            elif status in ["column_extraction_failed"]:
                st.error("Column extraction failed")
                st.session_state.column_retry_count = 0
                st.session_state["extracting_columns"] = False
                st.stop()

            elif status in ["Invalid_file"]:
                st.error("Invalid_file")
                st.session_state["extracting_columns"] = False
                st.session_state.column_retry_count = 0
                st.stop()

            elif status in ["Excel_parquet_conversion_failed"]:
                st.error("Excel_parquet_conversion_failed")
                st.session_state["extracting_columns"] = False
                st.session_state.column_retry_count = 0
                st.stop()

            elif status in ["job_not_found"]:
                st.error("job_not_found")
                st.session_state["extracting_columns"] = False
                st.session_state.column_retry_count = 0
                st.stop()


            elif status == "no_columns_found":
                st.warning("⚠️ No columns found in file")
                st.session_state["extracting_columns"] = False
                st.session_state.column_retry_count = 0
                st.stop()

            else:
                st.session_state.column_retry_count += 1
                if st.session_state.column_retry_count > 10:
                    st.error("⏱ Timeout: Column extraction taking too long")
                    st.session_state["extracting_columns"] = False
                    st.stop()

                st.info("Extracting columns...")
                time.sleep(2)
                st.rerun()


# -------------------- COLUMN SELECTION -------------------- #
    if job["status"] == "columns_extracted":

        selected = col1.multiselect("Select columns",options = job["columns"], key=f"cols_{job['job_id']}")
        
        if col3.button("Process", key=f"process_{job['job_id']}"):

            if not selected:
                st.warning("Select columns first")
                st.stop()

            service.process_columns(job["job_id"], selected)
            job["status"] = "passing_for_nlp_processing"




# -------------------- processing-------------------- #
    if job["status"] == "passing_for_nlp_processing":

        status_data = service.get_job_status(job["job_id"])
        
        if not status_data:
            st.error("Failed to fetch job status")
            #continue
        
        st.session_state.column_retry_count = 0
        status = status_data["status"]
        job["backend_status"] = status

        if status == "final_zip_file_uploaded_Azure":
            job["status"] = "final_zip_file_uploaded_Azure"
            

            # ensure download always set
            if not job.get("download_url"):
                job["download_url"] = service.fetch_download_url(job["job_id"])

            st.session_state.column_retry_count = 0


        elif status in ["system_failed","aggregation_process_failed","Zipping_failed"]:
            job["status"] = "failed"

        else:
            st.session_state.column_retry_count += 1

            if st.session_state.column_retry_count > 20:
                st.error("⏱ Timeout: Nlp processing taking too long")
        
                st.stop()

            time.sleep(4)
            st.rerun()

    

# -------------------- STATUS DISPLAY (ALWAYS LAST) -------------------- #
# col2 = main status
    if job["status"] == "nlp_process_completed":
        col2.write("completed")
    elif job["status"] == "failed":
        col2.error("failed")
    elif job["status"] == "processing":
        col2.warning("processing")
    else:
        col2.write(job["status"])

# col3 = backend step (optional)
    if job.get("backend_status"):
        col3.write(job["backend_status"])

# -------------------- DOWNLOAD BUTTON -------------------- #
    if job["status"] == "final_zip_file_uploaded_Azure" and job.get("download_url"):
        col4.markdown(f"[⬇️ Download]({job['download_url']})",unsafe_allow_html=True)
            


# ---------------- AUTO REFRESH ---------------- #
if any(job["status"] == "passing_for_nlp_processing" for job in st.session_state.jobs):
    time.sleep(5)
    st.rerun()

