import os,sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
import streamlit as st
import requests
import pandas as pd
from spacy import displacy
import streamlit.components.v1 as components
import spacy
from dataclasses import dataclass
import uuid, time

from logger.logging import setup_logging
logger = setup_logging()


BASE_URL = os.getenv("API_URL", "http://api_service:8000")
LONG_TIMEOUT = 30

st.set_page_config(page_title="Linguistic Intelligence", layout="wide")
st.header("Linguistic Intelligence Web Application")

if "job_id" not in st.session_state:
    st.session_state.job_id = None


@st.cache_resource
def load_spacy():
    return spacy.load("en_core_web_sm")

nlp = load_spacy()

@dataclass
class text_nlp_process:
    upload_url: str = None
    job_id: str = None
    object_name: str = None

    def create_job(self, input:str):
        try:
            # Here we are sendimg data using params
            res = requests.post(f"{BASE_URL}/jobs/", params = {"text": input}, headers={"Idempotency-Key": str(uuid.uuid4())}, timeout=LONG_TIMEOUT)
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

    def render_text_output(self,job):
        try:

            response = requests.get(f"{BASE_URL}/jobs/text_result/{job}", timeout=LONG_TIMEOUT)
            response.raise_for_status()

            result = response.json() 
            status = result.get("status","")

            if status in ["processing", "not_found"]:
                st.info("Processing... please wait ⏳")
                time.sleep(2)
                st.rerun()

            elif status == "completed":
                text_data = result.get("data",{})

                cleaned_text = text_data.get("text", {})
                linguistics = text_data.get("linguistics", {})
                metrics = text_data.get("metrics", {})

                st.subheader("Cleaned Text")
                clean_text = cleaned_text.get("clean", "No result found")
                st.markdown(clean_text)

                
                st.subheader("POS Distribution")
                st.markdown(linguistics.get("pos", "No result found"))

                pos_weight = linguistics.get("POS_weightage", {})
                if isinstance(pos_weight, dict) and pos_weight:
                    df = pd.DataFrame(pos_weight.items(), columns=["POS", "Percentage"])
                    st.bar_chart(df.set_index("POS"))

                st.subheader("Dependency Parsing")
                dep_text = linguistics.get("dependency", "No result found")
                st.markdown(dep_text)

                if dep_text != "No dependency parsing found":
                    doc = nlp(clean_text)
                    html = displacy.render(doc, style="dep")
                    components.html(html, height=500, scrolling=True)
                
                st.subheader("Negation Detection")
                st.markdown(linguistics.get("negation", "No result found"))

                st.subheader("Keyword Stopword Ratio")
                st.markdown(metrics.get("keyword_stopword_ratio", "No result found"))

                st.subheader("Readability Metrics")
                st.markdown(metrics.get("readability", "No result found"))

                st.subheader("Named Entity Recognition")
                st.markdown(text_data.get("entities", "No result found"))

                st.subheader("Lemmatized Text")
                st.markdown(cleaned_text.get("lemmatized", "No result found"))

                st.subheader("Keyphrases")
                st.markdown(text_data.get("keyphrases", "No result found"))

                st.subheader("Morphological Analysis")
                st.markdown(linguistics.get("morphology", "No result found"))

            
            elif status == "failed":
                st.write("Text Processing failed")
                st.stop()
   
            else:
                st.write("Processing... please wait ⏳")
                time.sleep(3)
                st.rerun()

        except requests.exceptions.RequestException:
            logger.exception("Text processing failed")
            st.write("Text processing failed")
            
        except ValueError:
            logger.exception("Invalid JSON response from backend")
            if response is not None:
                    logger.error(f"Response content : {response.text}")
            st.write("Backend returned invalid response")




    def main(self):

        try:
            with st.form("text_form"):
                user_input = st.text_input("Enter text")
                submitted = st.form_submit_button("Submit")

            if submitted:


                if not user_input:
                    st.error("Please enter text.")
                    logger.warning("Empty user input submitted")
                    st.stop()


                logger.info(f"Sending user_iput to backend -> {user_input}")

                with st.spinner("Running NLP pipeline..."):
                    job_id = self.create_job(user_input)

                    if not job_id:
                        st.error("Job creation failed")
                        return 

                    st.session_state["job_id"] = job_id
                    st.success(f"Job created: {job_id}")

            job_id = st.session_state.get("job_id")
            if job_id:
                logger.info(f"job-is {job_id}")
                self.render_text_output(job_id)

        except Exception:
            logger.exception("Form submisison failed")



if __name__ == "__main__":
    text_nlp_process().main()

    

       
