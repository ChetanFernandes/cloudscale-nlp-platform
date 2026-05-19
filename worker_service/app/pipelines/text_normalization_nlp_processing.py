
from utility.chat_words import chat_conversion
from utility.aspect_text import aspect_extraction_text
import logging
log = logging.getLogger(__name__)

class TextProcessing():
    pass

    @staticmethod
    def pre_process_text(text):
        import emoji
        import contractions
        import re
        try:

            if not isinstance(text,str):
                return text
            
            #strip and lower case
            text = text.strip().lower()

            #Remove HTML page
            HTML_pattern = re.compile(r'<.*?>')
            text = HTML_pattern.sub("",text)

            # Remove URLs
            URL_pattern = re.compile(r'https?://\S+|www\.\S+')
            text = URL_pattern.sub("",text)

            # Contraction Expansion 🔥 ADD THIS
            text = contractions.fix(text)

            # Chat_handling
            text = chat_conversion(text)

            # Covert emojis to words
            text = emoji.demojize(text)
            text = text.replace(":"," ").replace("_"," ")
            
            # Remove punctuation
            text = text.strip().replace('"', '').replace("'", '')
            #text = re.sub(f"[{re.escape(string.punctuation)}]", "", text) # Use ful for POS tagging, De

            # Remove whitespaces
            text = " ".join(text.split())
            return text
    
        except Exception:
            raise


# ---------------- POS Tagging ---------------- #

    @staticmethod
    def pos_tagging(doc):
        try:
            POS_weightage = {}

            tagged_pos_text = " , ".join(f"{token.text} -> {token.pos_}" for token in doc if token.is_alpha)
            
            length = sum(1 for token in doc if token.is_alpha)

            if length > 0:
                POS_weightage["Pronoun"]   = int((sum(1 for t in doc if t.pos_ == "PRON") / length) * 100)
                POS_weightage["Adjective"] = int((sum(1 for t in doc if t.pos_ == "ADJ") / length) * 100)
                POS_weightage["Noun"]      = int((sum(1 for t in doc if t.pos_ == "NOUN") / length) * 100)
                POS_weightage["Verb"]      = int((sum(1 for t in doc if t.pos_ == "VERB") / length) * 100)

           

            return tagged_pos_text if tagged_pos_text else "No POS tags found", POS_weightage
        
        except Exception:
            raise

# ---------------- Dependency Parsing ---------------- #  
    @staticmethod
    def dependency_parsing(doc):
        try:
            tagged_dependency_text = " , ".join(f"{token.text} ->{token.dep_} -> {token.head.text}" for token in doc if token.is_alpha)
            return tagged_dependency_text if tagged_dependency_text else "No dependnecy parsing found"
        except Exception:
            raise
    
            
# ---------------- NER ---------------- #    
    @staticmethod
    def NER(doc):
        try:
            raw  =  [(e.text, e.label_) for e in doc.ents]
            return " , ".join(f"{text} -> {label}" for text, label in raw) if raw else "No entity relation found for given input"
        except Exception:
            raise
            
# ---------------- Lemetized ---------------- # 
 
    @staticmethod
    def lemmatize_texts(doc):
        try:
            words = " ".join(token.lemma_ for token in doc if not token.is_stop and token.is_alpha)
            return words if words else "None"
        except Exception:
            raise
    


# ---------------- Key Phrase extraction ---------------- #
    @staticmethod
    def key_phrase_extraction(doc, model):
        try:

            key_phrases = aspect_extraction_text(doc,model)
            return key_phrases if key_phrases else "None"
        except Exception:
            raise


# ---------------- Morphological Analysi ---------------- #
    @staticmethod
    def Morphological_analysis(doc):
        try:
            morph_info =  [
                    (token.text, str(token.morph))
                    for token in doc if token.is_alpha
                ]
            return " , ".join(f"{text} -> {morph}" for text, morph in morph_info) if morph_info else "None"

        except Exception:
            raise
        
# ----------------Negation Detection ---------------- #

    @staticmethod
    def negation_detection(doc):
        try:
            negations = []

            for token in doc:
                if token.dep_ == "neg":
                    negations.append(f"{token.text} -> {token.head.text}")

            return negations if negations else []

        except Exception:
            raise



# ----------------Keyword vs Stopword Ratio ⭐⭐ (Quality / Informativeness Signal) ---------------- #


    @staticmethod
    def keyword_stopword_ratio(doc):
        try:
            #doc = nlp(text)
            key_stop_ratio = {}

            keywords = sum(1 for token in doc if token.is_alpha and not token.is_stop)
            stopwords = sum(1 for token in doc if token.is_alpha and token.is_stop)
            ratio = round(keywords / (stopwords + 1), 2) # avoid division by zero
 

            key_stop_ratio["keywords"] = keywords
            key_stop_ratio["stopwords"] = stopwords
            key_stop_ratio["ratio"] = ratio

            return " , ".join(f"{k} -> {v}" for k, v in key_stop_ratio.items()) if key_stop_ratio else "None"

        except Exception:
            raise



# ----------------Readability / Complexity Metrics ⭐⭐⭐ ---------------- #


    @staticmethod
    def readability_metrics(doc):
        try:
            readable_metrics = {}
            sentences = list(doc.sents)

            words = [token.text for token in doc if token.is_alpha]

            avg_sentence_length = len(words) / len(sentences) if sentences else 0
            avg_word_length = sum(len(word) for word in words) / len(words) if words else 0

            complexity_score = round((avg_sentence_length * 0.6) + (avg_word_length * 0.4), 2)
            
            readable_metrics["avg_sentence_length"] = round(avg_sentence_length, 2)
            readable_metrics["avg_word_length"] = round(avg_word_length, 2)
            readable_metrics["complexity_score"] = complexity_score

            return " , ".join(f"{k} -> {v}" for k, v in readable_metrics.items()) if readable_metrics else "None"


        except Exception:
            raise






    


    
    













