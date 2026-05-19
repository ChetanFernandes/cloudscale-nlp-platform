nlp = None
model = None


def init_nlp():
    global nlp
    if nlp is None:
        import spacy
        nlp = spacy.load("en_core_web_sm")


def get_nlp():
    global nlp
    if nlp is None:
        print("🔄 Loading spaCy model...")
        init_nlp()
    return nlp


def init_model():
    global model
    if model is None:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer('all-MiniLM-L6-v2')


def get_model():
    global model
    if model is None:
        print("🔄 Loading SentenceTransformer model...")
        init_model()
    return model

