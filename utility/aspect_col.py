import numpy as np
from collections import Counter
from sklearn.metrics.pairwise import cosine_similarity

def aspect_extraction_col(doc, model, doc_embedding, phrase_embedding_cache):

       # Step 1: Word importance
    words = [token.text.lower() for token in doc if token.is_alpha and not token.is_stop]
    word_freq = Counter(words)
    max_freq = max(word_freq.values()) if word_freq else 1

       # Step 2: Phrase extraction
    phrases = [chunk.text.lower() for chunk in doc.noun_chunks if len(chunk.text.split()) > 1]

    if not phrases:
        return None

    # 🔥 Cached phrase embeddings
    phrase_embeddings = []
    for phrase in phrases:
        if phrase not in phrase_embedding_cache:
            phrase_embedding_cache[phrase] = model.encode([phrase])[0]
        phrase_embeddings.append(phrase_embedding_cache[phrase])

    phrase_embeddings = np.array(phrase_embeddings)

    similarities = cosine_similarity(phrase_embeddings, doc_embedding)

    hybrid_scores = []

    for phrase, sim in zip(phrases, similarities):

        phrase_words  = phrase.split()

        textrank_score = sum(word_freq.get(w, 0) for w in phrase_words)
        textrank_score = textrank_score / max_freq

        combined_score = 0.7 * sim[0] + 0.3 * textrank_score

        hybrid_scores.append((phrase, combined_score))

    hybrid_scores.sort(key=lambda x: x[1], reverse=True)

    return " | ".join(p[0] for p in hybrid_scores)