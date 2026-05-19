from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx


def aspect_extraction_text(doc,model):

    key_phrases = []

    #doc = nlp(text)

    words = [token.text.lower() for token in doc if token.is_alpha and not token.is_stop]
    graph = nx.Graph() # build edges

    # For each wprds you connect to next three words
    for i, word in enumerate(words):
        for j in range(i+1, min(i+4, len(words))):
            graph.add_edge(word, words[j])

    if len(graph.nodes) == 0:
        key_phrases.append(None)
 

    scores = nx.pagerank(graph)

    phrases = [chunk.text.lower() for chunk in doc.noun_chunks if len(chunk.text.split()) > 1]

    if not phrases:
        return None
  

    doc_embedding = model.encode([doc.text])
    phrase_embeddings = model.encode(phrases)


    similarities = cosine_similarity(phrase_embeddings, doc_embedding)

    hybrid_scores = []

    for phrase, sim in zip(phrases, similarities):

        words = phrase.split()

        textrank_score = sum(scores.get(w, 0) for w in words) # Add importance of words inside phrase

        max_score = max(scores.values()) if scores else 1 # Convert score → relative scale
        textrank_score = textrank_score / max_score 

        combined_score = 0.7 * sim[0] + 0.3 * textrank_score

        hybrid_scores.append((phrase, combined_score))

    hybrid_scores.sort(key=lambda x: x[1], reverse=True)

    top_phrases = " , ".join(p[0] for p in hybrid_scores)
    return top_phrases