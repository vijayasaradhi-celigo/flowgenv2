def get_ngrams(text, n):
    """
    Generate a set of ngrams from the given text.
    If the text is shorter than n, returns the text itself as a single ngram.
    """
    if len(text) < n:
        return {text}
    return {text[i:i+n] for i in range(len(text) - n + 1)}

def normalized_ngram_score(query, candidate, n=3):
    """
    Compute a normalized match score between 0 and 1 based on the Jaccard similarity
    of the ngram sets from the query and candidate strings.
    
    Parameters:
      query (str): The input string.
      candidate (str): The candidate string.
      n (int): The ngram size (default is 3).
      
    Returns:
      float: A score between 0 and 1, where 1 means a perfect match.
    """
    query_ngrams = get_ngrams(query.lower(), n)
    candidate_ngrams = get_ngrams(candidate.lower(), n)
    
    intersection = query_ngrams.intersection(candidate_ngrams)
    union = query_ngrams.union(candidate_ngrams)
    
    # Avoid division by zero (shouldn't happen unless both strings are empty)
    if not union:
        return 1.0 if not query_ngrams and not candidate_ngrams else 0.0
    
    return len(intersection) / len(union)

def get_top_candidate_scores(query, candidates, top_k=5):
    # Compute the normalized ngram score for each candidate
    scores = {candidate: normalized_ngram_score(query, candidate) for candidate in candidates}
    # Sort the scores in descending order
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_scores[:top_k]

# Example usage:
if __name__ == "__main__":
    query = "appple"  # Intentional misspelling
    candidate = "apple"
    candidates = ["banana", "orange", "grape", "apple", "kiwi", "strawberry"]

    
    scores = get_top_candidate_scores(query, candidates, top_k=3)
    for score in scores:
        print(score)

