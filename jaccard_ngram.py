def compute_ngram_jaccard_similarities(word1, word2):
    """
    Compute the Jaccard similarity for 1-grams, 2-grams, 3-grams, and 4-grams
    between two words.

    Parameters:
        word1 (str): The first word.
        word2 (str): The second word.

    Returns:
        dict: A dictionary mapping n-gram size to its Jaccard similarity.
              Example: {"1-gram": 0.5, "2-gram": 0.3, "3-gram": 0.0, "4-gram": 0.0}
    """
    
    def ngram_set(s, n):
        # Generate set of n-grams from string s.
        return {s[i:i+n] for i in range(len(s) - n + 1)} if len(s) >= n else set()
    
    similarities = {}
    
    for n in range(1, 5):
        set1 = ngram_set(word1, n)
        set2 = ngram_set(word2, n)
        union = set1 | set2  # Union of n-gram sets
        
        # Handle case where both sets are empty (e.g., when word length < n)
        if not union:
            sim = 1.0
        else:
            sim = len(set1 & set2) / len(union)  # Intersection divided by union
        
        similarities[f"{n}-gram"] = sim
        
    return similarities

# Example usage:
if __name__ == "__main__":
    word_a = "hello"
    word_b = "hallo"
    results = compute_ngram_jaccard_similarities(word_a, word_b)
    for ngram, sim in results.items():
        print(f"{ngram} similarity: {sim:.2f}")

