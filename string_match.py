import re

def preprocess(text):
    """
    Lowercase the text and remove non-alphanumeric characters.
    This ensures uniformity for both LCS and n-gram comparisons.
    """
    return re.sub(r'[^a-z0-9]', '', text.lower())

def longest_common_substring(s1, s2):
    """
    Compute the length of the longest common substring between s1 and s2 using dynamic programming.
    """
    m, n = len(s1), len(s2)
    # Create a matrix to store lengths of longest common suffixes.
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    max_len = 0
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
                if dp[i][j] > max_len:
                    max_len = dp[i][j]
            else:
                dp[i][j] = 0
    return max_len

def lcs_similarity(s1, s2):
    """
    Compute a normalized LCS similarity score between two strings.
    The score is the length of the LCS divided by the length of the longer string.
    """
    if not s1 or not s2:
        return 0
    lcs_len = longest_common_substring(s1, s2)
    return lcs_len / max(len(s1), len(s2))

def get_char_ngrams(s, n):
    """
    Return a list of character n-grams for the string.
    If the string is shorter than n, returns an empty list.
    """
    return [s[i:i+n] for i in range(len(s)-n+1)]

def jaccard_similarity(set1, set2):
    """
    Compute the Jaccard similarity between two sets.
    """
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

def match_string(target, string_list, char_ngram_n=3, weight_lcs=0.5, weight_jaccard=0.5):
    """
    For a given target string, compute a combined similarity score with each string in string_list.

    The function computes:
      - LCS similarity: Normalized longest common substring similarity.
      - Jaccard similarity: Jaccard similarity based on character n-grams.

    The final score is a weighted average of these two measures.

    Parameters:
      target (str): The target string.
      string_list (list): A list of candidate strings.
      char_ngram_n (int): The size of the character n-grams (default is 3).
      weight_lcs (float): Weight for the LCS similarity.
      weight_jaccard (float): Weight for the Jaccard similarity.

    Returns:
      best_match (str): The string from string_list with the highest combined similarity score.
      results (list): A list of tuples (candidate string, score) for all comparisons.
    """
    target_processed = preprocess(target)
    target_ngrams = set(get_char_ngrams(target_processed, char_ngram_n))
    
    best_score = -1
    best_match = None
    results = []
    
    for candidate in string_list:
        candidate_processed = preprocess(candidate)
        candidate_ngrams = set(get_char_ngrams(candidate_processed, char_ngram_n))
        
        # Compute the LCS similarity.
        lcs_sim = lcs_similarity(target_processed, candidate_processed)
        # Compute the Jaccard similarity of character n-grams.
        jaccard_sim = jaccard_similarity(target_ngrams, candidate_ngrams)
        
        # Combine the scores using the provided weights.
        score = weight_lcs * lcs_sim + weight_jaccard * jaccard_sim
        results.append((candidate, score))
        
        if score > best_score:
            best_score = score
            best_match = candidate
            
    return best_match, results

# Example usage
if __name__ == "__main__":
    target = "Customer"
    string_list = [
        "Customers", 
        "Client", 
        "Consumer", 
        "Buyer"
    ]
    
    best_match, scores = match_string(target, string_list, char_ngram_n=3, weight_lcs=0.5, weight_jaccard=0.5)
    print("Best match:", best_match)
    print("Scores for each string:")
    for s, score in scores:
        print(f'"{s}" --> {score:.3f}')

