def get_ngrams(text, n):
    """
    Generate a set of ngrams from the given text.
    If the text is shorter than n, returns the text itself as a single "ngram".
    """
    if len(text) < n:
        return {text}
    return {text[i:i+n] for i in range(len(text) - n + 1)}

def ngram_match_score(query, candidate, n):
    """
    Compute the score based on the number of matching ngrams between query and candidate.
    Both strings are converted to lowercase to ensure case-insensitive matching.
    """
    query_ngrams = get_ngrams(query.lower(), n)
    candidate_ngrams = get_ngrams(candidate.lower(), n)
    # Count the common ngrams
    return len(query_ngrams.intersection(candidate_ngrams))

def get_best_match(query, options, n=3):
    """
    Returns the option with the highest number of matching character ngrams with the query.
    
    Parameters:
      query (str): The input string to match.
      options (list of str): The list of candidate strings.
      n (int): The ngram size. (Default is 3.)
      
    Returns:
      str or None: The best matching string, or None if no significant match is found.
    """
    best_match = None
    best_score = 0

    for option in options:
        score = ngram_match_score(query, option, n)
        if score > best_score:
            best_score = score
            best_match = option

    # Return None if no common ngrams were found
    return best_match if best_score > 0 else None

# Example usage:
if __name__ == "__main__":
    options = ["apple", "banana", "cherry", "date"]
    query = "appple"  # Notice the misspelling
    best_match = get_best_match(query, options, n=3)
    print("Best match:", best_match)
    query= 'shopify refund to microsoft dynamics 365 business central sales credit memo (add) syncs refunds from shopifyto microsoft dynamics 365 business central.  this is a scheduled flow. for more information, see'
    options =['shopify', 'microsoftbusinesscentral', 'microsoftdynamics365',
              'microsoftdynamics365businesscentral']
    best_match = get_best_match(query, options)
    print("Best match:", best_match)
