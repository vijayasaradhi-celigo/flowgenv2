import pandas as pd
df = pd.read_csv('../flowgenv2/all_connectors.csv')
connectors = df['connector'].unique().tolist()


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
    Compute the match score based on the number of matching ngrams
    between the query and a candidate string.
    Both strings are compared in lowercase.
    """
    query_ngrams = get_ngrams(query.lower(), n)
    candidate_ngrams = get_ngrams(candidate.lower(), n)
    return len(query_ngrams.intersection(candidate_ngrams))

def get_top_matches(query, options, n=3, top=5):
    """
    Compute the ngram match score for every option and return the top `top` probable matches.
    
    Parameters:
      query (str): The input string to match.
      options (list of str): The list of candidate strings.
      n (int): The ngram size to be used in matching (default is 3).
      top (int): The number of top matches to return (default is 5).
      
    Returns:
      list of tuples: Each tuple contains (option, score) sorted by score in descending order.
    """
    # Calculate match score for each option
    scored_options = [(option, ngram_match_score(query, option, n)) for option in options]
    # Sort options based on the score (highest first)
    scored_options.sort(key=lambda x: x[1], reverse=True)
    return scored_options[:top]

# Example usage:
if __name__ == "__main__":
    options = ["apple", "apply", "applet", "appple", "appliance", "ape", "banana", "cherry"]
    query = "appple"  # Intentional misspelling for demonstration
    top_matches = get_top_matches(query, options, n=3, top=5)
    
    print("Top 5 Matches (option -> score):")
    for match, score in top_matches:
        print(f"{match} -> {score}")

    query='''microsoft dynamics 365 business central items to shopify products this integration flow syncs microsoft dynamics 365 business central items as shopify products.'''
    options = connectors
    top_matches = get_top_matches(query, options, n=3, top=5)
    for match, score in top_matches:
        print(f"{match} -> {score}")


