import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

def find_most_similar_json(json_list):
    # Convert JSON objects to string representations
    json_strings = [json.dumps(obj, sort_keys=True) for obj in json_list]

    # Vectorize the JSON strings
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(json_strings)

    # Compute cosine similarity matrix
    similarity_matrix = cosine_similarity(tfidf_matrix)

    # Compute average similarity for each JSON
    avg_similarities = similarity_matrix.mean(axis=1)

    # Find the index of the JSON with the highest average similarity
    most_similar_index = np.argmax(avg_similarities)

    return json_list[most_similar_index]

# Example usage
json_objects = [
    {"name": "Alice", "age": 25, "city": "New York"},
    {"name": "Bob", "age": 30, "city": "New York"},
    {"name": "Charlie", "age": 25, "city": "New York"},
    {"name": "David", "age": 26, "city": "Boston"},
]
if __name__ == '__main__':
    most_similar_json = find_most_similar_json(json_objects)
    print(most_similar_json)

