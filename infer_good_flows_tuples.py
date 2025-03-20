import sqlite3
import json
from rich.progress import track
import sys
import pandas as pd
from rich import print
#from best_string_match import get_best_match
#from all_candidates_score import get_top_matches
#from ngram_score import get_top_candidate_scores
from search_exports_imports_typesense import main as search_func

# Connect to the database
conn = sqlite3.connect('flowgen.db')

connector_df = pd.read_csv("all_connectors.csv")
print(connector_df)
connectors = connector_df['connector'].unique().tolist()
print("Found ", len(connectors), " connectors")
resources_df = pd.read_csv("all_resources2.csv")
print(resources_df)
resources = resources_df['resource'].tolist()
print("Found ", len(resources), " resources")

connector_resource_dict = {}
for connector in connectors:
    resources = resources_df[resources_df['connector'] == connector]['resource'].tolist()
    connector_resource_dict[connector] = resources
#print(connector_resource_dict)

def main():
    fp = open('good_flows.jsonl', 'r')
    for line in fp:
        flow = json.loads(line)
        flow_id = flow['flow_id']
        flow_tuple = process_flow(flow_id)
        if flow_tuple:
            print(flow_tuple)


def process_flow(flow_id):
    flow_obj = get_flow_obj(flow_id)
    flow_name = flow_obj['name']
    flow_description = flow_obj.get('description', '')

    flow_name_desc = flow_name + " " + flow_description
    flow_name_desc = flow_name_desc.lower()

    page_generators = flow_obj.get('pageGenerators', [])
    if len(page_generators) < 1:
        return False
    page_processors = flow_obj.get('pageProcessors', [])
    if len(page_processors) < 1:
        return False

    for step in page_generators:
        export_obj = get_export_obj(step)

    # Try to identify the applications listed in the flow
    if flow_description=='': return False
    app_names = find_app_names(flow_description)
    if len(app_names) ==0: return False
    print("Flow: ", flow_description)
    print("Found apps: ", app_names)
    top_docs = search_func(flow_description)
    print("++++++++++++")
    filtered_top_docs = [t for t in top_docs if t[1] > 0.5]
    if len(filtered_top_docs) == 0:
        return False
    print("Flow: ", flow_name_desc)
    for doc, sim in top_docs:
        if sim > 0.5:
            print(doc, sim)
    print("====================================")
    return False
def process_flow_TBD(flow_id):
    flow_obj = get_flow_obj(flow_id)
    flow_name = flow_obj['name']
    flow_description = flow_obj.get('description', '')

    flow_name_desc = flow_name + " " + flow_description
    flow_name_desc = flow_name_desc.lower()

    # Try to identify the applications listed in the flow
    #app_names = find_app_names(flow_name_desc)
    top_matches = get_top_candidate_scores(flow_name_desc, connectors)
    if flow_description=='': return False
    print(flow_description)
    for match, score in top_matches:
        print(f"{match} : {score}")
    if len(app_names) > 0:
        print("Flow: ", flow_description)
        print("Apps: ", app_names)
        return True
    return False

def find_app_names(flow_name_desc):
    app_names = []
    for connector in connectors:
        if connector in flow_name_desc:
            app_names.append(connector)

    if len(app_names) == 0:
        partial_app_names = fetch_partial_app_names(flow_name_desc)
        app_names.extend(partial_app_names)
    return app_names

def fetch_partial_app_names(flow_name_desc):
    app_names = []
    for connector in connectors:
        # find the jaccard similarity between the connector name and the flow
        # name
        jaccard_similarity = jaccard_similarity_trigrams(connector, flow_name_desc)
        if jaccard_similarity > 0.5:
            app_names.append(connector)
    return app_names
def jaccard_similarity_trigrams(word1, word2):
    def get_trigrams(word):
        # If the word is shorter than 3 characters, return an empty set
        if len(word) < 4:
            return set(word)
        # Create a set of all 3-character grams in the word
        return {word[i:i+4] for i in range(len(word) - 3)}
    
    # Get trigrams for each word
    trigrams1 = get_trigrams(word1)
    trigrams2 = get_trigrams(word2)
    #print("Entered JC with {} and {}".format(word1, word2))
    
    # Compute the intersection and union of the trigram sets
    intersection = trigrams1.intersection(trigrams2)
    union = trigrams1.union(trigrams2)
    
    # Avoid division by zero if both sets are empty
    if not union:
        return 0.0
    
    return len(intersection) / len(trigrams1)

# Example usage
if __name__ == "__main__":
    word1 = "apple"
    word2 = "apricot"
    similarity = jaccard_similarity_trigrams(word1, word2)
    print(f"Jaccard Similarity (trigrams) between '{word1}' and '{word2}': {similarity:.4f}")

def get_export_obj(export_id):
    c = conn.cursor()
    c.execute('SELECT V from raw_exports WHERE ID = ?', (export_id,))
    row = c.fetchone()
    V = row[0]
    obj = json.loads(V)
    return obj


def get_import_obj(import_id):
    c = conn.cursor()
    c.execute('SELECT V from raw_imports WHERE ID = ?', (import_id,))
    row = c.fetchone()
    V = row[0]
    obj = json.loads(V)
    return obj


def get_flow_obj(flow_id):
    c = conn.cursor()
    c.execute('SELECT V from raw_flows WHERE ID = ?', (flow_id,))
    flow = c.fetchone()
    V = flow[0]
    flow_obj = json.loads(V)
    return flow_obj

if __name__ == '__main__':
    main()
