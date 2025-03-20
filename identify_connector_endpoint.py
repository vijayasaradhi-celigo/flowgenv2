import pandas as pd
import json
from rich import print
from rich.progress import track
from sklearn.metrics import jaccard_score
import sys

filename = 'sorted_all_existing_assistant_flows_with_keys.json'
def main():
    with open(filename) as f:
        data = json.load(f)

    print("Loaded flows {}".format(len(data)))

    connector_metadata = pd.read_csv('all_connectors.csv')


    final_objs = []
    for flow in track(data):
        obj = {}
        obj['flow_id'] = flow['flow_id']
        obj['flow_name'] = flow['flow_name']
        obj['flow_description'] = flow['flow_description']
        obj['final_flow_description'] = flow['flow_description']
        obj['num_assistants'] = flow['num_assistants']
        resources = []
        for resource in flow['assistants']:
            description = get_description(resource, connector_metadata)
            resources.append({'resource': resource, 'description': description,
                              "final_resource_description": ''})
        obj['resources'] = resources
        final_objs.append(obj)

    #print(final_objs)
    ft = open('final_objs.json', 'w')
    json.dump(final_objs, ft, indent=4)
    ft.close()

def jaccard_similarity(set1, set2):
	# intersection of two sets
	intersection = len(set1.intersection(set2))
	# Unions of two sets
	union = len(set1.union(set2))
	return intersection / union

def compute_distance(a:str, b:str):
    a = set(a)
    b = set(b)
    return jaccard_similarity(a, b)


def get_description(resource, connector_metadata):
    parts = resource.split('|')
    #print("Entered get_description with resource:|{}|".format(resource))
    connector = parts[0].strip()
    method = parts[-2].strip()
    relative_uri = parts[-1].strip()
    #print("connector:|{}|".format(connector))
    #print("method:|{}|".format(method))
    #print("relative uri:|{}|".format(relative_uri))
    connector_df = connector_metadata[connector_metadata['connector'] == connector]
    mini_df = connector_df[connector_metadata['method'] == method]
    #print(mini_df)
    if mini_df.empty:
        return "NOMATCH"
    distances = {}
    for index, row in mini_df.iterrows():
        distance = compute_distance(row['relative_uri'], relative_uri)
        distances[row['relative_uri'] + " | " + row['name']+"|"+ str(row['description'])] = distance

    sorted_distances = sorted(distances.items(), key=lambda x: x[1], reverse=True)
    top_hit = sorted_distances[0]
    return top_hit[0]



if __name__ == '__main__':
    main()
