import sqlite3
import json
from rich.progress import track
from rich import print
import re

db = 'flowgen.db'
conn = sqlite3.connect(db)
def main():
    sql = "SELECT ID, V from raw_flows"
    cur = conn.cursor()
    batch_size = 100000
    cur.execute(sql)
    cx = 0
    total = 0
    flow_names = []
    adaptor_types_dict = {}
    samples = []
    fp = open('all_flows_for_scaffolding.jsonl', 'w')
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for row in track(rows, total=len(rows), description="Processing rows..."):
            total += 1
            flow_id, V = row
            v_obj = json.loads(V)
            result = is_valid_flow(v_obj)
            if result is None:
                #print("Fail")
                continue
            fp.write(json.dumps(result) + '\n')
            cx += 1
            source_adaptor_type = result['steps'][0]['adaptorType']
            dest_adaptor_type = result['steps'][1]['adaptorType']
            adaptor_tuple = (source_adaptor_type, dest_adaptor_type)
            adaptor_types_dict[adaptor_tuple] = adaptor_types_dict.get(adaptor_tuple, 0) + 1
            flow_name = result['flow_name']
            flow_names.append(flow_name)
            samples.append(result)
    fp.close()
    print(f"Processed {cx} valid flows out of {total} rows.")
    print(f"#unique flow names: {len(set(flow_names))}")
    # Sort the adaptor types dictionary by the number of occurrences
    sorted_adaptor_types = sorted(adaptor_types_dict.items(), key=lambda x: x[1], reverse=True)
    # Print the sorted adaptor types and their counts
    print(f"Adaptor types and their counts:")
    for adaptor_type, count in sorted_adaptor_types:
        print(f"{adaptor_type[0]} -> {adaptor_type[1]}: {count}")
    # Convert the counts to percentages
    total_count = sum(adaptor_types_dict.values())
    percentages = [(adaptor_type, count / total_count * 100) for adaptor_type, count in sorted_adaptor_types]

    # Print the number of keys in the dictionary
    print(f"Number of unique keys in the dictionary: {len(adaptor_types_dict)}")
    total_unique = 0
    for source_dest_tuple in adaptor_types_dict.keys():
        print("Processing the tuple: ", source_dest_tuple)
        # Filter the number of flows with the same source and destination

        # Filter the number of samples that match the given pair
        filtered_samples = [sample for sample in samples if sample['steps'][0]['adaptorType'] == source_dest_tuple[0] and sample['steps'][1]['adaptorType'] == source_dest_tuple[1]]
        # Print the number of samples that match the given pair
        print(f"Number of samples that match the pair {source_dest_tuple}: {len(filtered_samples)}")
        # Find the unique flow names in the filtered samples
        unique_flow_names = set(sample['flow_name'] for sample in filtered_samples)
        total_unique += len(unique_flow_names)
        # Print the number of unique flow names
        print(f"Number of unique flow names that match the pair {source_dest_tuple}: {len(unique_flow_names)}")
        # Print the unique flow names
    print("Total unique flow names: ", total_unique)
    return

    # Print the results
    print(f"Adaptor types and their percentages:")
    for adaptor_type, percentage in percentages:
        print(f"{adaptor_type}: {percentage:.2f}%")
def remove_bracket_content(s):
    # Regex pattern to match content between brackets including the
        # brackets themselves
    pattern = r'[\[\(\{].*?[\]\)\}]'
    return re.sub(pattern, '', s)
def is_valid_flow(V):
    steps = []
    flow = {}

    # Validate the name first
    flow_name = V.get('name', 'unknown')
    flow_name = remove_bracket_content(flow_name)
    # Make sure that there are at least 5 words in the flow name
    if len(flow_name.split()) < 6:
        return None
    # Make sure that there are at least 20 characters in the flow name
    if len(flow_name) < 20:
        return None

    #2 pageGenerators
    #1 pageProcessors
    pageGenerators = V['pageGenerators']
    if len(pageGenerators) != 1:
        return None
    pageProcessors = V['pageProcessors']
    if len(pageProcessors) != 1:
        return None

    for i, pageGenerator in enumerate(pageGenerators):
        try:
            resource = fetch_resource(pageGenerator)
        except Exception as e:
            return None
        step_name = resource.get('name', 'unknown')
        step_name = remove_bracket_content(step_name)
        # Make sure there at least 5 words in the step name
        if len(step_name.split()) < 5:
            return None
        # Make sure there at least 20 characters in the step name
        if len(step_name) < 20:
            return None
        adaptorType = resource.get('adaptorType', 'NA')
        if adaptorType=='NA':
            return None
        if adaptorType=='WebhookExport':
            return None
        #print(resource)
        if resource.get('isLookup', False):
            step_type = 'lookup'
        else:
            step_type = 'export'
        #print(f"PG:Step {i}: {step_name}, Type: {step_type}, AdaptorType: {adaptorType}")
        #if i == 0 and step_type !='export':
        #    return None
        #if i == 1 and step_type !='lookup':
        #    return None
        steps.append({ 'type': step_type, 'adaptorType': adaptorType, 'name': step_name })
    for i, pageProcessor in enumerate(pageProcessors):
        try:
            resource = fetch_resource(pageProcessor)
        except Exception as e:
            return None
        step_name = resource.get('name', 'unknown')
        step_name = remove_bracket_content(step_name)
        # Make sure there at least 5 words in the step name
        if len(step_name.split()) < 5:
            return None
        # Make sure there at least 20 characters in the step name
        if len(step_name) < 20:
            return None
        adaptorType = resource.get('adaptorType', 'NA')
        if adaptorType=='NA':
            return None

        step_type='import'
        #print(f"PP:Step {i}: {step_name}, Type: {step_type}, AdaptorType: {adaptorType}")


        steps.append({ 'type': step_type, 'adaptorType': adaptorType, 'name': step_name })
    flow['steps'] = steps
    flow['flow_name'] = flow_name
    #flow['num_steps'] = len(steps)
    #flow['num_exports'] = len(pageGenerators)


    return flow

def fetch_resource(obj):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM raw_imports WHERE ID = ? UNION SELECT * FROM raw_exports WHERE ID=?', (obj, obj))
    row = cursor.fetchone()
    return json.loads(row[1])

if __name__ == "__main__":
    main()
