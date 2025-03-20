import sqlite3
import json
from rich import print
from rich.progress import track
import pandas as pd

conn = sqlite3.connect('flowgen.db')

def has_assistants_in_flow_steps(flow_obj):
    # Fetch the page generators and page processors

    pageGenerators = flow_obj.get('pageGenerators', [])
    pageProcessors = flow_obj.get('pageProcessors', [])
    all_steps = pageGenerators + pageProcessors
    if len(all_steps) < 2:
        return (False, None)
    assistants = []
    names = []
    num_exports = 0
    num_imports = 0
    num_lookups = 0
    for step in all_steps:
        # Fetch the resource
        resource = get_resource(step)
        if resource is None:
            continue
        assistant = resource.get('assistant')
        name = resource.get('name')
        # Use description as it is more informative for exports and imports
        description = resource.get('description', 'description')
        if assistant is None: return False, None
        is_lookup = resource.get('isLookup', False)
        if step in pageGenerators:
            resource_type = 'export'
            num_exports += 1
        if step in pageProcessors:
            resource_type = 'import'
            num_imports += 1
        if is_lookup:
            resource_type= 'lookup'
            num_lookups += 1

        http_method = resource.get('http', {}).get('method', '')
        relative_uri = resource.get('http', {}).get('relativeURI', '')

        if isinstance(http_method, list) and len(http_method)==0:
            return (False, None)

        if isinstance(http_method, list) and len(http_method) > 0:
            http_method = http_method[0]

        if isinstance(relative_uri, list) and len(relative_uri)==0:
            return (False, None)

        if isinstance(relative_uri, list) and len(relative_uri) > 0:
            relative_uri = relative_uri[0]

        print("assistant={} resource_type={} name={} description={} http_method={} relative_uri={}".format(assistant, resource_type, name, description, http_method, relative_uri))



        assistants.append(assistant+" | "+resource_type + " | " + name + " | "
                          + description + " | " + http_method + " | " + relative_uri)
        names.append(name)
    #print("Flow_id={} num_exports={} num_imports={}".format(flow_obj['_id'], num_exports, num_imports))
    if num_exports < 1:
        return (False, None)
    if num_imports < 1:
        return (False, None)

    return (True, assistants) if len(assistants) > 1 else (False, None)


def get_resource(eid):
    cursor = conn.cursor()
    cursor.execute('SELECT V FROM raw_exports WHERE ID = ? UNION SELECT V from raw_imports where ID= ?', (eid, eid))
    row = cursor.fetchone()
    if row is None:
        return None
    V = row[0]
    return json.loads(V)


def get_assistants_flows(table_name):
    cx = 0
    all_rows = []
    cursor = conn.cursor()
    cursor.execute('SELECT V FROM %s' % table_name)
    chunk_size = 100000
    batch = 0
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        batch += 1
        for row in track(rows, description="Processing rows batch %d" % batch):
            V = row[0]  # V is the first column
            flow_obj = json.loads(V)
            flow_id = flow_obj.get('_id')
            flow_name = flow_obj.get('name', '')
            flow_description = flow_obj.get('description', '')
            (has_assistants, assistants) = has_assistants_in_flow_steps(flow_obj)
            if has_assistants:
                cx += 1
                #print(flow_id, flow_name, assistants)
                num_assistants = len(assistants)
                record = [flow_id, flow_name, flow_description, num_assistants, assistants]
                all_rows.append(record)
        # break # break after 1st batch for debug
    print("Found total flows with assistants={}".format(cx))
    # Sort the rows by the number of assistants
    all_rows = sorted(all_rows, key=lambda x: x[2])
    # Write all_rows to json
    json_filename = 'sorted_all_existing_assistant_flows.json'
    with open(json_filename, 'w') as f:
        json.dump(all_rows, f, indent=2)
    # Write all_rows to csv

    filename = "sorted_all_existing_assistant_flows.csv"
    df = pd.DataFrame(all_rows, columns=['flow_id', 'flow_name',
    'flow_description', 'num_assistants',  'assistants'])
    df.to_csv(filename, index=False)

    # Create json objects from df
    json_objects = []
    for row in all_rows:
        flow_id, flow_name, flow_description, num_assistants, assistants = row
        json_obj = {
            'flow_id': flow_id,
            'flow_name': flow_name,
            'flow_description': flow_description,
            'num_assistants': num_assistants,
            'assistants': assistants
        }
        json_objects.append(json_obj)
    # Write json objects to json file
    json_filename = 'sorted_all_existing_assistant_flows_with_keys.json'
    with open(json_filename, 'w') as f:
        json.dump(json_objects, f, indent=2)


get_assistants_flows('raw_flows')
print("Finished")
