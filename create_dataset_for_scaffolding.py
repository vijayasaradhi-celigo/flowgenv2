import sqlite3
import json
from rich import print
from rich.progress import track
import pandas as pd

conn = sqlite3.connect('flowgen.db')

def get_export_resouce(export_id):
    cursor = conn.cursor()
    cursor.execute('SELECT name, description, is_lookup FROM http_exports WHERE _id = ?', (export_id,))
    row = cursor.fetchone()
    return row

def get_import_resouce(import_6yyid):
    cursor = conn.cursor()
    cursor.execute('SELECT name, description, is_lookup FROM http_imports WHERE _id = ?', (import_id,))
    row = cursor.fetchone()
    return row

def fetch_resource(resource_id):
    #print("Resource ID: ", resource_id)
    cursor = conn.cursor()
    cursor.execute('''SELECT name, description, is_lookup, "import" FROM
                   http_imports WHERE import_id = ?  UNION SELECT name,
                   description, is_lookup, "export" FROM http_exports WHERE
                   export_id=?''', (resource_id, resource_id))
    row = cursor.fetchone()
    record = {'name': row[0], 'description': row[1], 'is_lookup': row[2], "type": row[3]}

    return record

def fetch_resource_raw(resource_id):
    cursor = conn.cursor()
    cursor.execute('''SELECT V, "export" FROM raw_exports WHERE ID = ? UNION
                   SELECT V, "import" FROM raw_imports WHERE ID = ? ''',
                   (resource_id, resource_id))
    row = cursor.fetchone()
    V = row[0]
    json_v = json.loads(V)
    record = {'name': json_v['name'], 'description': json_v.get('description', ''),
              'is_lookup': json_v.get('isLookup', False), "type": row[1]}
    return record

def get_http_flows(table_name, http_flow_ids):
    cursor = conn.cursor()
    cursor.execute('SELECT V FROM %s' % table_name)
    chunk_size = 100000
    total = 0
    all_objs = []
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        for row in track(rows, description="Processing rows"):
            V = row[0]  # V is the first column
            json_v = json.loads(V)
            flow_id = json_v['_id']
            if flow_id not in http_flow_ids:
                continue
            description = json_v.get('description', '')
            name = json_v.get('name', '')
            if description !='' and name !='': continue
            if json_v.get('deletedAt', False): continue
            total +=1
            page_generators = json_v.get('pageGenerators', [])
            page_processors = json_v.get('pageProcessors', [])
            flow_description = json_v.get('description', '')
            bubbles = page_generators + page_processors
            # print(bubbles)
            resources = []
            # print(total, flow_id, "Flow Description: ", flow_description)
            obj = {'flow_id': flow_id, 'description': flow_description, 'name': name}
            for bubble in bubbles:
                try:
                    resource = fetch_resource_raw(bubble)
                except Exception as e:
                    print("Error fetching resource: ", e) 
                    raise e
                    continue
                resources.append(resource)
            if resources[-1]['is_lookup']: 
                print("Ignoring flow with last bubble as lookup")
                continue
            # Last bubble can never be lookup
            if resources[0]['type'] !='export':
                print("Ignoring flow with frst bubble not as export")
                continue
            obj['resources'] = resources

            # print("Resources: ", resources)
            # print("=============")
            all_objs.append(obj)
    return all_objs

flow_ids_filename = 'http_flow_ids.json'
with open(flow_ids_filename, 'r') as f:
    http_flow_ids = json.load(f)
http_flow_ids = set(http_flow_ids)
objs = get_http_flows('raw_flows', http_flow_ids)
sorted_objs = sorted(objs, key=lambda x: x['description'])
with open('sorted_flows_full_raw.json', 'w') as f:
    json.dump(sorted_objs, f, indent=4)

# Create a jsonl file from sorted_objs
print("Creating jsonl file")
fp = open('sorted_flows_full_raw.jsonl', 'w')
for obj in track(sorted_objs):
    final_obj = {}
    final_obj['flow_id'] = obj['flow_id']
    final_obj['description'] = obj['description']
    final_obj['name'] = obj['name']
    final_obj['resources'] = obj['resources']
    json.dump(final_obj, fp)
    fp.write('\n')
fp.close()
print("Finished")
