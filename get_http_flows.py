import sqlite3
import json
from rich import print
from rich.progress import track
import pandas as pd

conn = sqlite3.connect('flowgen.db')

def get_http_flows(table_name, export_import_ids, lookup_ids):
    all_ids = []
    all_rows = []
    cursor = conn.cursor()
    cursor.execute('SELECT V FROM %s' % table_name)
    chunk_size = 100000
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        for row in track(rows, description="Processing rows"):
            V = row[0]  # V is the first column
            json_v = json.loads(V)
            flow_id = json_v.get('_id', None)
            flow_name = json_v.get('name', '')
            page_generators = json_v.get('pageGenerators', [])
            page_processors = json_v.get('pageProcessors', [])
            total_generators = page_generators + page_processors
            http = True
            num_lookups = 0
            for generator in total_generators:
                if generator not in export_import_ids:
                    http = False
                    break
                if generator in lookup_ids:
                    num_lookups += 1

            if http:
                description = json_v.get('description', '')
                num_exports = len(page_generators)
                num_imports = len(page_processors)
                num_bubbles = num_exports + num_imports
                if num_bubbles < 2:
                    continue
                all_ids.append(flow_id)
                row = [flow_id, flow_name, num_exports, num_imports, num_bubbles, num_lookups,
                       description]
                all_rows.append(row)
    df = pd.DataFrame(all_rows, columns=['flow_id', 'flow_name', 'num_exports',
                                         'num_imports', 'num_bubbles',
                                         'num_lookups', 'description'])
    df.to_csv('http_flows.csv', header=True, index=False)
    return all_ids

export_filename = 'http_export_ids.json'
import_filename = 'http_import_ids.json'
lookup_filename = 'http_lookup_ids.json'

with open(export_filename, 'r') as f:
    export_ids = json.load(f)
print("Found {} export ids".format(len(export_ids)))
with open(import_filename, 'r') as f:
    import_ids = json.load(f)
print("Found {} import ids".format(len(import_ids)))
with open(lookup_filename, 'r') as f:
    lookup_ids = json.load(f)
    lookup_ids = set(lookup_ids)
print("Found {} lookup ids".format(len(lookup_ids)))
all_export_import_ids = set(export_ids + import_ids)
print("Total export and import ids: {}".format(len(all_export_import_ids)))


all_flow_ids = get_http_flows('raw_flows', all_export_import_ids, lookup_ids)
print("Found {} flows".format(len(all_flow_ids)))
with open('http_flow_ids.json', 'w') as f:
    json.dump(all_flow_ids, f)

print("Finished")
