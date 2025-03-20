import sqlite3
import json
from rich import print
#from tqdm import tqdm
from rich.progress import track

conn = sqlite3.connect('flowgen.db')

def get_http_resources(table_name):
    all_ids = []
    lookup_ids = []
    cursor = conn.cursor()
    cursor.execute('SELECT V FROM %s' % table_name)
    chunk_size = 100000
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        for row in track(rows, description="Processing rows"):
            V = row[0] # V is the first column
            json_v = json.loads(V)
            #print(json_v)
            try:
                adaptorType = json_v['adaptorType']
            except KeyError:
                #print("AdaptorType not found")
                continue
            if adaptorType in ['HTTPExport', 'HTTPImport', 'RESTExport', 'RESTImport']:
                resource_id = json_v['_id']
                all_ids.append(resource_id)
            lookup = json_v.get('isLookup', False)
            if lookup:
                lookup_ids.append(resource_id)

    return all_ids, lookup_ids


all_export_ids, all_lookup_ids = get_http_resources('raw_exports')
print("Found {} HTTPExport resources".format(len(all_export_ids)))
with open('http_export_ids.json', 'w') as f:
    json.dump(all_export_ids, f)
print("Found {} Lookup resources".format(len(all_lookup_ids)))
with open('http_lookup_ids.json', 'w') as f:
    json.dump(all_lookup_ids, f)

all_import_ids, _ = get_http_resources('raw_imports')
print("Found {} HTTPImport resources".format(len(all_import_ids)))
with open('http_import_ids.json', 'w') as f:
    json.dump(all_import_ids, f)

print("Finished")
