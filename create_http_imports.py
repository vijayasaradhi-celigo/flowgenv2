import sqlite3
import json
from rich import print
from rich.progress import track
import pandas as pd

conn = sqlite3.connect('flowgen.db')

def get_http_imports(table_name):
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
            deleted = json_v.get('deletedAt', False)
            if deleted: continue
            export_id = json_v.get('_id', None)
            adaptorType = json_v.get('adaptorType', None)
            if adaptorType!='HTTPImport':
                continue
            name = json_v.get('name', '')
            description = json_v.get('description', '')
            is_lookup = json_v.get('isLookup', False)
            row = [export_id, name, description, is_lookup]
            all_rows.append(row)
    df = pd.DataFrame(all_rows, columns=['import_id', 'name', 'description', 'is_lookup'])
    print(df.shape)
    df.to_csv('http_imports.csv', escapechar="\\", header=True, index=False)


get_http_imports('raw_imports')
print("Finished")
