import sqlite3
import json
from rich.progress import track
from rich import print
import sys
import pandas as pd

conn = sqlite3.connect('flowgen.db')

def main():
    sql = "SELECT * FROM raw_flows"
    batch_size = 100000

    cursor = conn.cursor()
    cursor.execute(sql)
    batch = 0
    while True:
        batch_export_rows = []
        batch_import_rows = []
        batch += 1
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in track(rows, description='Processing batch {}'.format(batch)):
            V = row[1]
            obj_v = json.loads(V)

            # process row
            mini_export_rows = process_row_for_exports(obj_v)
            mini_import_rows = process_row_for_imports(obj_v)
            batch_export_rows.extend(mini_export_rows)
            batch_import_rows.extend(mini_import_rows)
        # Create a df from the output_rows
        df = pd.DataFrame(batch_export_rows, columns=['flow_id', 'eid'])
        df.to_sql('flow_exports', conn, if_exists='append', index=False)
        df = pd.DataFrame(batch_import_rows, columns=['flow_id', 'eid'])
        df.to_sql('flow_imports', conn, if_exists='append', index=False)

def process_row_for_exports(obj_v):
    rows = []
    flow_id = obj_v['_id']
    page_generators = obj_v['pageGenerators']
    for page_generator in page_generators:
        row = (flow_id, page_generator)
        rows.append(row)
    return rows

def process_row_for_imports(obj_v):
    rows = []
    flow_id = obj_v['_id']
    page_processors = obj_v['pageProcessors']
    rows=[]
    for page_processor in page_processors:
        row = (flow_id, page_processor)
        rows.append(row)
    return rows

if __name__ == '__main__':
    main()
