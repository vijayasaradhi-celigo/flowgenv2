import pandas as pd
import sqlite3
import json
import sys
from rich import print
from rich.progress import track


conn = sqlite3.connect('flowgen.db')


def main():
    exports_file = 'inferred_exports.csv'
    imports_file = 'inferred_imports.csv'
    lookups_file = 'inferred_lookups.csv'
    exports = pd.read_csv(exports_file)
    imports = pd.read_csv(imports_file)
    lookups = pd.read_csv(lookups_file)

    export_lookup_ids = set(exports['id'].tolist() + lookups['id'].tolist())
    import_ids = set(imports['id'].tolist())
    print(f"# of export lookups: {len(export_lookup_ids)}")
    print(f"# of imports: {len(import_ids)}")

    sql = "SELECT flow_id from http_flows";
    flows = pd.read_sql_query(sql, conn)
    print("Found rows in flows: ", len(flows))
    #Iterate the rows in flows

    fp=open("flows_on_good_exp_imp_lookups.txt", "w")
    flow_ids = []

    for index, row in track(flows.iterrows()):
        flow_id = row['flow_id']
        if process_flow(flow_id, export_lookup_ids, import_ids):
            flow_ids.append(flow_id)
            fp.write(f"{flow_id}\n")

    fp.close()

    # Now process each of these flow ids
    for flow_id in flow_ids:
        get_full_flow(flow_id)

    print(f"Total flows: {len(flow_ids)}")
def get_full_flow(flow_id):
    flow_obj = get_flow_obj(flow_id)
    print(flow_obj['name'])
    for page in flow_obj['pageGenerators']:
        export_obj = get_export_obj(page)
        print(f"Export: {export_obj['name']}")
    for page in flow_obj['pageProcessors']:
        import_obj = get_import_obj(page)
        print(f"Import: {import_obj['name']}")
    print("\n\n")

def process_flow(flow_id, export_lookup_ids, import_ids):
    #print(flow_id)
    flow_obj = get_flow_obj(flow_id)
    page_generators = flow_obj['pageGenerators']
    page_processors = flow_obj['pageProcessors']
    for page in page_generators:
        if page not in export_lookup_ids:
            return False
    for page in page_processors:
        if page not in import_ids:
            return False
    return True




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

