import pandas as pd
import sqlite3
import json
import sys
from rich import print
from rich.progress import track


conn = sqlite3.connect('flowgen.db')


def main():
    impor_descriptions_file = 'http_imports_filtered_rules.json'
    export_description_file = 'http_exports_filtered_rules.json'
    export_descriptions = []
    import_descriptions = []

    fp = open(export_description_file, 'r')
    for line in fp:
        obj = json.loads(line)
        description = obj['description']
        export_descriptions.append(description)
    fp.close()
    fp = open(impor_descriptions_file, 'r')
    for line in fp:
        obj = json.loads(line)
        description = obj['description']
        import_descriptions.append(description)
    fp.close()


    sql = "SELECT flow_id from http_flows";
    #sql = "SELECT ID as flow_id from raw_flows";
    flows = pd.read_sql_query(sql, conn)
    print("Found rows in flows: ", len(flows))
    #Iterate the rows in flows

    flow_ids = []
    fp= open('flow_ids_with_good_exp_imp_descriptions.txt', 'w')

    for index, row in track(flows.iterrows(), total=len(flows)):
        flow_id = row['flow_id']
        if process_flow(flow_id, export_descriptions, import_descriptions):
            flow_ids.append(flow_id)
            fp.write(f"{flow_id}\n")

    fp.close()

    # Now process each of these flow ids
    fp = open('flow_ids_with_good_exp_imp_descriptions_final.jsonl', 'w')
    for flow_id in flow_ids:
        flow_obj = get_full_flow(flow_id)
        if flow_obj is not None:
            print(flow_obj['name'])
            for step in flow_obj['steps']:
                print(step)
            fp.write(json.dumps(flow_obj) + "\n")
            print("\n\n")
    fp.close

    print(f"Total flows: {len(flow_ids)}")

def get_full_flow(flow_id):
    flow_obj = get_flow_obj(flow_id)
    if flow_obj is None: return False
    obj = {'flow_id': flow_id, 'name': flow_obj['name'], 'description': flow_obj['description'],
           'pageGenerators': flow_obj['pageGenerators'], 'pageProcessors':
           flow_obj['pageProcessors']}
    print(flow_obj['name'])
    steps = []
    for page in flow_obj['pageGenerators']:
        export_obj = get_export_obj(page)
        if export_obj.get('isLookup', False):
            obj_detail = {'type': "Export",
                          "description":export_obj['description'],
                          'name':export_obj['name']}
        else:
            obj_detail = {'type': "Lookup",
                          "description":export_obj['description'],
                          'name':export_obj['name']}
        steps.append(obj_detail)
    for page in flow_obj['pageProcessors']:
        import_obj = get_import_obj(page)
        obj_detail = {'type': "Import",
                      "description":import_obj['description'],
                      'name':import_obj['name']}
        steps.append(obj_detail)
    obj['steps'] = steps
    return obj

def process_flow(flow_id, export_descriptions, import_descriptions):
    #print(flow_id)
    flow_obj = get_flow_obj(flow_id)
    page_generators = flow_obj['pageGenerators']
    page_processors = flow_obj['pageProcessors']
    if len(page_processors) == 0 or len(page_generators) == 0: return False
    for page in page_generators:
        obj = get_export_obj(page)
        if obj is None: return False
        if 'description' not in obj: return False
        description = obj['description']
        lookup = obj.get('isLookup', False)
        if lookup: continue
        if description not in export_descriptions: return False

    for page in page_processors:
        obj = get_import_obj(page)
        if obj is None: return False
        if 'description' not in obj: return False
        description = obj['description']
        if description not in import_descriptions: return False
    return True




def get_export_obj(export_id):
    c = conn.cursor()
    c.execute('SELECT V from raw_exports WHERE ID = ?', (export_id,))
    row = c.fetchone()
    if row is None: return None
    V = row[0]
    obj = json.loads(V)
    return obj


def get_import_obj(import_id):
    c = conn.cursor()
    c.execute('SELECT V from raw_imports WHERE ID = ?', (import_id,))
    row = c.fetchone()
    if row is None: return None
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

