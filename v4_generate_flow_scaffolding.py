import sqlite3
import pandas as pd
import json
from show_all_keys import fields_to_ignore
from flatten_json import flatten, unflatten
from most_similar_jsons import find_most_similar_json
from transformers import T5Tokenizer
from rich.progress import track
from rich import print
import random
import sys


conn = sqlite3.connect('flowgen.db')
def strip_keys(obj):
    #return obj
    stripped_document = {k: v for k, v in obj.items() if k not in fields_to_ignore}
    return stripped_document

def process_obj(obj):
    stripped_obj = strip_keys(obj)
    flattened_obj = flatten(stripped_obj, '|')
    replaced_obj = unflatten(replace_keys_with_id(flattened_obj), '|')

    return replaced_obj

def replace_keys_with_id(obj):
    new_obj = {}
    for k,v in obj.items():

        if k.endswith('Id'):
            v='TBD_{}'.format(k.split('_')[-1])
            v='TBD'
        new_obj[k] = v 
    return new_obj



def main():
    samples=[]
    export_description_filename = 'unique_flow_descriptions_for_scaffolding.csv'
    export_description_df = pd.read_csv(export_description_filename)
    print(export_description_df.head())

    #Iterate the dataframe with DESCRIPTION column
    for index, row in track(export_description_df.iterrows(), total=len(export_description_df)):
        description = row['DESCRIPTION']
        sample = create_sample(description)
        if not sample: continue
        samples.append(sample)
    print(f"Generated {len(samples)} samples")
    with open('ds_scaffolding_samples.json', 'w') as f:
        json.dump(samples, f, indent=4)

    # Save in jsonl format for processing by machine
    fp=open('ds_scaffolding_samples.jsonl','w')
    for sample in samples:
        json.dump(sample, fp)
        fp.write('\n')
    fp.close()


def create_sample(flow_description):
    print("Processing description: ", flow_description)
    flow_exports = "flow_exports"
    flow_imports = "flow_imports"

    cursor = conn.cursor()
    cursor.execute("SELECT ID FROM all_flow_names_descriptions WHERE description = ?", (flow_description,))
    data = cursor.fetchall()
    ids = [row[0] for row in data]

    # Choose a random id from ids
    flow_id = random.sample(ids, 1)[0]
    #flow_id = ids[0]

    query = f"SELECT eid FROM {flow_exports} WHERE flow_id ='{flow_id}'"
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    eids = [row[0] for row in data]
    print(f"Fetch {len(eids)} export ids for flow_id {flow_id}")
    print("-----")
    all_exports = []
    for eid in eids:
        export_obj = get_export_obj(eid)
        if export_obj is None: return None
        name = export_obj.get('name', '')
        if name =='':return None
        description = export_obj.get('description', '')
        if description == '':return None
        step_type = "export"
        if export_obj.get('isLookup', False):
            step_type = "lookup"
        export_tuple = {
            "step_type": step_type,
            "description": description,
        }
        all_exports.append(export_tuple)
    print("ALL EXPORTS")
    print(all_exports)
    # Fetch the import ids
    query = f"SELECT eid FROM {flow_imports} WHERE flow_id ='{flow_id}'"
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    iids = [row[0] for row in data]
    print(f"Fetch {len(iids)} import ids for flow_id {flow_id}")
    print("-----")
    all_imports = []
    for iid in iids:
        import_obj = get_import_obj(iid)
        if import_obj is None: return None
        name = import_obj.get('name', '')
        if name =='':return None
        description = import_obj.get('description')
        if description == '':return None
        step_type = "import"
        import_tuple = {
            "step_type": step_type,
            "description": description,
        }
        all_imports.append(import_tuple)
    print("ALL IMPORTS")
    print(all_imports)
    if len(all_exports) == 0 or len(all_imports) == 0:
        return None
    flow_description = flow_description.split('.')[0]
    flow_description = flow_description.replace("This integration", '')
    flow_description = flow_description.replace("This integration flow", '')
    flow_description = flow_description.replace("This flow", '')
    flow_description = flow_description.strip()
    obj = {
        "flow_id": flow_id,
        "flow_description": flow_description,
        "exports": all_exports,
        "imports": all_imports
    }
    return obj

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


if __name__ == '__main__':
    main()
