import sqlite3
import json
from rich.progress import track

# Connect to the database
conn = sqlite3.connect('flowgen.db')


def main():
    # Create a cursor
    c = conn.cursor()
    sql = "select flow_id from http_flows where num_imports > 1 and num_bubbles IN(2,3);"
    c.execute(sql)
    rows = c.fetchall()
    for row in track(rows,total=len(rows)):
        #print("Processing flow: ", row[0])
        try:
            flow_obj =  is_good_flow(row[0])
            if flow_obj:
                print(json.dumps(flow_obj))
        except Exception as e:
            continue


def is_good_flow(flow_id):
    final_flow_obj = process_flow(flow_id)
    steps = final_flow_obj['steps']
    for step in steps:
        if step['assistant'] == 'NA':
            return False
    return final_flow_obj

def process_flow(flow_id):
    steps = []
    flow_obj = get_flow_obj(flow_id)
    flow_name = flow_obj['name']

    flow_description = flow_obj.get('description')
    page_generators = flow_obj['pageGenerators']
    page_processors = flow_obj['pageProcessors']
    for page_generator in page_generators:
        obj = get_export_obj(page_generator)
        mini_obj = {
                'name': obj['name'],
                'type': 'export',
                'id': page_generator,
                'is_lookup': obj.get('isLookup', False),
                'assistant': obj.get('assistant', 'NA'),
                'http_relativeURI': obj.get('http', {}).get('relativeURI', 'NA'),
                'http_response_resourcePath': obj.get('http', {}).get('response', {}).get('resourcePath', 'NA')
                }
        steps.append(mini_obj)
    for page_processor in page_processors:
        obj = get_import_obj(page_processor)
        mini_obj = {
                'name': obj['name'], 
                'type': 'import',
                'id': page_processor,
                'is_lookup': False,
                'assistant': obj.get('assistant', 'NA'),
                'http_relativeURI': obj.get('http', {}).get('relativeuri', 'NA'),
                'http_response_resourcePath': obj.get('http', {}).get('response', {}).get('resourcePath', 'NA')
                }
        steps.append(mini_obj)

    final_obj = {'flow_id': flow_id, 'flow_name': flow_name,
                 'flow_description': flow_description, 'steps': steps}
    return final_obj


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
