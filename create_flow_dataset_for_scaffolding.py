import sqlite3
import json
from rich.progress import track

# Connect to the database
conn = sqlite3.connect('flowgen.db')

def main():
    fp = open('final_flow_steps_data.jsonl', 'w')
    # Create a cursor object
    c = conn.cursor()
    c.execute('SELECT flow_id from http_flows')
    rows = c.fetchall()
    for row in track(rows):
        try:
            final_flow_obj = process_flow(row)
        except Exception as e:
            continue
        num_steps = len(final_flow_obj['steps'])
        if num_steps < 2: continue
        for step in final_flow_obj['steps']:
            fp.write(json.dumps(step) + '\n')
        fp.write(json.dumps({})+'\n')
    fp.close()


def process_flow(flow):
    steps = []
    flow_id = flow[0]
    flow_obj = get_flow_obj(flow_id)
    flow_name = flow_obj['name']

    flow_description = flow_obj.get('description')
    page_generators = flow_obj['pageGenerators']
    page_processors = flow_obj['pageProcessors']
    for page_generator in page_generators:
        obj = get_export_obj(page_generator)
        mini_obj = {'flow_id': flow_id, 'flow_name': flow_name,
                    'flow_description': flow_description, 'name': obj['name'], 'type': 'export', 'id': page_generator, 'is_lookup': obj.get('isLookup', False)}
        steps.append(mini_obj)
    for page_processor in page_processors:
        obj = get_import_obj(page_processor)
        mini_obj = {'flow_id': flow_id, 'flow_name': flow_name,
                    'flow_description': flow_description, 'name': obj['name'], 'type': 'import', 'id': page_processor, 'is_lookup': False}
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
