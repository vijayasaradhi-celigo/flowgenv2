import sqlite3
import json
from rich import print
from rich.progress import track
import pandas as pd

conn = sqlite3.connect('flowgen.db')

def get_connector_names_in_flow(flow_name, connector_names):
    num_connectors = 0
    connectors_str = ''
    connectors = []
    for connector_name in connector_names:
        if connector_name in flow_name:
            num_connectors += 1
            connectors.append(connector_name)
    connectors_str = ', '.join(connectors)
#    print("Flow name: [bold red]{}[/bold red]".format(flow_name))
#    print("Connectors: [bold green]{}[/bold green]".format(connectors_str))
#    print("Number of connectors: [bold blue]{}[/bold blue]".format(num_connectors))
    return num_connectors, connectors_str


def get_connector_names_in_flows(table_name, connector_names):
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
            flow_obj = json.loads(V)
            flow_id = flow_obj.get('_id')
            flow_name = flow_obj.get('name', '')

            num_connectors, connectors_str = get_connector_names_in_flow(flow_name.lower(), connector_names)
            if num_connectors < 2: continue
            row = [flow_id, flow_name, num_connectors, connectors_str]
            all_rows.append(row)

    df = pd.DataFrame(all_rows, columns=['flow_id', 'flow_name', 'num_connectors', 'connectors_str'])
    df.to_csv('flows_with_connectors.csv', header=True, index=False)

filename = 'connectors.txt'
connector_names = []
with open(filename, 'r') as f:
    for line in f:
        connector_names.append(line.strip())
print("Found {} connectors".format(len(connector_names)))


get_connector_names_in_flows('raw_flows', connector_names)

print("Finished")
