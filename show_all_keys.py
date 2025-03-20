import sqlite3
import json
from rich import print
from rich.progress import track

conn = sqlite3.connect('flowgen.db')
fields_to_ignore = ["__encryptedFieldsLastModified", "__v", "_connectionId",
                    "_sourceId", "_userId", "apiIdentifier", "createdAt",
                    "lastModified", "sandbox", "_templateId","mockOutput",
                    "_id", "aiDescription", "description",
                    "hooks", "microServices",
                    "settingsForm", "settings", "mockInput", "mockOutput",
                    "mockResponse",
                    "name", "_connectorId", "_integrationId", "c_t",
                    'externalId']
def main():
    all_keys = []
    key_counts = {}
    cursor = conn.cursor()
    cursor.execute('SELECT flow_id FROM http_flows')
    flow_rows = cursor.fetchall()
    for flow_row in track(flow_rows):
        flow_id = flow_row[0]
        cursor.execute('SELECT * FROM raw_flows WHERE ID = ?', (flow_id,))
        row = cursor.fetchone()
        V = json.loads(row[1])
        page_generators = V['pageGenerators']
        page_processors = V['pageProcessors']
        for obj in page_generators+page_processors:
            resource = fetch_resource(obj)
            resource = strip_unnecessary_keys(resource)
            all_keys.extend(resource.keys())
            for key in resource.keys():
                key_counts[key] = key_counts.get(key, 0) + 1



    all_keys = list(set(all_keys))
    print(all_keys)
    sorted_keys = sorted(key_counts.items(), key=lambda x: x[1], reverse=True)
    for key, count in sorted_keys:
        print(f'{key}: {count/2570}')

def strip_unnecessary_keys(resource):
    for key in fields_to_ignore:
        if key in resource:
            del resource[key]
    return resource

def fetch_resource(obj):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM raw_imports WHERE ID = ? UNION SELECT * FROM raw_exports WHERE ID=?', (obj, obj))
    row = cursor.fetchone()
    return json.loads(row[1])

if __name__ == "__main__":
    main()
