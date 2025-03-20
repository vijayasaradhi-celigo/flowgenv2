import os
import json
import typesense
import sys
from tqdm import tqdm

COLLECTION_NAME = sys.argv[1]
JSONL_FILE = sys.argv[2]


def main():
    print("Ingesting data to typesense")
    client = connect_to_typesense()
    drop_collection(client, COLLECTION_NAME)
    create_collection(client)
    ingest_jsonl_tqdm(client, JSONL_FILE)

# Drop pre-existing collection if any
def drop_collection(client, collection):
    try:
        client.collections[collection].delete()
    except Exception as e:
        print("Collection {} does not exist".format(collection))
        pass


def ingest_jsonl_tqdm(client, filename):
    batch_size = 1024
    records = []
    print("Starting ingestion of {}".format(filename))
    with open(filename) as jsonl_file:
        for line in jsonl_file:
            obj = json.loads(line)
            records.append(obj)
    jsonl_file.close()
    for i in tqdm(range(0, len(records), batch_size)):
        batch = records[i:i+batch_size]
        client.collections[COLLECTION_NAME].documents.import_(batch, {'action': 'create'})

def ingest_jsonl(client, filename):
    print("Starting ingestion of {}".format(filename))
    full_path = os.path.join(DATA_DIR, filename)
    with open(full_path) as jsonl_file:
        client.collections[COLLECTION_NAME].documents.import_(jsonl_file.read().encode('utf-8'),
                {'action': 'create'})


def connect_to_typesense():
    client = typesense.Client({
    'api_key': 'xyz',
    'nodes': [{
        'host': 'localhost',
        'port': '8108',
        'protocol': 'http'
    }],
    'connection_timeout_seconds': 600
})
    return client

# Drop pre-existing collection if any
def drop_collection(client, collection):
    try:
        client.collections[collection].delete()
    except Exception as e:
        print("Collection {} does not exist".format(collection))
        pass

def create_collection(client):
    create_response = client.collections.create({
    "name": COLLECTION_NAME,
    "fields": [
        {"name": "flow_id", "type": "string", "index": False, "optional": True},
        {"name": "flow_name", "type": "string"},
        {"name": "flow_description", "type": "string"},
    ],
})
    #print(create_response)

if __name__ == "__main__":
    main()

