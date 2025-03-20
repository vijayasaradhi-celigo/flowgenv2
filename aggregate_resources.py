from typing import List, Dict
from collections import Counter
import sqlite3
from rich import print
import json
import typesense
from show_all_keys import fields_to_ignore
import time
from rich.progress import track

conn = sqlite3.connect('flowgen.db')
def aggregate_jsons(json_list, min_freq=2):
    # Count occurrences of each key
    key_count = {}
    for obj in json_list:
        for key in obj.keys():  # keys are strings by assumption
            key_count[key] = key_count.get(key, 0) + 1

    aggregated = {}
    # Aggregate values for frequent keys
    for key, count in key_count.items():
        if count >= min_freq:
            values = [obj[key] for obj in json_list if key in obj]
            aggregated[key] = aggregate_values(values)
    return aggregated

def aggregate_values(values):
    # Check the type of values (assumes homogeneous types per key)
    if all(isinstance(v, (int, float)) for v in values):
        # Example: Sum numeric values (or calculate average)
        return sum(values)  # Alternatively: sum(values) / len(values)
    elif all(isinstance(v, str) for v in values):
        # Use the mode for strings (most common string)
        return Counter(values).most_common(1)[0][0]
    elif all(isinstance(v, bool) for v in values):
        # Majority vote for booleans
        return True if values.count(True) > values.count(False) else False
    elif all(isinstance(v, list) for v in values):
        # Merge lists and remove duplicates
        merged = []
        for lst in values:
            merged.extend(lst)
        return deduplicate_list(merged)
    elif all(isinstance(v, dict) for v in values):
        # Recursively aggregate nested JSON objects
        return aggregate_jsons(values)
    else:
        # For mixed types, return the most frequent value or a unique list
        return Counter(values).most_common(1)[0][0]
def deduplicate_list(lst):
    deduped = []
    for item in lst:
        if item not in deduped:
            deduped.append(item)
    return deduped

def main_old():
    # Example usage
    json_list = [
        {"name": "Alice", "age": 25, "city": "New York"},
        {"name": "Bob", "age": 30, "city": "San Francisco"},
        {"name": "Alice", "age": 27, "city": "New York"},
        {"name": "Charlie", "age": 25, "city": "Los Angeles"},
        {"name": "Alice", "age": 25, "city": "New York"},
    ]
    aggregated = aggregate_jsons(json_list)
    print(aggregated)

def main():
    # Example usage
    client = connect_to_typesense()
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM raw_exports where ID in (select export_id
                   from http_exports where description!='' );''')
    rows = cursor.fetchall()
    num_rows = len(rows)
    print(f"Number of rows: {num_rows}")
    ft = open('final_aggregated_exports.jsonl', 'w')
    for i, row in track(enumerate(rows)):
        if i < 119370: continue
        print("Processing row {} of {}".format(i, num_rows))
        V = row[1]
        json_v = json.loads(V)
        #print(json_v)
        name = json_v.get('name', 'NONAME')
        description = json_v.get('description', 'NODESCRIPTION')
        num_words_in_description = len(description.split())
        if num_words_in_description < 5: continue
        if len(description) > 4000: continue
        print("Search query: {}".format(description))
        search_params = {
            'q': f"{description}",
            'query_by': 'description,name',
            #'query_by_weights': '2,1',
            'filter_by': '',
            'per_page': 20
        }
        response = client.collections['exports'].documents.search(search_params)
        # print number of search results
        print(f"Found results: {response['found']}")
        num_results = response['found']
        if num_results < 20: continue
        results = response['hits']
        small_documents=[]
        for result in results:
            document = result['document']

            export_id = document['id']
            export_document = fetch_export(export_id)

            #print("Document before stripping")
            #print(export_document)
            try:
                print(export_document['name'],
                      export_document.get('description', 'NODESCRIPTION'),
                      export_document['http']['relativeURI'],
                      export_document['http'].get('method', 'NOMETHOD'))
            except Exception as e:
                continue
            small_document = {k: v for k, v in export_document.items() if k not in fields_to_ignore}
            #print("After removing unnecessary fields")
            #print(stripped_document)
            print(f"Name: {document['name']} Description: {document['description']}")
            small_documents.append(small_document)

        aggregated_document = aggregate_jsons(small_documents)
        #print(aggregated_document)
        ft.write(json.dumps(aggregated_document)+'\n')

        #time.sleep(5)


    #aggregated = aggregate_jsons(json_list)
    #print(aggregated)

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

def fetch_export(export_id):
    sql = f"SELECT * FROM raw_exports where ID='{export_id}'"
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        V = row[1]
        json_v = json.loads(V)
        return json_v

if __name__ == "__main__":
    main()

