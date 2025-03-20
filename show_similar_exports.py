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


def main():
    # Example usage
    client = connect_to_typesense()
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM raw_exports where ID in (select export_id
                   from http_exports where description!='' );''')
    rows = cursor.fetchall()
    num_rows = len(rows)
    print(f"Number of rows: {num_rows}")
    output_rows = []
    ft = open('final_aggregated_exports.jsonl', 'w')
    for i, row in enumerate(track(rows)):
        #print("Processing row {} of {}".format(i, num_rows))
        V = row[1]
        json_v = json.loads(V)
        #print(json_v)
        name = json_v.get('name', 'NONAME')
        description = json_v.get('description', 'NODESCRIPTION')
        num_words_in_description = len(description.split())
        if num_words_in_description < 5: continue
        if len(description) > 4000: continue
        #print("Search query: {}".format(description))
        search_params = {
            'q': f"{description}",
            'query_by': 'description,name',
            #'query_by_weights': '2,1',
            'filter_by': '',
            'per_page': 20
        }
        response = client.collections['exports'].documents.search(search_params)
        # print number of search results
        #print(f"Found results: {response['found']}")
        oprow = {
            'description': description,
            'num_hits': response['found']
        }
        output_rows.append(oprow)
        ft.write(json.dumps(oprow) + '\n')
    ft.close()

    # Sort rows based on the description
    output_rows = sorted(output_rows, key=lambda x: x['description'])
    filtered_output_rows = set()
    for row in output_rows:
        filtered_output_rows.add((row['description'], row['num_hits']))
    # Write the filtered output rows to a file
    ft = open('filtered_final_aggregated_exports.jsonl', 'w')
    for row in filtered_output_rows:
        ft.write(json.dumps(row) + '\n')
    ft.close()

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

