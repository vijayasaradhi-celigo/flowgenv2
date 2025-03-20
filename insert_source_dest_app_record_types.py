import sqlite3
import pandas as pd
import json

conn = sqlite3.connect('flowgen.db')

def main():
    filename = 'flows_with_app_names_good_exp_imp_descriptions_final.jsonl'
    rows = []
    fp = open(filename, 'r')
    for line in fp:
        obj = json.loads(line)
        source_application = obj['source_application']
        source_resource = obj['source_resource']
        destination_application = obj['destination_application']
        destination_resource = obj['destination_resource']
        if isinstance(source_application, list):
            print("Source application is list for line: ", line)
            continue
        if isinstance(source_resource, list):
            print("Source resource is list for line: ", line)
            continue
        if isinstance(destination_application, list):
            print("Destination application is list for line: ", line)
            continue
        if isinstance(destination_resource, list):
            print("Destination resource is list for line: ", line)
            continue
        row = (source_application, source_resource, destination_application, destination_resource)
        rows.append(row)
    fp.close()
    df = pd.DataFrame(rows, columns=['source_application', 'source_resource', 'destination_application', 'destination_resource'])
    df.to_sql('source_dest_app_record_types', conn, if_exists='replace', index=False)
    print("Finished inserting source_dest_app_record_types")


if __name__ == '__main__':
    main()
