import os
import json
import pandas as pd

metadatacreation_repo = '/home/vijay/metadataCreation/metadata'
all_lines = []
# For each directory in the metadata creation repository
for connector in os.listdir(metadatacreation_repo):
    print("Processing connector: ", connector)
    filename = 'httpConnectorEndpoints.json'
    full_path = os.path.join(metadatacreation_repo, connector, filename)
    try:
        f = open(full_path, 'r')
    except Exception:
        print("File not found: ", full_path)
        continue
    with f:
        data = json.load(f)
        for row in data:
            #print(row)
            cname = connector
            name = row['name']
            description = row.get('description', 'NA')
            method = row['method']
            relative_uri = row['relativeURI'] 
            line = (connector, cname, name, description, method, relative_uri)
            #print(line)
            all_lines.append(line)

filename = 'all_connectors.csv'
df = pd.DataFrame(all_lines, columns=['connector', 'cname', 'name', 'description', 'method', 'relative_uri'])

# sort the df by connector
df = df.sort_values(by=['connector', 'name'])
df.to_csv(filename, index=False, header=True)

print("Done. Output written to: ", filename)
print(df.shape)
print(df['connector'].value_counts())
