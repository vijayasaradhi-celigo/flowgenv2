import os
import json

metadatacreation_repo = '/home/vijay/metadataCreation/metadata'

# For each directory in the metadata creation repository
for directory in os.listdir(metadatacreation_repo):
    filename = os.path.join(metadatacreation_repo, directory, 'httpConnectorResources.json')
    try:
        fp = open(filename, 'r')
    except Exception:
        #print('Could not open file', filename)
        continue
    data = fp.read()
    objs = json.loads(data)
    for obj in objs:
        name = obj['name']
        name = name.replace('.', ' ')
        name = name.replace('_', ' ')
        #print(name)

        print(directory, name)


