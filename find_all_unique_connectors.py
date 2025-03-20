import os
import json

metadatacreation_repo = '/home/vijay/metadataCreation/metadata'

# For each directory in the metadata creation repository
for directory in os.listdir(metadatacreation_repo):
    print(directory)
