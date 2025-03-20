import pandas as pd
import time

connector_df = pd.read_csv('all_connectors.csv')
connectors = connector_df['connector'].unique().tolist()
print(f"Loaded {len(connectors)} connectors")

resources_df = pd.read_csv('all_resources.csv')
resources = resources_df['resource'].unique().tolist()
print(f"Loaded {len(resources)} resources")
count = 0
fp = open('all_enumerated_exports.txt', 'w')
for source_app in connectors:
    for source_resource in resources:
        fp.write(f"Export {source_resource} from {source_app}\n")
        count += 1
        if count % 100000 == 0:
            print(f"Processed {count} exports")
            time.sleep(1)
fp.close()

count = 0
fp = open('all_enumerated_imports.txt', 'w')
for target_app in connectors:
    for target_resource in resources:
        fp.write(f"Import to {target_app} as {target_resource}\n")
        count += 1
        if count % 100000 == 0:
            print(f"Processed {count} imports")
            time.sleep(1)
fp.close()
