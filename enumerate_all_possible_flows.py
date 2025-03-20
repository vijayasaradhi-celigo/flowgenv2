import pandas as pd
import time

connector_df = pd.read_csv('all_connectors.csv')
connectors = connector_df['connector'].unique().tolist()
print(f"Loaded {len(connectors)} connectors")

resources_df = pd.read_csv('all_resources.csv')
resources = resources_df['resource'].tolist()
print(f"Loaded {len(resources)} resources")
count = 0
for source_app in connectors:
    for target_app in connectors:
        for source_resource in resources:
            for target_resource in resources:
                print(f"Export {source_resource} from {source_app}  and import to {target_app} as {target_resource}")
                count += 1
                if count %10000 == 0:
                    print(f"Imported {count} flows")
                    time.sleep(5)
