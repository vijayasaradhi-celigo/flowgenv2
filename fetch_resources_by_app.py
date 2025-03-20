import pandas as pd
import sys

try:
    app_name = sys.argv[1]
except IndexError:
    app_name = '3plcentral'
resources_df = pd.read_csv('all_resources.csv')

# Filter resources by app name
mini_resources_df = resources_df[resources_df['connector'] == app_name]
# Keep resource and connector columns
mini_resources_df = mini_resources_df[['resource', 'connector']]
# Sort on the resource column
mini_resources_df = mini_resources_df.sort_values(by='resource')
print("Resources for app: ", app_name)
print(mini_resources_df)
print(mini_resources_df.columns)
