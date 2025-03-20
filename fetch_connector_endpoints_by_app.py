import pandas as pd
# set pandas to print all rows
pd.set_option('display.max_rows', None)
import sys

try:
    app_name = sys.argv[1]
except IndexError:
    app_name = '3plcentral'
resources_df = pd.read_csv('all_connectors.csv')

# Filter resources by app name
mini_resources_df = resources_df[resources_df['connector'] == app_name]
# Keep resource and connector columns
mini_resources_df = mini_resources_df[['description', 'relative_uri']]
# Sort on the resource column
#mini_resources_df = mini_resources_df.sort_values(by='method')
print("Resources for app: ", app_name)
print(mini_resources_df)
print(mini_resources_df.columns)
