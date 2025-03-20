import pandas as pd
from rich import print
from string_match import match_string

connectors_df = pd.read_csv('all_connectors.csv')
resources_df = pd.read_csv('all_resources.csv')
print("Shape of connectors_df: ", connectors_df.shape)
print("Shape of resources_df: ", resources_df.shape)
print(connectors_df.columns)

# Find unique applications
unique_apps = connectors_df['connector'].unique()
print("Number of unique applications: ", len(unique_apps))

def normalize_applications_and_resources(obj):
    normalized_source_application = normalize_app(obj['source_application'])
    normalized_destination_application = normalize_app(obj['destination_application'])
    #normalized_source_resource = normalize_resource(obj['source_resource'], normalized_source_application)
    #normalized_destination_resource = normalize_resource(obj['destination_resource'], normalized_destination_application)
    obj['normalized_source_application'] = normalized_source_application
    obj['normalized_destination_application'] = normalized_destination_application
    #obj['source_resource'] = normalized_source_resource
    #obj['destination_resource'] = normalized_destination_resource
    return obj


def normalize_app(approximate_app_name):
    if approximate_app_name is None:
        return "None"
    print("Entering normalize_app with approximate_app_name: ", approximate_app_name)
    # Find the closest match
    best_match, scores = match_string(approximate_app_name.lower(), unique_apps)
    print("Returning best_match: ", best_match)
    return best_match

def normalize_resource(approximate_resource_name, app_name):
    # Get the resources associated with the app name
    resources = resources_df[resources_df['connector'] == app_name]['resource'].unique()
    print("Resources for app_name: ", app_name, " are: ", resources)
    # Find the closest match
    best_match, scores = match_string(approximate_resource_name.lower(), resources)
    print("Returning best_match for resource: ", best_match)
    return best_match

