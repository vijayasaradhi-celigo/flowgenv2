import os
import json
import pandas as pd
import json
import re
#from rich import print

def extract_resources(json_input):
    """
    Extracts a list of resources from a JSON input.

    Args:
        json_input (str): Either a file path to a JSON file or a JSON string.
    
    Returns:
        list: A list of dictionaries with each resource's key properties.
    """
    try:
        # Try to open the input as a file path.
        with open(json_input, "r") as file:
            data = json.load(file)
    except (FileNotFoundError, OSError):
        # If file not found, assume it's a JSON string.
        data = json.loads(json_input)
    
    resources = []
    for resource in data:
        name = resource.get("name")
        fqn = remove_brackets(name)
        resource_name = fqn.split(':')[-1].lower()
        resource_name = resource_name.replace('.', ' ').strip()

        resource_info = {
            "id": resource.get("_id"),
            "name": name,
            'fqn': fqn,
            'resource': resource_name,
            #"resourceFields": resource.get("resourceFields")
        }
        resources.append(resource_info)
    return resources


def remove_brackets(text):
    """
    Remove all content within any brackets ((), [], {}) from the input text,
    including the brackets themselves, and collapse multiple spaces into one.
    """
    # Pattern to match content inside parentheses, square brackets, or curly braces.
    pattern = r'\([^()]*\)|\[[^\[\]]*\]|\{[^{}]*\}'
    
    # Remove nested brackets iteratively.
    previous = None
    while previous != text:
        previous = text
        text = re.sub(pattern, '', text)
    
    # Collapse multiple spaces into a single space and strip leading/trailing spaces.
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Example usage:
if __name__ == "__main__":
    sample_text = "This is a test (remove this (even nested)) and [remove this too]   but keep   this."
    cleaned_text = remove_brackets(sample_text)
    print(cleaned_text)

# This function is unused
def extract_fields(fields):
    """
    Recursively extracts field IDs and their data types from a list of resource fields.

    Args:
        fields (list): A list of resource field dictionaries.
    
    Returns:
        dict: A dictionary where keys are field IDs and values are their data types,
              or nested dictionaries for nested resource fields.
    """
    result = {}
    for field in fields:
        field_id = field.get("id")
        data_type = field.get("dataType", "N/A")
        # If nested resource fields exist, extract recursively.
        if "resourceFields" in field and field["resourceFields"]:
            result[field_id] = extract_fields(field["resourceFields"])
        else:
            result[field_id] = data_type
    return result

metadatacreation_repo = '/home/vijay/metadataCreation/metadata'
all_lines = []
# For each directory in the metadata creation repository
for connector in os.listdir(metadatacreation_repo):
    print("Processing connector: ", connector)
    filename = 'httpConnectorResources.json'
    full_path = os.path.join(metadatacreation_repo, connector, filename)
    try:
        f = open(full_path, 'r')
    except Exception:
        print("File not found: ", full_path)
        continue
    f.close()
    resources = extract_resources(full_path)
    for resource in resources:
        resource['connector'] = connector
        print(resource)
        all_lines.append(resource)


filename = 'all_resources.csv'
df = pd.DataFrame(all_lines)
df.to_csv(filename, index=False, header=True)

print("Done. Output written to: ", filename)
print(df.shape)
print(df['connector'].value_counts())
