from identify_source_dest_apps import identify_source_and_destination_applications
from normalize_apps_and_resource import normalize_applications_and_resources
from rich import print
import json
import sys


filename = 'prompts_from_tony.jsonl'
fp = open(filename, 'r')
for line in fp:
    line = line.strip()
    print("Line: ")
    print(line)
    prompt_obj = json.loads(line)
    prompt = prompt_obj['prompt']
    print(prompt)
    obj = identify_source_and_destination_applications(line)
    print(obj)
    normalised_obj = normalize_applications_and_resources(obj)
    print("Normalised object: ")
    print(normalised_obj)
