from identify_source_dest_apps import identify_source_and_destination_applications
from normalize_apps_and_resource import normalize_applications_and_resources
from rich import print
from rich.progress import track
import json
import sys


input_filename = 'flows_with_app_names_good_exp_imp_descriptions_final.jsonl'
output_filename = 'flows_with_app_names_good_exp_imp_descriptions_final_normalized.jsonl'
fp = open(input_filename, 'r')
ft = open(output_filename, 'w')
for line in track(fp, total=5125):
    line = line.strip()
    obj = json.loads(line)
    #obj = identify_source_and_destination_applications(line)
    #print(obj)
    normalised_obj = normalize_applications_and_resources(obj)
    print("Normalised object: ")
    print(normalised_obj)
    ft.write(json.dumps(normalised_obj))
    ft.write('\n')
fp.close()
ft.close()
print("Done")
