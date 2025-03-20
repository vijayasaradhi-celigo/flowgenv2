import json

filename = 'source_target__record_type_with_fid_full.jsonl'
descriptions = []
fp = open(filename, 'r')
for line in fp:
    obj = json.loads(line)
    description = obj['flow_name']
    print("Description: ", description)
    descriptions.append(description)

print("Total number of descriptions: ", len(descriptions))
print("Number of distinct descriptions: ", len(set(descriptions)))
