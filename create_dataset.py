import json

filename = 'sorted_flows_full_raw.jsonl'
X_all = []
y_all = []
fp = open(filename, 'r')
for line in fp:
    obj = json.loads(line)
    flow_description = obj.get('description', '')
    flow_name = obj.get('name', '')
    X = f"{flow_name} {flow_description}"

    resources = obj['resources']
    resource_contexts = []
    for resource in resources:
        resource_type = resource['type']
        is_lookup = resource['is_lookup']
        if is_lookup:
            resource_type = 'lookup'
        resource_name = resource['name']
        resource_description = resource.get('description', '')
        resource_context = f"{resource_type}: {resource_name} {resource_description}"
        resource_contexts.append(resource_context)
    y = '|'.join(resource_contexts)
    X_all.append(X)
    y_all.append(y)

filename = 'dataset_for_model_1.jsonl'
fp = open(filename, 'w')
max_input_words = 0
max_output_words = 0
for X, y in zip(X_all, y_all):
    obj = {
        'X': X,
        'y': y
    }
    # Check that X should have at least 5 words
    if len(X.split()) < 5:
        continue
    if "customsearch" in X:
        continue
    num_input_words = len(X.split())
    num_output_words = len(y.split())
    if num_output_words > 50: continue
    if num_input_words > max_input_words:
        max_input_words = num_input_words
    if num_output_words > max_output_words:
        max_output_words = num_output_words

    print(f"X: {X}")
    print(f"y: {y}")
    print('---')
    fp.write(json.dumps(obj))
    fp.write('\n')
print(f"Max input words: {max_input_words}")
print(f"Max output words: {max_output_words}")
fp.close()

