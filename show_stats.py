import json
filename = 'flows_with_app_names_good_exp_imp_descriptions_final_normalized.jsonl'
source_dict = {}
destination_dict = {}
total_dict = {}
pairs_dict = {}
fp = open(filename, 'r')
for line in fp:
    obj = json.loads(line)
    source = obj['normalized_source_application']
    destination = obj['normalized_destination_application']
    source_dict[source] = source_dict.get(source, 0) + 1
    destination_dict[destination] = destination_dict.get(destination, 0) + 1
    total_dict[source] = total_dict.get(source, 0) + 1
    total_dict[destination] = total_dict.get(destination, 0) + 1
    pair = (source, destination)
    pairs_dict[pair] = pairs_dict.get(pair, 0) + 1

fp.close()
print('Source Applications:')
sorted_sources = sorted(source_dict.items(), key=lambda x: x[1], reverse=True)
for source, count in sorted_sources:
    print(source, count)
print()
print('Destination Applications:')
sorted_destinations = sorted(destination_dict.items(), key=lambda x: x[1], reverse=True)
for destination, count in sorted_destinations:
    print(destination, count)
print()
print('Total Applications:')
sorted_totals = sorted(total_dict.items(), key=lambda x: x[1], reverse=True)
for app, count in sorted_totals:
    print(app, count)
print()
sorted_pairs = sorted(pairs_dict.items(), key=lambda x: x[1], reverse=True)
print('Source-Destination Pairs:')
for pair, count in sorted_pairs:
    print(pair, count)
print()

