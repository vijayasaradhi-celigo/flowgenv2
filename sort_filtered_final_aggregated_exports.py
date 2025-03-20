import json
from rich.progress import track
filename = 'filtered_final_aggregated_exports.jsonl'
fp = open(filename, 'r')
objs = []
for line in track(fp):
    obj = json.loads(line)
    objs.append(obj)
fp.close()
sorted_objs = sorted(objs, key=lambda x: x[1], reverse=True)
ft = open('sorted_filtered_final_aggregated_exports.jsonl', 'w')
for obj in sorted_objs:

    ft.write(json.dumps(obj) + '\n')
ft.close()
