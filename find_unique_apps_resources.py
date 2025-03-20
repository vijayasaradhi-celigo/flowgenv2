import json
import sys
filename = 'prod_flows_inferred.jsonl'
fp = open(filename, 'r')
all_apps = set()
all_resources = []
cx = 0
for line in fp:
    cx+=1
    data = json.loads(line)
    print(cx, data)
    print(data.keys())
    src_app = data['source_application']
    if isinstance(src_app, list):
        print("Found list")
        for app in src_app:
            if isinstance(app, list):
                print(app)
                sys.exit()
            all_apps.add(app)
    else:
        all_apps.add(src_app)
    dest_app = data['destination_application']
    if isinstance(dest_app, list):
        print("Dest app Found list")
        for app in dest_app:
            if isinstance(app, list):
                print(app)
                sys.exit()
            all_apps.add(app)
    else:
        all_apps.add(dest_app)
    src_res = data['source_resource']
    if isinstance(src_res, list):
        print("Found list")
        for res in src_res:
            if isinstance(res, list):
                print(res)
                sys.exit()
            all_resources.append(res)
    else:
        all_resources.append(src_res)

    dest_res = data['destination_resource']
    if isinstance(dest_res, list):
        print("Dest res Found list")
        for res in dest_res:
            if isinstance(res, list):
                print(res)
                sys.exit()
            all_resources.append(res)
    else:
        all_resources.append(dest_res)
all_apps = list(set(all_apps))
all_resources = list(set(all_resources))
print("Unique Applications: ", len(all_apps))
print("Unique Resources: ", len(all_resources))
fp.close()

for app in all_apps:
    print(app)
print("Total apps: ", len(all_apps))
sys.exit()

for res in all_resources:
    print(res)

