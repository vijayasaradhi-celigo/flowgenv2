import marvin
import json
from rich import print
from rich.progress import track
from pydantic import BaseModel, Field

with open('assistants.txt', 'r') as f:
    assistants = f.read().splitlines()
    assistants_str = ' '.join(assistants)

class Details(BaseModel):
    source: str = Field(..., title="The source application from which data is fetched. This should be one among the following: "+assistants_str)
    destination: str = Field(..., title="The destination application to which data is sent. This should be one among the following: "+assistants_str)

def main():
    filename = 'sorted_flows_full_raw.jsonl'
    fp = open(filename, 'r')
    cx = 0
    ft = open('source_target_with_fid_full.jsonl', 'a')
    #for line in track(fp):
    for line in fp:
        cx += 1
        if cx < 6108: 
            print("Skipping ", cx)
            continue
        flow_obj = json.loads(line)
        flow_id = flow_obj.get('flow_id', 'UNK')
        flow_description = flow_obj.get('flow_description', '')
        flow_name = flow_obj.get('name', '')
        resources = flow_obj.get('resources', [])
        content = []
        content.append("Flow name: "+flow_name)

        content.append("Flow description:" + flow_description)
        for resource in resources:
            is_lookup = resource.get('is_lookup', False)
            if is_lookup:
                resource_type = 'lookup'
            else:
                resource_type = resource.get('type', '')
            content.append(resource_type)
            name = resource.get('name', '')
            content.append("Name: "+name)
            description = resource.get('description', '')
            content.append("Description:"+description)

        final_content = '\n'.join(content)
        print("Final content=", final_content)

        result = determine_source_destination(final_content)
        if result.source not in assistants:
            result.source = 'FAIL'
        if result.destination not in assistants:
            result.destination = 'FAIL'
        #print(flow_obj)
        print(result)
        if result:
            ft.write(json.dumps({'flow_id': flow_id, 'flow_name': flow_name, 'flow_description': flow_description, 'source': result.source, 'destination': result.destination})+'\n')
        print("Processed {} flows".format(cx))
        #if cx==100: break
    ft.close()

def determine_source_destination(content):
    results = marvin.extract(content, Details)
    if len(results) > 0:
        return results[0]

if __name__ == "__main__":
    main()
