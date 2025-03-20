import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from rich import print
from rich.progress import track
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

system_prompt = f"""
You are an expert in various end points of the various http applications. Answer the user's question about the possible resource referred in the end point. Just mention the resource name, do not give any explanations.
Identify the resource used in this web request
"""


def main():
    filename = 'sorted_all_existing_assistant_flows_with_keys.json'
    with open(filename, 'r') as f:
        data = json.load(f)
    print("Loaded {} samples".format(len(data)))
    fp = open("runlog.jsonl", "a")
    final_data = []
    i= 0
    past_flow_names = []
    total = len(data)
    for obj in track(data):
        i+=1
        if i < 11618:
            continue
        if obj["flow_name"] == "Create a max number of flows": continue
        if obj["flow_name"] == "Clone flow mapping from one store to others": continue
        flow_name = obj["flow_name"]
        if flow_name in past_flow_names: continue
        past_flow_names.append(flow_name)
        final_obj = process(obj)
        print("Processed: {} of {}".format(i, total))
        print(final_obj)
        final_data.append(final_obj)
        fp.write(json.dumps(final_obj) + "\n")
    fp.close()

    with open('sorted_all_existing_assistant_flows_with_keys_resources.json', 'w') as f:
        json.dump(final_data, f, indent=2)


def process(obj):
    flow_steps = obj['assistants']
    final_flow_steps = []
    for flow_step in flow_steps:
        new_obj = identify_resource_type(flow_step)
        final_flow_steps.append(new_obj)
    obj['assistants'] = final_flow_steps
    return obj

def identify_resource_type(flow_step):
    obj = {}
    messages = [{'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': flow_step}]
    response = get_completion(messages)
    content = response.choices[0].message.content
    obj['resource'] = content
    obj['flow_step'] = flow_step

    return obj


def get_completion(messages, model="gpt-4o"):
    response = client.chat.completions.create(model=model,
    messages=messages, temperature=0.0
    )
    return response




if __name__ == '__main__':
    main()
#print("Sending request to OpenAI")
#flow_json = {"description": "", "name": "", "resources": [
#    {"name": "Shopify Order HTTP Connection", "description": "", "is_lookup": False, "type": "export"},
#    {"name": "HTTP Big Commerce Lookup", "description": "", "is_lookup": True, "type": "export"}, {"name": "Big Commerce Order HTTP", "description": "", "is_lookup": False, "type": "import"}]}
#print(flow_json)
#response = get_completion(messages)
#content = response.choices[0].message.content
#prompt_tokens = response.usage.prompt_tokens
#completion_tokens = response.usage.completion_tokens
#total_tokens = prompt_tokens + completion_tokens
#print("Prompt tokens: ", prompt_tokens)
#print("Completion tokens: ", completion_tokens)
#print("Total tokens: ", total_tokens)
#print(content)
