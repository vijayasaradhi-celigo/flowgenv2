import sys
import json


import os

from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
from dotenv import load_dotenv

load_dotenv()
def get_completion(messages, model="gpt-4o"):
    response = client.chat.completions.create(model=model,
    messages=messages, temperature=0.0
    )
    return response



system_prompt = f"""
You are a helpful Celigo Inc. assistant with deep knowledge about the Celigo Integrator.io platform.
Use your own knowledge and the information provided to answer the user's question. 
Output your response in json format.

"""
user_prompt = """
You will be provided with a json of a flow containing the following fields -
flow_id, name, description and a list of resources.
Come up with a description that the flow is performing from its name and
description. Do not forget to include the source and the destination
application names in the flow description. 
For each of the resource in the flow, determine the type of resource it is and
come with a meaningful description of the resource. You can use the name and the
type of the resource to determine the description. 
Here is the input flow json:
    ```
    {flow_json}
    ```
"""


messages = [{'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
           ]
print("Sending request to OpenAI")
flow_json = {"description": "", "name": "", "resources": [
    {"name": "Shopify Order HTTP Connection", "description": "", "is_lookup": False, "type": "export"},
    {"name": "HTTP Big Commerce Lookup", "description": "", "is_lookup": True, "type": "export"}, {"name": "Big Commerce Order HTTP", "description": "", "is_lookup": False, "type": "import"}]}
print(flow_json)
response = get_completion(messages)
content = response.choices[0].message.content
prompt_tokens = response.usage.prompt_tokens
completion_tokens = response.usage.completion_tokens
total_tokens = prompt_tokens + completion_tokens
print("Prompt tokens: ", prompt_tokens)
print("Completion tokens: ", completion_tokens)
print("Total tokens: ", total_tokens)
print(content)
