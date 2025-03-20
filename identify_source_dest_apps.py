from openai import OpenAI
import json
import time
from rich import print

client = OpenAI()

def call_openai(messages, model="gpt-3.5-turbo"):
    response = client.chat.completions.create(model=model,  # or another model like "gpt-4" if available
                                              messages=messages)
    return response.choices[0].message.content
# Create a chat completion request with a system and a user message


def identify_source_and_destination_applications(flow_description):
    sys_message = {"role": "system", "content": '''You are expert in extractive question answering 
                   and extracting data from text. 
                   For the description given, extract the source application from where data is fetched,
                   the type of resource that is being fetched, destination
                   application where data is pushed, destination resource that is
                   being created or modified, and any other flow conditions.
                   If the destination resource has a pronoun, resolve it with
                   the noun. Avoid any adjectives in the source and destination
                   resources.
                   . Dont explain or suggest. Just
                   answer in json with the following keys source_application,
                   source_resource, destination_application, destination_resource,
                   flow_condition
                   '''}
    user_message = {"role": "user", "content": "Text : ```{}```"}
    user_message["content"] = user_message["content"].format(flow_description)
    messages = [sys_message, user_message]
    response = call_openai(messages)

    # Check for the presenece of keys
    response_obj = json.loads(response)
    if 'source_application' not in response_obj:
        response_obj['source_application'] = ""
    if 'source_resource' not in response_obj:
        response_obj['source_resource'] = ""
    if 'destination_application' not in response_obj:
        response_obj['destination_application'] = ""
    if 'destination_resource' not in response_obj:
        response_obj['destination_resource'] = ""
    return response_obj
