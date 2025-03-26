from openai import OpenAI
import json

model_flow_steps = 'ft:gpt-4o-mini-2024-07-18:celigo-inc:flowgenv2-scaffolding-v4:BEzlWjgz'
model_export_description_to_export_json = 'ft:gpt-4o-mini-2024-07-18:celigo-inc:flowgenv2-exports-v4:BDS1YAIL'
model_import_description_to_import_json = 'ft:gpt-4o-mini-2024-07-18:celigo-inc:flowgenv2-imports:BDVuhsEk'

SYSTEM_MSG_FLOW_TO_STEPS = '''You are an expert in creating the plan for a flow. You are helping     a user create the plan in json. '''

SYSTEM_MSG_EXPORT_TO_EXPORT_JSON = '''You are an expert in creating json export configuration for Celigo Integrator. You are helping a user create their export configuration in json.'''
SYSTEM_MSG_IMPORT_TO_IMPORT_JSON = '''You are an expert in creating json export configuration for Celigo Integrator. You are helping a user create their export configuration in json.'''

USER_MSG_FLOW_TO_STEPS = '''create a plan for a flow with this description: '''
USER_MSG_EXPORT_DESC_TO_EXPORT_JSON = '''create a json export configuration for this description: '''
USER_MSG_IMPORT_DESC_TO_IMPORT_JSON = '''create a json export configuration for this description: '''

client = OpenAI()

flow_examples = [
    "sync Shopify orders with cancelled status as NetSuite closed sales orders.",
    "sync NetSuite sales order cancellations as BigCommerce order cancellations.",
    "sync NetSuite opportunity emails as Salesforce opportunity email messages.",
    "sync SalesForce contacts as NetSuite contacts",
    "sync Salesforce opportunities into Acumatica as sales order record",
]

def create_messages(system_message, user_message, payload):
    messages = []
    sys_message = {
        "role": "system",
        "content": system_message,
    }
    messages.append(sys_message)
    user_message = {
        "role": "user",
        "content": user_message+payload,
    }
    messages.append(user_message)
    return messages

def generate_import_json_from_import_description(import_description):
    messages = create_messages(SYSTEM_MSG_IMPORT_TO_IMPORT_JSON,
                               USER_MSG_IMPORT_DESC_TO_IMPORT_JSON,
                               import_description)
    print(json.dumps(messages, indent=4))
    response = client.chat.completions.create(model=model_import_description_to_import_json, messages=messages, temperature=0)
    return response.choices[0].message.content


def generate_export_json_from_export_description(export_description):
    messages = create_messages(SYSTEM_MSG_EXPORT_TO_EXPORT_JSON,
                               USER_MSG_EXPORT_DESC_TO_EXPORT_JSON,
                               export_description)
    print(json.dumps(messages, indent=4))
    response = client.chat.completions.create(model=model_export_description_to_export_json,
                                   messages=messages, temperature=0)
    return response.choices[0].message.content

def generate_flow_steps_json_from_flow_description(flow_description):
    messages = create_messages(SYSTEM_MSG_FLOW_TO_STEPS,
                               USER_MSG_FLOW_TO_STEPS, flow_description)
    print(json.dumps(messages, indent=4))
    response = client.chat.completions.create(model=model_flow_steps,
                                   messages=messages, temperature=0)
    return response.choices[0].message.content



