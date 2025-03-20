from openai import OpenAI
import pandas as pd
import json
import time
from rich import print
from nltk.tokenize import sent_tokenize
from rich.progress import track

client = OpenAI()

def call_openai(messages, model="gpt-3.5-turbo"):
    response = client.chat.completions.create(model=model,  # or another model like "gpt-4" if available
                                              messages=messages)
    return response.choices[0].message.content
# Create a chat completion request with a system and a user message

def main():
    input_filename = "flow_ids_with_good_exp_imp_descriptions_final.jsonl"
    target_filename = 'flows_with_app_names_good_exp_imp_descriptions_final.jsonl'
    processed_data = []
    # Check processed data
    fp = open(target_filename, 'r')
    for line in fp:
        flow_obj = json.loads(line)
        processed_data.append(flow_obj['description'])
    fp.close()

    sys_message = {"role": "system", "content": '''You are expert in extractive question answering 
                   and extracting data from text. 
                   For the description given, extract the source application from where data is fetched,
                   the type of resource that is being fetched, target
                   application where data is pushed, target resource that is
                   being pushed, and any other flow conditions.
                   . Dont explain or suggest. Just
                   answer in json with the following keys source_application,
                   source_resource, destination_application, destination_resource,
                   flow_condition
                   '''}

    fp = open(input_filename, 'r')
    ft = open(target_filename, 'a')
    count = 0
    for line in track(fp, total=5627):
        count += 1
        line = line.strip().strip('"')
        flow_obj = json.loads(line)
        flow_description = flow_obj['description']
        if flow_description in processed_data: continue
        if len(flow_description) < 30: continue
        print("Processing flow {} with description: {}".format(count, flow_description))
        sentences = sent_tokenize(flow_description)
        first_sentence = sentences[0]

        user_message = {"role": "user", "content": "Text : ```{}```"}
        user_message["content"] = user_message["content"].format(first_sentence)
        messages = [sys_message, user_message]
        try:
            print("Content: ", flow_description)
            print("First Sentence: ", first_sentence)
            response = call_openai(messages)
            #print(response)
            response_obj = json.loads(response)
            flow_obj['source_application'] = response_obj['source_application']
            flow_obj['source_resource'] = response_obj['source_resource']
            flow_obj['destination_application'] = response_obj['destination_application']
            flow_obj['destination_resource'] = response_obj['destination_resource']
        except Exception as e:
            print("Error: ", e)
            print("Object: ", flow_obj)
            continue
        ft.write(json.dumps(flow_obj) + "\n")
        #time.sleep(2)
    fp.close()


if __name__ == "__main__":
    main()
