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
    input_filename = "flows_with_good_verbs.csv"
    target_filename = 'prod_flows_inferred.jsonl'
    processed_data = []
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
    for line in track(fp, total=8979):
        count += 1
        #if count > 10: break
        if count < 6069: continue
        flow_description = line.strip().strip('"')
        # Split flow description into multiple sentences using nltk
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
            response_obj["flow_description"] = flow_description
            response_obj["flow_description_first_sentence"] = first_sentence
            print(response_obj)
        except Exception as e:
            print("Error: ", e)
            print("Content: ", flow_description)
            continue
        ft.write(json.dumps(response_obj) + "\n")
        #time.sleep(2)
    fp.close()


if __name__ == "__main__":
    main()
