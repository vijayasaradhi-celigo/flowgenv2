from openai import OpenAI
import json
import time
from rich import print
from rich.progress import track

client = OpenAI()

def call_openai(messages, model="gpt-3.5-turbo"):
    response = client.chat.completions.create(model=model,  # or another model like "gpt-4" if available
                                              messages=messages)
    return response.choices[0].message.content
# Create a chat completion request with a system and a user message

def main():
    filename = "http_imports_filtered_rules.json"
    target_filename = 'http_imports_filtered_rules_with_openai.jsonl'
    fp = open(target_filename, 'r')
    processed_data = []
    for line in fp:
        data = json.loads(line)
        processed_data.append(data['description'])
    fp.close()
    sys_message = {"role": "system", "content": '''You are expert in extractive question answering 
                   and extracting data from text. 
                   For the description given, extract the application from
                   where data is pushed to,
                   the type of resource that is being discussed and any filter
                   conditions that you see. Dont explain or suggest. Just
                   answer in json with the following keys application,
                   resource, filters.'''}

    fp = open(filename, 'r')
    ft = open(target_filename, 'a')
    count = 0
    for line in track(fp, total=5840):
        count += 1
        #if count > 10: break
        data = json.loads(line)
        if data['description'] in processed_data:
            continue
        user_message = {"role": "user", "content": "Text : ```{}```"}
        user_message["content"] = user_message["content"].format(data["description"])
        messages = [sys_message, user_message]
        #print("Content: ", data["description"])
        try:
            response = call_openai(messages)
            #print(response)
            response_obj = json.loads(response)
        except Exception as e:
            print("Error: ", e)
            print("Content: ", data["description"])
            continue
        data['open_ai_response'] = response_obj
        ft.write(json.dumps(data) + "\n")
        #time.sleep(2)
    fp.close()


if __name__ == "__main__":
    main()
