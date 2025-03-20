from openai import OpenAI

client = OpenAI()

def call_openai(messages, model="gpt-3.5-turbo"):
    response = client.chat.completions.create(model=model,  # or another model like "gpt-4" if available
                                              messages=messages)
    return response.choices[0].message.content
# Create a chat completion request with a system and a user message
messages=[
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Can you tell me a fun fact about space?"}
]

response = call_openai(messages)
print(response)
