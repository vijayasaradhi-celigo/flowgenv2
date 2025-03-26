import json
from sklearn.model_selection import train_test_split

def main():
    filename = 'ds_lookup_description_samples.jsonl'
    train_filename = 'ds_lookups_train_v4_openai_format.jsonl'
    test_filename = 'ds_lookups_test_v4_openai_format.jsonl'
    ftrain = open(train_filename, 'w')
    ftest = open(test_filename, 'w')

    samples = [json.loads(line) for line in open(filename)]
    print("Loaded {} samples".format(len(samples)))
    converted_samples = [convert_sample_to_openai_format(sample) for sample in samples]
    # Split the converted samples into train and test
    train_samples, test_samples = train_test_split(converted_samples, test_size=0.2)
    for sample in train_samples:
        ftrain.write(sample + '\n')
    ftrain.close()
    for sample in test_samples:
        ftest.write(sample + '\n')
    ftest.close()


def convert_sample_to_openai_format(sample):
    description = sample['description']
    resource_json = sample['resource_json']
    converted = {"messages": [
    {"role": "system", "content": "You are an expert in creating json export configuration for Celigo Integrator. You are helping a user create their export configuration in json."}, 
    {"role": "user", "content": "create a json export configuration for this description: {}".format(description)}, {"role": "assistant", "content": resource_json}
]
}
    return json.dumps(converted)

if __name__ == '__main__':
    main()

