import json
from sklearn.model_selection import train_test_split

def main():
    filename = 'ds_export_description_samples.jsonl'
    train_filename = 'ds_exports_json_to_desc_train_v5_openai_format.jsonl'
    test_filename = 'ds_exports_json_to_desc_test_v5_openai_format.jsonl'
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
    {"role": "system", "content": "You are an expert in creating the description of an export from its json representation for Celigo Integrator. You are helping a user create the escription of the export from  the json of the export."}, 
    {"role": "user", "content": "create the description for this export json {}".format(resource_json)},
    {"role": "assistant", "content": description}
]
}
    return json.dumps(converted)

if __name__ == '__main__':
    main()

