import json
from sklearn.model_selection import train_test_split

def main():
    filename = 'ds_scaffolding_samples.jsonl'
    train_filename = 'ds_scaffolding_train_v4_openai_format.jsonl'
    test_filename = 'ds_scaffolding_test_v4_openai_format.jsonl'
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
    description = sample['flow_description']
    exports = sample['exports']
    imports = sample['imports']
    steps = exports + imports

    plan_json = json.dumps(steps)

    converted = {"messages": [
    {"role": "system", "content": "You are an expert in creating the plan for a flow. You are helping a user create the plan in json."}, 
    {"role": "user", "content": "create a plan for a flow with this description: {}".format(description)}, {"role": "assistant", "content": plan_json}
]
}
    return json.dumps(converted)

if __name__ == '__main__':
    main()

