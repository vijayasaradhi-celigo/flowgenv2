import sqlite3
import pandas as pd
import json
from show_all_keys import fields_to_ignore
from flatten_json import flatten, unflatten
from most_similar_jsons import find_most_similar_json
from transformers import T5Tokenizer
from rich.progress import track
from rich import print


conn = sqlite3.connect('flowgen.db')
def strip_keys(obj):
    #return obj
    stripped_document = {k: v for k, v in obj.items() if k not in fields_to_ignore}
    return stripped_document

def process_obj(obj):
    stripped_obj = strip_keys(obj)
    flattened_obj = flatten(stripped_obj, '|')
    replaced_obj = unflatten(replace_keys_with_id(flattened_obj), '|')

    return replaced_obj

def replace_keys_with_id(obj):
    new_obj = {}
    for k,v in obj.items():

        if k.endswith('Id'):
            v='TBD_{}'.format(k.split('_')[-1])
            v='TBD'
        new_obj[k] = v 
    return new_obj



def main():
    samples=[]
    export_description_filename = 'most_frequent_export_descriptions.csv'
    export_description_df = pd.read_csv(export_description_filename)
    print(export_description_df.head())

    #Iterate the dataframe with DESCRIPTION column
    for index, row in track(export_description_df.iterrows(), total=len(export_description_df)):
        description = row['DESCRIPTION']
        print(description)
        sample = create_sample(description)
        if not sample:
            continue
        if sample['t5_tokens'] > 512:
            continue
        samples.append(sample)
    print(f"Generated {len(samples)} samples")
    with open('ds_export_description_samples.json', 'w') as f:
        json.dump(samples, f, indent=4)

    # Save in jsonl format for processing by machine
    fp=open('ds_export_description_samples.jsonl','w')
    for sample in samples:
        json.dump(sample, fp)
        fp.write('\n')
    fp.close()


def create_sample(description):
    raw_table_name = "raw_exports"
    query = f"SELECT ID FROM all_export_ids_descriptions WHERE description == '{description}'"
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    ids = [row[0] for row in data]
    print(f"Fetch {len(ids)} ids for description {description}")

    sql_in = str(tuple(ids)).replace(",)", ")")
    query = f"SELECT * FROM {raw_table_name} WHERE ID IN {sql_in}"
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    print(f"Fetch {len(data)} rows for description {description}")
    json_objs = [json.loads(row[1]) for row in data]
    # Filter the jsons where they are deleted
    json_objs = [obj for obj in json_objs if obj.get('deletedAt') is None]
    # sort the json_objs in the descending order of the lastModified field
    json_objs.sort(key=lambda x: x['lastModified'], reverse=True)
    processed_json_objs = [process_obj(obj) for obj in json_objs]
    most_similar_json = find_most_similar_json(processed_json_objs)
    num_t5_tokens = get_t5_tokenized_length(json.dumps(most_similar_json))
    if num_t5_tokens > 512:
        return None

    sample = {
        'description': description,
        'resource_json': json.dumps(most_similar_json),
        't5_tokens': num_t5_tokens
    }
    return sample

def get_t5_tokenized_length(text):
    tokenizer = T5Tokenizer.from_pretrained('t5-small')
    tokenized_text = tokenizer(text)
    return len(tokenized_text['input_ids'])

if __name__ == "__main__":
    main()
