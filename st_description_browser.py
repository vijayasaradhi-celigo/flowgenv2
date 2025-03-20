import streamlit as st
import sqlite3
import pandas as pd
import json
from show_all_keys import fields_to_ignore
from flatten_json import flatten, unflatten
from most_similar_jsons import find_most_similar_json
from transformers import T5Tokenizer

conn = sqlite3.connect('flowgen.db')


def main():
    description = st.text_input("Enter the description")
    eid_type = st.selectbox("Select the type of entity", ["Export", "Lookup", "Import"])
    button = st.button("Submit")

    if button:
        fetch_data(description, eid_type)
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


def fetch_data(description, eid_type):
    eid_types_to_table = {
		"Export": "all_export_ids_descriptions",
		"Lookup": "all_lookup_ids_descriptions",
		"Import": "all_import_ids_descriptions"
	}
    raw_table_names = {
        "Export": "raw_exports",
        "Lookup": "raw_exports",
        "Import": "raw_imports"
    }
    table_name = eid_types_to_table[eid_type]
    raw_table_name = raw_table_names[eid_type]
    query = f"SELECT ID FROM {table_name} WHERE description == '{description}'"
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    ids = [row[0] for row in data]
    st.write(f"Fetched {len(ids)} ids")
    top_k_ids = ids[:]

    sql_in = str(tuple(top_k_ids)).replace(",)", ")")
    #st.write(sql_in)
    query = f"SELECT * FROM {raw_table_name} WHERE ID IN {sql_in}"
    cursor.execute(query)
    data = cursor.fetchall()
    json_objs = [json.loads(row[1]) for row in data]
    #st.write(json_objs[0])

    # Filter the jsons where they are deleted
    json_objs = [obj for obj in json_objs if obj.get('deletedAt') is None]
    st.write("Final objects=", len(json_objs))

    # sort the json_objs in the descending order of the lastModified field
    json_objs.sort(key=lambda x: x['lastModified'], reverse=True)
    processed_json_objs = [process_obj(obj) for obj in json_objs]

#    for obj in processed_json_objs[:10]:
#        st.json(obj)
#        st.markdown('---')
#
    # Find the most similar jsons
    most_similar_json = find_most_similar_json(processed_json_objs)
    idx = processed_json_objs.index(most_similar_json)
    #st.write("Most similar jsoni at idx=", idx)
    st.json(most_similar_json)
    num_t5_tokens = get_t5_tokenized_length(json.dumps(most_similar_json))
    st.write(num_t5_tokens)
    st.markdown('---')
    #st.json(json_objs[idx])


def get_t5_tokenized_length(text):
    # Tokenize the text with t5 tokenizer
    tokenizer = T5Tokenizer.from_pretrained('t5-small')
    tokenized_text = tokenizer(text)
    return len(tokenized_text['input_ids'])



if __name__ == '__main__':
    main()
