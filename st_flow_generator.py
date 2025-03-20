import streamlit as st
import typesense
from identify_source_dest_apps import identify_source_and_destination_applications
from normalize_apps_and_resource import normalize_applications_and_resources
from show_all_keys import fields_to_ignore
import sqlite3
import json

conn = sqlite3.connect('flowgen.db')
#from model_helpers import *
st.set_page_config(page_title="Flow Generator", page_icon=":shark:", layout="wide")

st.title('Flow Generator')

def connect_to_typesense():
    try:
        client = typesense.Client({
			'api_key': 'xyz',
			'nodes': [{
				'host': '192.168.64.2',
				'port': '8108',
				'protocol': 'http'
			}],
			'connection_timeout_seconds': 20
		})
        return client
    except Exception as e:
        st.write(f"Error connecting to typesense: {e}")
        st.stop()

client = connect_to_typesense()
def main():
    description = st.text_input("Describe your flow:")
    create_in_io = st.checkbox("Create components in integrator.io")
    if st.button("Generate Flow components"):
        response_obj = identify_source_and_destination_applications(description)
        #st.write(response_obj)
        normalized_obj = normalize_applications_and_resources(response_obj)
        given_normalized_source_application = normalized_obj['normalized_source_application']
        given_normalized_destination_application = normalized_obj['normalized_destination_application']
        #st.write("Normalized: ", normalized_obj)
        st.write("Generating components...")

        # query typesense
        search_parameters = {
            'q': description,
            'query_by': "*",
            'exhaustive_search': True,
            'page': 1,
            'per_page': 50,
            #'sort_by': sort_by
        }
        try:
            response = client.collections['flow_steps_ds'].documents.search(search_parameters)
            #print(response)
        except Exception as e:
            print(f"Error searching for flow steps: {e}")
            print(e)
        results = response['hits']
        possible_hits = []
        for result in results:
            doc= result['document']
            it_normailized_source_application = doc['normalized_source_application']
            it_normalized_destination_application = doc['normalized_destination_application']
            if it_normailized_source_application == given_normalized_source_application and it_normalized_destination_application == given_normalized_destination_application:
            #if True:
                #st.write(doc['flow_id'], doc['description'])
                #st.write(doc['normalized_source_application'], doc['normalized_destination_application'])
                #st.write(doc['source_application'], doc['destination_application'])
                steps = doc['steps']
                num_steps = len(steps)
                if num_steps in (2, 3):
                    possible_hits.append(doc)


        if len(possible_hits) == 0:
            st.write("No matching flow steps found.")
            st.stop()

        # For now take the first result, but aggregate in the future
        hit = possible_hits[0]
        #st.write(hit)
        steps = hit['steps']
        for step in steps:
            st.write(step['name'], step['description'])
            step_obj = create_flow_step(step)
            st.json(step_obj)
            st.markdown("---")


def create_flow_step(step):
    client = connect_to_typesense()
    if step['type'] == 'import':
        collection = 'imports'
    else:
        collection = 'exports'
    search_parameters = {
        'q': step['name'],
        'query_by': "*",
        'exhaustive_search': True,
        'page': 1,
        'per_page': 50,
        #'sort_by': sort_by
    }
    try:
        response = client.collections[collection].documents.search(search_parameters)
        print(response)
    except Exception as e:
        print(f"Error searching for flow steps: {e}")
        print(e)
    results = response['hits']
    if len(results) == 0:
        st.write(f"No matching {collection} found for step {step['name']}")
        st.stop()

    hit = results[0]
    doc = hit['document']
    eid = doc['id']
    doc_json = fetch_step_json(eid, collection)

    #st.write(doc_json)
    #st.markdown("---")
    return doc_json

def fetch_step_json(eid, collection):
    raw_table = "raw_"+collection
    #st.write("Querying from raw_table: ", raw_table)
    sql= f"SELECT V FROM {raw_table} WHERE ID = '{eid}'"
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    V = row[0]
    json_v = json.loads(V)
    #st.json(json_v)
    stripped_document = {k: v for k, v in json_v.items() if k not in fields_to_ignore}
    print("Stripped document={}".format(stripped_document))
    return stripped_document

def test():
    st.text_area("Generated Export", value=json.dumps(export_json_obj, indent=4), height=600)
    st.text_area("Generated Import", value=json.dumps(import_json_obj, indent=4), height=600)
    st.text_area("Generated Flow", value=json.dumps(flow_json_obj, indent=4), height=600)
    if create_in_io:
        #create_connection(export_json_obj, import_json_obj)
        export_json_obj['_connectionId'] = export_connection_ids[export_json_obj['adaptorType']]
        export_json_obj['name'] = export_description
        exp = create_export(export_json_obj)
        export_id = exp['_id']
        print("Returned Export: ", exp)
        import_json_obj['_connectionId'] = import_connection_ids[import_json_obj['adaptorType']]
        import_json_obj['name'] = import_description
        imp = create_import(import_json_obj)
        import_id = imp['_id']
        print("Returned Import: ", imp)

        flow_json_obj['pageGenerators'] = [{"_exportId": export_id}]
        flow_json_obj['pageProcessors'] = [{'_importId': import_id, 'type': 'import'}]
        flow_json_obj['name'] = "AUTOGEN:" + description
        if 'schedule' in flow_json_obj:
            del flow_json_obj['schedule']

        flow = create_flow(flow_json_obj)
        print("Returned Flow: ", flow)
        st.write("Flow created in integrator.io")


#with st.expander("Examples"):
#    all_examples_content = '\n'.join(flow_examples)
#    st.text_area("all_examples", all_examples_content, height=200)
if __name__ == "__main__":
    main()
