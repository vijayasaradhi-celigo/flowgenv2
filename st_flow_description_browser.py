import sqlite3
import json
import streamlit as st
from rich import print
from rich.progress import track
import pandas as pd

st.set_page_config(layout="wide")

conn = sqlite3.connect('flowgen.db')
fields_to_ignore = ["__encryptedFieldsLastModified", "__v", "_connectionId",
                    "_sourceId", "_userId", "apiIdentifier", "createdAt",
                    "lastModified", "sandbox", "_templateId",]
def main():
    flow_name = st.text_input("Flow Name")
    all_rows = []
    if st.button("Search"):
        st.write("Searching for flow ID: ", flow_name)
        cursor = conn.cursor()
        cursor.execute('SELECT ID FROM all_flow_names_descriptions WHERE name = ?', (flow_name,))
        rows = cursor.fetchall()
        num_rows = len(rows)
        if len(rows) == 0:
            st.write("Flow not found")
            return
        st.write("Found ", num_rows, " flows")
        for row in rows:
            show_row(row)
def show_row(row):
    all_rows = []
    flow_id = row[0]
    V = fetch_flow(flow_id)
    flow_description = V['description']
    flow_name = V['name']
    page_generators = V['pageGenerators']
    page_processors = V['pageProcessors']
    #st.write("Number of pageGenerators: ", len(page_generators))
    #st.write("Number of pageProcessors: ", len(page_processors))

    for obj in page_generators+page_processors:
        resource = fetch_resource(obj)

        adaptorType = resource['adaptorType']
        lookup = resource.get('isLookup')
        name = resource['name']
        description = resource.get('description', "NOT AVAILABLE")
        #st.write(obj, name, description)
        #st.write(adaptorType, lookup)
        resource = strip_unnecessary_keys(resource)
        #st.json(resource, expanded=False)
        row = [obj, name, description, adaptorType, lookup]
        #st.markdown(" --- ")
        all_rows.append(row)
    df = pd.DataFrame(all_rows, columns=['ID', 'Name', 'Description', 'Adaptor Type', 'Lookup'])
    st.write("Flow ID:", flow_id, "Name", flow_name, "Description", flow_description)
    st.dataframe(df)
    st.markdown(" --- ")
        #st.write("Raw Flow")
        #st.json(V, expanded=False)



def strip_unnecessary_keys(resource):
    for key in fields_to_ignore:
        if key in resource:
            del resource[key]
    return resource

def fetch_resource(obj):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM raw_imports WHERE ID = ? UNION SELECT * FROM raw_exports WHERE ID=?', (obj, obj))
    row = cursor.fetchone()
    return json.loads(row[1])

def fetch_flow(flow_id):
    cursor = conn.cursor()
    cursor.execute('SELECT ID, V FROM raw_flows WHERE ID = ?', (flow_id,))
    row = cursor.fetchone()
    return json.loads(row[1])

if __name__ == "__main__":
    main()
