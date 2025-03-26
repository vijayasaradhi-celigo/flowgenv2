import streamlit as st
import json
from model_helpers import *
import json
import pandas as pd
from time import time
from generic_flow_skeleton import skeleton_flow
import uuid

st.set_page_config(page_title="Flow Generator", page_icon=":shark:", layout="wide")

st.title('Flow Generator')

def main():
    all_import_ids = []
    all_export_ids = []
    total_time = 0
    flow_description = st.text_input("Describe your flow:")
    create_in_io = st.checkbox("Create components in integrator.io")
    if st.button("Generate Flow components"):
        t1 = time()
        flow_steps = generate_flow_steps_json_from_flow_description(flow_description)
        #st.json(flow_steps)
        flow_steps = json.loads(flow_steps)
        # Create a dataframe of flow steps
        flow_steps_df = pd.DataFrame(flow_steps)
        t2 = time()
        st.success(f"Generated flow steps in {t2-t1:.2f} seconds")
        total_time += t2-t1
        st.markdown("### Flow Steps")
        st.table(flow_steps_df)
        for step in flow_steps:
            step_type = step['step_type']
            step_description = step['description']
            with st.spinner(f"Generating {step_type} JSON"):
                t1 = time()
                step_json = create_flow_step_json(step_type, step_description)
                step_json = json.loads(step_json)
                if step_type == 'import':
                    step_json = post_process_import(step_json)
                t2 = time()
                total_time += t2-t1
                st.success(f"Generated {step_type} JSON in {t2-t1:.2f} seconds")
                step_json['description'] = step_description
                step_id = generate_id()
                if step_type == 'export':
                    all_export_ids.append(step_id)
                elif step_type == 'import':
                    all_import_ids.append(step_id)

            st.text_area(f"{step_type} JSON", value=json.dumps(step_json, indent=4), height=600)

        # Generate the flow skeleton
        t1 = time()
        flow_json_obj = dict(skeleton_flow)
        flow_json_obj['description'] = flow_description
        flow_json_obj['pageGenerators'] = all_export_ids
        flow_json_obj['pageProcessors'] = all_import_ids
        flow_json_obj['name'] = "AUTOGEN:" + flow_description
        t2 = time()
        total_time += t2-t1
        st.text_area(f"Flow JSON", value=json.dumps(flow_json_obj, indent=4), height=600)
        st.success(f"Generated flow JSON in {total_time:.2f} seconds")
    with st.expander("Examples"):
        all_examples_content = '\n'.join(flow_examples)
        st.text_area("Examples", all_examples_content, height=200)

def generate_id():
    return str(uuid.uuid4())

def post_process_import(import_json_obj):
    print("Entered post_process_import")
    print(import_json_obj)
    # Convert any keys with 0 as keyname to list
    return convert_obj(import_json_obj)

def convert_obj(obj):
    """
    Recursively traverses the object.
    If a dictionary has exactly one key and that key is 0,
    it replaces that dictionary with a list containing only the corresponding value.
    """
    if isinstance(obj, dict):
        # First process each value recursively.
        new_dict = {k: convert_obj(v) for k, v in obj.items()}
        # If the dict has exactly one key and the key is 0, replace it.
        if len(new_dict) == 1 and '0' in new_dict:
            return [new_dict['0']]
        return new_dict
    elif isinstance(obj, list):
        # Process each element in the list recursively.
        return [convert_obj(item) for item in obj]
    else:
        # For non-dict and non-list objects, return them as is.
        return obj


def create_flow_step_json(step_type, step_description):
    if step_type == 'export':
        export_json_obj = generate_export_json_from_export_description(step_description)
        return export_json_obj
    elif step_type == 'import':
        import_json_obj = generate_import_json_from_import_description(step_description)
        return import_json_obj
    else:
        return None
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
