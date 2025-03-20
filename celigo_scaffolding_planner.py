import os
import re
from typing import Tuple, Dict


from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_openai import ChatOpenAI

from langchain.tools import BaseTool, StructuredTool, tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import pandas as pd
from identify_source_dest_apps import identify_source_and_destination_applications
from normalize_apps_and_resource import  normalize_app


@tool
def get_supported_end_points_for_export(app_name)->str:
    """Fetches the list of supported end points and their description  for the
    export application. Each element is an object with keys 'name', 'description',
    'method' and 'relativeuri'."""
    connectors_df = pd.read_csv('all_connectors.csv')
    connectors_df = connectors_df[(connectors_df['connector'] == app_name) &
                                  (connectors_df['method'] == 'GET')]
    connectors = connectors_df[['name', 'description', 'method','relative_uri']].to_dict('records')
    return connectors
@tool
def get_supported_end_points_for_import(app_name)->str:
    """Fetches the list of supported end points and their description  for the
    import application. Each element is an object with keys 'name', 'description',
    'method' and 'relativeuri'."""
    connectors_df = pd.read_csv('all_connectors.csv')
    connectors_df = connectors_df[(connectors_df['connector'] == app_name) &
                                  (connectors_df['method'] != 'GET')]
    connectors = connectors_df[['name', 'description', 'method','relative_uri']].to_dict('records')
    return connectors

@tool

def get_source_and_destination_applications(flow_description:str)->Dict:
    """Fetches the source application and destination application for the given
    description of the flow with keys source_application and destination_application"""
    obj = identify_source_and_destination_applications(flow_description)
    obj['source_application'] = normalize_app(obj['source_application'])
    obj['destination_application'] = normalize_app(obj['destination_application'])
    return obj

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", """ You are an expert in planning the sequence of end points
        that need to be called to  move data from source application to
        destination application. You will have
        a source application and the list of supported end points in the source
        application. You will have the destination application and the list of
        supported endpoints. Please do not explain or suggest. Output in json
         format with a list of steps. Each step should contain the description
         of the step and the end point that needs to be called
        """),
        ("human", '''Plan the list of endpoints that have to be called to achieve
         this description:
         {flow_description}'''),
        MessagesPlaceholder("agent_scratchpad"),
    ]
)
def main(flow):
    print("Entered main with data ", flow)
    # Choose the LLM that will drive the agent
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Setup the toolkit
    toolkit = [get_supported_end_points_for_export,
               get_supported_end_points_for_import,
               get_source_and_destination_applications]

    # Construct the OpenAI Tools agent
    agent = create_openai_tools_agent(llm, toolkit, prompt)

    agent_executor = AgentExecutor(agent=agent, tools=toolkit, verbose=True)
    result = agent_executor.invoke({'flow_description': flow['description']})
    print(result['output'])
    return result['output']


if __name__ == "__main__":
    flow = {'description': "Sync all new microsoft dynamics 365 business central customers into shopify"}
    flow = {'description': "Fetch purchase orders from Business Central and insert into Shopify"}
    main(flow)

