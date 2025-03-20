import sys
import typesense
from sentence_transformers import SentenceTransformer
from rich import print

def connect_to_typesense(host='localhost', port='8108', protocol='http', api_key='xyz'):
    client = typesense.Client({
    'api_key': api_key,
    'nodes': [{
        'host': host,
        'port': port,
        'protocol':protocol 
    }], 
    'connection_timeout_seconds': 600 
})
    return client


query = 'acumatica shipments to shopify fulfillments this integration flow syncs acumatica shipments as shopify fulfillments.'
query='amazon mws inventory adjustments(fba) to acumatica inventory adjustments this integration flow syncs amazon mws inventory adjustments(fba) as acumatica inventory adjustments.'
query='acumatica sales orders to slack chats this integration flow syncs acumatica sales orders as slack chats.'
query = 'evoretro - export marketplace orders to shopify export marketplace orders to shopify and saves the shopify order id  '
query = 'orangehrm employees(terminated) to acumatica inactive employees this integration flow syncs terminated orangehrm employees as acumatica inactive employees'
client = connect_to_typesense(host='192.168.64.2')
model = SentenceTransformer("all-mpnet-base-v2")
def main(query):
    export_candidates = []
    import_candidates = []
    top_export_candidates = []
    top_import_candidates = []

    search_params = {
    'q': query,
    'query_by': '*',
    'per_page': 20,
    'exhaustive_search': True,
    }
    response = client.collections['enumerated_exports'].documents.search(search_params)
    num_results = response['found']
    if num_results == 0: return []
    #print("Query=", query)
    #print(f"Found {num_results} results")
    results = response['hits']
    for result in results:
        document = result['document']
        content = document['name']
        #print("Retrieved document={}".format(content))
        export_candidates.append(content)

    # Do an embedding based nearest neighbor search
    query_embedding = model.encode(query)
    passage_embeddings = model.encode(export_candidates)
    #print(passage_embeddings.shape)
    similarity = model.similarity(query_embedding, passage_embeddings)
    #print(similarity.shape)
    # Flatten similarity to a 1-d array
    similarity = similarity.flatten()
    #print("Flattened similarity={}".format(similarity))
    doc_similarities = list(zip(export_candidates, similarity))
    #print("Document similarities:")
    #print(doc_similarities)
    #print("======")
    doc_similarities.sort(key=lambda x: x[1], reverse=True)
    #print(doc_similarities[:5])
    #print("Top 5 most similar exports:")
    for doc, sim in doc_similarities[:5]:
        #print(f"Document: {doc} Similarity: {sim}")
        top_export_candidates.append(doc)

    response = client.collections['enumerated_imports'].documents.search(search_params)
    num_results = response['found']
    if num_results == 0: return []
    #print(f"Found {num_results} results")
    results = response['hits']
    for result in results:
        document = result['document']
        content = document['name']
        import_candidates.append(content)

    # Do an embedding based nearest neighbor search
    query_embedding = model.encode(query)
    passage_embeddings = model.encode(import_candidates)
    #print(passage_embeddings.shape)
    similarity = model.similarity(query_embedding, passage_embeddings)
    #print(similarity.shape)
    # Flatten similarity to a 1-d array
    similarity = similarity.flatten().tolist()
    #print("Flattened similarity={}".format(similarity))
    doc_similarities = list(zip(import_candidates, similarity))
    #print("Document similarities:")
    #print(doc_similarities)
    #print("======")
    doc_similarities.sort(key=lambda x: x[1], reverse=True)
    #print(doc_similarities[:5])
    #print("Top 5 most similar imports:")
    for doc, sim in doc_similarities[:5]:
        #print(f"Document: {doc} Similarity: {sim}")
        top_import_candidates.append(doc)

    # Create possible flows and do a search
    all_flow_candidates = []
    for export_candidate in top_export_candidates:
        for import_candidate in top_import_candidates:
            flow_query = export_candidate + " " + import_candidate
            #print("Flow query=", flow_query)
            all_flow_candidates.append(flow_query)
    # Do an embedding based nearest neighbor search
    query_embedding = model.encode(query)
    passage_embeddings = model.encode(all_flow_candidates)
    #print(passage_embeddings.shape)
    similarity = model.similarity(query_embedding, passage_embeddings)
    #print(similarity.shape)
    # Flatten similarity to a 1-d array
    similarity = similarity.flatten().tolist()
    #print("Flattened similarity={}".format(similarity))
    doc_similarities = list(zip(all_flow_candidates, similarity))
    #print("Document similarities:")
    #print(doc_similarities)
    #print("======")
    doc_similarities.sort(key=lambda x: x[1], reverse=True)
    #print(doc_similarities[:5])
    #print("Query=", query)
    #print("Top 5 most similar documents:")
    #for doc, sim in doc_similarities[:5]:
    #    print(f"Document: {doc} Similarity: {sim}")
    return doc_similarities[:5]



if __name__ == '__main__':
    top_docs = main(query)
    for doc in top_docs:
        print(doc)
