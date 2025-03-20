import sys
import pickle
from rank_bm25 import BM25Okapi

with open('tokenized_exports.pkl', 'rb') as f:
    export_corpus = pickle.load(f)
print(f"#documents in export corpus={len(export_corpus)}")


# Load the import corpus

with open('tokenized_imports.pkl', 'rb') as f:
    import_corpus = pickle.load(f)
print(f"#documents in import corpus={len(import_corpus)}")


bm25e = BM25Okapi(export_corpus)
bm25i = BM25Okapi(import_corpus)
query = 'acumatica shipments to shopify fulfillments this integration flow syncs acumatica shipments as shopify fulfillments.'

print("Fetching the top 5 documents...")
print("Query: ", query)
tokenized_query = query.split(" ")
docs = bm25e.get_top_n(tokenized_query, export_corpus, n=5)
for doc in docs:
    print(doc)



docs = bm25i.get_top_n(tokenized_query, import_corpus, n=5)
for doc in docs:
    print(doc)
