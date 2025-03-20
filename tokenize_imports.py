import sys
from rich.progress import track
import pickle

corpus = []
fp = open('all_enumerated_imports.txt', 'r')
for line in fp:
    corpus.append(line.strip())

tokenized_corpus = [doc.split(" ") for doc in track(corpus, description="Tokenizing corpus...")]

# Pickle the tokenized_export_corpus to a file
with open('tokenized_imports.pkl', 'wb') as f:
    pickle.dump(tokenized_corpus, f)


