import json
from rich import print
from rich.progress import track
import sqlite3
import nltk
from nltk.tokenize import word_tokenize
from nltk import pos_tag

database = 'flowgen.db'

tables = ['raw_flows']
conn = sqlite3.connect(database)

def main():
    rows_to_fetch = 100000
    all_verbs = []

    for table in tables:
        cursor = conn.execute(f"SELECT V FROM {table}")
        while True:
            rows = cursor.fetchmany(rows_to_fetch)
            if not rows:
                break

            for row in track(rows, description=f"Processing {table}"):
                content = row[0]
                obj = json.loads(content)
                name = obj.get("name", '')
                description = obj.get('description', '')
                content = f"{name} {description}".lower()

                # Fetch all verbs from the content
                words = nltk.word_tokenize(content)
                pos = nltk.pos_tag(words)
                verbs = [word for word, tag in pos if tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']]
                all_verbs.extend(verbs)

    all_verbs = list(set(all_verbs))
    print("Found unique verbs=", len(all_verbs))

    # All verbs
    fp = open('all_verbs.txt', 'w')
    for word in all_verbs:
        fp.write(f"{word}\n")
    fp.close()


if __name__ == "__main__":
    main()
