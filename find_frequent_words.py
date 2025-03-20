import json
from rich import print
from rich.progress import track
import sqlite3
database = 'flowgen.db'

tables = ['raw_flows', 'raw_exports', 'raw_imports']
conn = sqlite3.connect(database)
def main():
    rows_to_fetch = 100000
    word_count = {}

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
                #print(content)
                words = content.split()
                for word in words:
                    if word in word_count:
                        word_count[word] += 1
                    else:
                        word_count[word] = 1
            print("Current status: Found unique words=", len(word_count))

    print("Found unique words=", len(word_count))

    sorted_word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)
    for word, count in sorted_word_count[:1000]:
        print(f"{word}: {count}")

    # All plurals
    fp = open('plural_record_types.txt', 'w')
    for word, count in track(sorted_word_count, "Writing record types to file"):
        if word.endswith('s') and count > 100:
            #print(f"Plural {word}: {count}")
            fp.write(f"{word}: {count}\n")
    fp.close()


if __name__ == "__main__":
    main()
