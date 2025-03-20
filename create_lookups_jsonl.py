import sqlite3
import json
from rich import print
from rich.progress import track

db='flowgen.db'
conn = sqlite3.connect(db)
c = conn.cursor()
sql = "SELECT * from http_exports where is_lookup ='True'"
c.execute(sql)
rows = c.fetchall()
conn.close()
with open('http_lookups.jsonl', 'w') as f:
    for row in track(rows):
        obj = {}
        obj['id'] = row[0]
        obj['name'] = row[1]
        obj['description'] = row[2]
        obj['is_lookup'] = row[3]
        f.write(json.dumps(obj) + '\n')
