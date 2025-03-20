import sqlite3
import pandas as pd
import json

conn = sqlite3.connect('flowgen.db')

def fetch_http_exports():
    sql = '''select description, count(*) as cx from http_exports where description not in (select description from http_exports where description!='' 
and description not like '%sync%'                                                                                  
and description not like '%complete%' 
and description not like '%import%' 
and description not like '%export%' 
and description not like '%fetch%'
and description not like '%add%' 
and description not like '%update%' 
and description not like '%create%'
and description not like '%request%' 
and description not like '%clone%' 
and description not like '%cancel%' 
and description not like '%write%' 
and description not like '%configure%' 
and description not like '%send%' 
and description not like '%get%' 
and description not like '%deposits%' 
and description not like '%generate%' 
and description not like '%process%' 
and description not like '%convert%' 
and description not like '%retrieve%' 
and description not like '%lookup%' 
and description not like '%look%' 
and description not like '%check%' 
and description not like '%pull%' 
and description not like '%list%' 
and description not like '%return%' 
and description not like '%find%' 
and description not like '%read%' 
and description not like '%search%' 
and description not like '%extract%' 
and description not like '%pick%' 
and description not like '%transfer%' 
and description not like '%open%'
and description not like '%match%' 
and description not like '%collect%' 
and description not like '%publish%' 
and description not like '%download%' 
and description not like '%query%' 
and description not like '%grab%' 
and description not like '%load%' 
and description not like '%bring%' 
and description not like '%upload%' ) and LENGTH(description) < 100 and LENGTH(description) > 30 
GROUP BY description ORDER BY count(*) DESC;
'''
    fp = open('http_exports_filtered_rules.json', 'w')
    df = pd.read_sql_query(sql, conn)
    print("Shape of the dataframe: ", df.shape)
    for index, row in df.iterrows():
        print(row['description'], row['cx'])
        obj = {}
        obj['description'] = row['description']
        obj['count'] = row['cx']
        fp.write(json.dumps(obj)+'\n')
    fp.close()

def fetch_http_imports():
    sql = '''select description, count(*) as cx from http_imports where description not in (select description from http_exports where description!='' 
and description not like '%sync%'
and description not like '%complete%' 
and description not like '%import%' 
and description not like '%export%' 
and description not like '%fetch%'
and description not like '%add%' 
and description not like '%update%' 
and description not like '%create%'
and description not like '%request%' 
and description not like '%clone%' 
and description not like '%cancel%' 
and description not like '%write%' 
and description not like '%configure%' 
and description not like '%send%' 
and description not like '%get%' 
and description not like '%deposits%' 
and description not like '%generate%' 
and description not like '%process%' 
and description not like '%convert%' 
and description not like '%retrieve%' 
and description not like '%lookup%' 
and description not like '%look%' 
and description not like '%check%' 
and description not like '%pull%' 
and description not like '%list%' 
and description not like '%return%' 
and description not like '%find%' 
and description not like '%read%' 
and description not like '%search%' 
and description not like '%extract%' 
and description not like '%pick%' 
and description not like '%transfer%' 
and description not like '%open%'
and description not like '%match%' 
and description not like '%collect%' 
and description not like '%publish%' 
and description not like '%download%' 
and description not like '%query%' 
and description not like '%grab%' 
and description not like '%load%' 
and description not like '%bring%' 
and description not like '%upload%' ) and LENGTH(description) < 100 and LENGTH(description) > 30 
GROUP BY description ORDER BY count(*) DESC;
'''
    fp = open('http_imports_filtered_rules.json', 'w')
    df = pd.read_sql_query(sql, conn)
    print("Shape of the dataframe: ", df.shape)
    for index, row in df.iterrows():
        print(row['description'], row['cx'])
        obj = {}
        obj['description'] = row['description']
        obj['count'] = row['cx']
        fp.write(json.dumps(obj)+'\n')
    fp.close()
def main():
    fetch_http_exports()
    fetch_http_imports()


if __name__ == "__main__":
    main()
