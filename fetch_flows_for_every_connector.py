import pandas as pd
import sqlite3
from rich import print
from rich.progress import track

conn = sqlite3.connect("flowgen.db")

connector_df = pd.read_csv("all_connectors.csv")
connectors = connector_df["connector"].unique().tolist()
print("Total connectors: ", len(connectors))

def main():
    all_flow_ids = []
    for connector in track(connectors):
        print("Connector: ", connector)
        sql = f"SELECT ID FROM raw_flows where V like '%{connector}%' AND ID in (select flow_id from http_flows);"
        sql = f"SELECT ID FROM raw_flows where V like '%{connector}%';"
        df = pd.read_sql_query(sql, conn)
        print(f"Connector: {connector}, Total flows: {len(df)}")
        ids = df["ID"].tolist()
        all_flow_ids.extend(ids)

    print("Total flows: ", len(all_flow_ids))
    all_flow_ids = list(set(all_flow_ids))
    print("Unique flows: ", len(all_flow_ids))
    fp = open("all_flow_ids.txt", "w")
    for flow_id in all_flow_ids:
        fp.write(str(flow_id) + "\n")
    fp.close()
    print("Done")

if __name__ == "__main__":
    main()
