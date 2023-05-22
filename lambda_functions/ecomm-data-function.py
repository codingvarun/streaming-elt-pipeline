import os
import json
import pandas as pd
import pymysql as sql
from base64 import b64decode
from datetime import datetime

def connect():
    connection = sql.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["DATABASE"],
        port=3306,
    )
    return connection


def group_kinesis_payload(event):
    records = event["Records"]
    records = pd.DataFrame(records)
    kinesis_data = list(records["kinesis"])
    kinesis_df = pd.DataFrame(kinesis_data)
    kinesis_df["data"] = kinesis_df["data"].apply(b64decode).apply(json.loads)
    groups = list(pd.unique(kinesis_df["partitionKey"]))
    kinesis_gb = kinesis_df.groupby(by=["partitionKey"])
    return kinesis_gb, groups


def lambda_handler(event, context):
    
    kinesis_gb, groups = group_kinesis_payload(event)
    inserts = {}
    for group in groups:
        data = kinesis_gb.get_group(group)
        insert = f"insert into {group} values".upper() \
                    + ",".join( \
                        list( \
                            data["data"] \
                            .apply(lambda x: tuple(map(lambda x: '' if x==None else x,list(x.values())))) \
                            .apply(str) \
                        ) \
                    ) \
                    + ";"
        inserts[group] = insert
    with connect() as connection:
        with connection.cursor() as cursor:
            try:
                for group,insert in inserts.items():
                    print("TABLE:", group)
                    # print(insert)
                    print("inserted",cursor.execute(insert),f"rows into table {group}")
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()