import boto3
import json
from os.path import join
import os
from tqdm import tqdm

cwd = os.path.dirname(os.path.realpath(__file__))
partitionids = ["customers", "sellers", "orders","products","items","payments","reviews","product_categories"]
data = [json.load(open(join(cwd,"customers.json"))), 
        json.load(open(join(cwd,"sellers.json"))),
        json.load(open(join(cwd,"orders.json"))),
        json.load(open(join(cwd,"products.json"))),
        json.load(open(join(cwd,"order_items.json"))),
        json.load(open(join(cwd,"order_payments.json"))),
        json.load(open(join(cwd,"order_reviews.json"))),
        json.load(open(join(cwd,"product_categories.json")))]

client = boto3.client("kinesis")

for partition,file in zip(partitionids,data):
    for record in tqdm(file):
        response = client.put_record(StreamName="ecomm-data-stream",
                        Data=json.dumps(record),
                        PartitionKey=partition)
    