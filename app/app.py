import os
import sys
import requests
import time
import logging
import boto3
from botocore.exceptions import ClientError
from os import listdir
from os.path import isfile, join
from os import environ

def fetch_jsons(entry):
    path, uri = entry
    start = time.time()
    if not os.path.exists(path):
        r = requests.get(uri, stream=True)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)

    print(time.time() - start)
    return path

def upload_file(file_name, bucket, object_name=None):
    
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__ == "__main__":

    base_url = "https://s3.amazonaws.com/data-sprints-eng-test/"
    bucket_name = 'de-bera-nyc-cab-trips'
    payloads = [
        ("data-files/data-nyctaxi-trips-2009.json", base_url + "data-sample_data-nyctaxi-trips-2009-json_corrigido.json"),
        ("data-files/data-nyctaxi-trips-2010.json", base_url + "data-sample_data-nyctaxi-trips-2010-json_corrigido.json"),
        ("data-files/data-nyctaxi-trips-2011.json", base_url + "data-sample_data-nyctaxi-trips-2011-json_corrigido.json"),
        ("data-files/data-nyctaxi-trips-2012.json", base_url + "data-sample_data-nyctaxi-trips-2012-json_corrigido.json"),
        ("data-files/data-vendor_lookup.csv", base_url + "data-vendor_lookup-csv.csv"),
        ("data-files/data-payment_lookup.csv", base_url + "data-payment_lookup-csv.csv")
    ]

    for entry in payloads:
        fetch_jsons(entry)
        upload_file(entry[0],bucket_name)     
    

      

