#!python3
import pandas as pd
import numpy as np

import json
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":

    #readdata
    file = pd.read_csv('../data/raw_data/bigdata_log.csv').to_dict(orient='records')
   
    producer = KafkaProducer(bootstrap_servers=['localhost'], 
                             value_serializer=json_serializer)
    
    while True:
        for data in file:
            print(data)
            producer.send("finalproject", data)
            time.sleep(1)
