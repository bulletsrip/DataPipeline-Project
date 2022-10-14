#!python3
import os
import json
import pandas

from kafka import KafkaConsumer
from sqlalchemy import create_engine

def transformStream(df):
    df = df \
            .groupby(['Id','search_id','search_date','search_product']) \
            .reset_index()
    # df.columns = ['Id','newbalanceDest','device', 'timeformat1', 'timeformat2']
    
if __name__ == "__main__":
    print("starting the consumer")
    path = os.getcwd()+"\\"

    #connect database
    try:
        engine = create_engine('postgresql://yudityainsani:123456@localhost:5432/data_warehouse')
        print(f"[INFO] Successfully Connect Database .....")
    except:
        print(f"[INFO] Error Connect Database .....")

    #connect kafka server
    try:
        consumer = KafkaConsumer("finalproject", bootstrap_servers='localhost')
        print(f"[INFO] Successfully Connect Kafka Server .....")
    except:
        print(f"[INFO] Error Connect Kafka Server .....")

    #read message from topic kafka server
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")
        
        #insert database   
        df = pandas.DataFrame(data, index=[0])
        df.to_sql('stream_userlog', engine, if_exists='append', index=False)