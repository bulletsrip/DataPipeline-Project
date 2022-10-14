#!python3 
import pandas as pd
import numpy as np

from sqlalchemy import create_engine

if __name__ == "__main__":
    
#EXTRACT    
    try:
        conn = create_engine('postgresql://postgres:123456@localhost:5432/data_sources')
        print(f"Successfully Connect to PostgreSQL")
    except:
        print(f"Failed Connect to PostgreSQL")

    list_filename = ["customer", "product", "transaction"]
    for file in list_filename:
        pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=conn, if_exists='replace')
        print(f"Successfully Insert {file} File to Data Sources")