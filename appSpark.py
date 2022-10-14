#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    
    #connect db warehouse
    conn_dwh, engine_dwh  = connection.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="etl",config=conf)

    path_query = os.getcwd()+'/query/'

    query = sqlparse.format(
        open(
            path_query+'sql_query.sql','r'
            ).read(), strip_comments=True).strip()

    query2 = sqlparse.format(
        open(
            path_query+'sql_query2.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Starting .....")
        
        df = pd.read_sql(query, engine_dwh)
        path = os.getcwd()
        directory = path+'/'+'local'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_csv(f"{directory}transaction_{filetime}.csv", index=False)

        print(f"[INFO] Upload Data in LOCAL Success .....")

        #spark processing
        sparkDF = spark.createDataFrame(df)
        sparkDF.createOrReplaceTempView("TempView")
        
        print(f"Running First Spark Job")
        sqlDF = spark.sql("SELECT customer_country, COUNT(transaction_amount) AS transaction_amount \
                            FROM TempView \
                            GROUP BY customer_country \
                            ORDER BY transaction_amount DESC")
        sqlDF.toPandas().to_csv(f'/mnt/e/dataengineer/project/finalproject/data/processed_data/TransactionByCountry.csv', index=False)
        print(f"First Spark Job Completed")

        print(f"Running Second Spark Job")
        sqlDF = spark.sql("SELECT transaction_product, SUM(transaction_amount) AS pcs_sold \
                            FROM TempView \
                            GROUP BY transaction_product \
                            HAVING pcs_sold \
                            ORDER BY pcs_sold DESC")
        sqlDF.toPandas().to_csv(f'/mnt/e/dataengineer/project/finalproject/data/processed_data/CustomerAgeGender.csv', index=False)
        print(f"Second Spark Job Completed")

        print(f"Running Third Spark Job")
        sqlDF = spark.sql("SELECT customer_gender, YEAR(CURRENT_DATE())-YEAR(customer_birthdate) AS age \
                            FROM TempView")
        sqlDF.toPandas().to_csv(f'/mnt/e/dataengineer/project/finalproject/data/processed_data/AgeAndGender.csv', index=False)
        print(f"Third Spark Job Completed")
        
        df2 = pd.read_sql(query2, engine_dwh)

        sparkDF2 = spark.createDataFrame(df2)
        sparkDF2.createOrReplaceTempView("TempVieww")

        print(f"Running Fourth Spark Job")
        sqlDF2 = spark.sql("SELECT search_product AS product, COUNT(search_id) AS search \
                            FROM TempVieww \
                            GROUP BY product \
                            ORDER BY search DESC \
                            LIMIT 3")
        sqlDF2.toPandas().to_csv(f'/mnt/e/dataengineer/project/finalproject/data/processed_data/TopProductSearch.csv', index=False)
        print(f"Fourth Spark Job Completed")

        print(f"Running Fifth Spark Job")
        sqlDF2 = spark.sql("SELECT EXTRACT(year FROM search_date) AS year, COUNT(search_id) AS totalSearch \
                            FROM TempVieww \
                            GROUP BY year \
                            ORDER BY year ASC")
        sqlDF2.toPandas().to_csv(f'/mnt/e/dataengineer/project/finalproject/data/processed_data/SearchAnnual.csv', index=False)
        print(f"Fifth Spark Job Completed")

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
