import json
import os
from re import T
import connection

import pandas as pd
import numpy as np
import sqlparse
import psycopg2

if __name__ == "__main__":

    #connect db warehouse
    conn_dwh, engine_dwh  = connection.conn()
    cursor_dwh = conn_dwh.cursor()

    path_query = os.getcwd()+'/query/'

    ddl_query = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    dml_query = sqlparse.format(
        open(
            path_query+'data.sql','r'
            ).read(), strip_comments=True).strip()
    
    try:
        cursor_dwh.execute(ddl_query)
        print("Success Create DWH Schema")

    except (Exception, psycopg2.Error) as error:
        print("Failed to Execute Code", error)

    try:
        cursor_dwh.execute(dml_query)
        print("Success Insert Data to DWH")

    except (Exception, psycopg2.Error) as error:
        print("Failed to Execute Code", error)