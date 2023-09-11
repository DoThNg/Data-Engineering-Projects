from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import pandas as pd
from pandas import DataFrame
import os
import math
from etl_project.sql import greenTaxi_drop_table_sql, greenTaxi_create_table_sql, extension_uuid_sql, insert_trip_record_sql

load_dotenv()

def load_taxi_data(data: DataFrame):
    """
    Load data to some source.

    Args:
        data: The output from the upstream task (data transformation)
    """

    conn = None
    n_batch = 5

    try:
        # Set up connect to database
        conn = psycopg2.connect(database=os.getenv('DB_NAME'),
                                user=os.getenv('DB_USER'),
                                password=os.getenv('DB_PASS'),
                                host=os.getenv('DB_HOST'),
                                port=os.getenv('DB_PORT'))
        
        conn.autocommit = True

        # Create a cursor
        cur = conn.cursor()

        # Create table 'greentaxi'
        cur.execute(extension_uuid_sql)
        cur.execute(greenTaxi_drop_table_sql)
        cur.execute(greenTaxi_create_table_sql)

        conn.commit()
        cur.close()

    except Exception as error:

        print(error)
    
    finally:    
        # Close database connection
        if conn is not None:
            conn.close()

    # Load data
    try:
        # Set up connect to database
        conn = psycopg2.connect(database=os.getenv('DB_NAME'),
                                user=os.getenv('DB_USER'),
                                password=os.getenv('DB_PASS'),
                                host=os.getenv('DB_HOST'),
                                port=os.getenv('DB_PORT'))
        
        print("Database connected successfully")

        # Create a cursor
        cur = conn.cursor()

        print(len(data))

        records = list(data[list(data.columns)].values)
        
        len_record_list = len(records)
        records_per_batch = math.ceil(len(records) / n_batch)
    
        for n in range(n_batch):
            start_batch = int(records_per_batch * n)
            end_batch = int(records_per_batch * (n + 1))
            
            if n == 0:
                batch_records = records[start_batch:end_batch]        
                psycopg2.extras.execute_batch(cur, insert_trip_record_sql, batch_records)
            elif n == 4:
                batch_records = records[(start_batch + 1):len_record_list]        
                psycopg2.extras.execute_batch(cur, insert_trip_record_sql, batch_records)                
            else:
                batch_records = records[(start_batch + 1):end_batch]        
                psycopg2.extras.execute_batch(cur, insert_trip_record_sql, batch_records)

            conn.commit()

        cur.close()

        print("Records inserted successfully")

    except Exception as error:

        print(error)

    finally:    
        # Close communication with the database
        if conn is not None:
            conn.close()
