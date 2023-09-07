if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from dotenv import load_dotenv
import glob
import psycopg2
import psycopg2.extras
import pandas as pd
import os
import math
from sql import yellowTaxi_drop_table_sql, yellowTaxi_create_table_sql, extension_uuid_sql, insert_trip_record_sql

load_dotenv()

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
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

        # Create table 'yellowtaxi'
        cur.execute(extension_uuid_sql)
        cur.execute(yellowTaxi_drop_table_sql)
        cur.execute(yellowTaxi_create_table_sql)

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
