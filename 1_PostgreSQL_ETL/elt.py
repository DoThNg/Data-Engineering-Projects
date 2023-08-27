from dotenv import load_dotenv
from sql import insert_trip_record_sql
import glob
import psycopg2
import psycopg2.extras
import pandas as pd
import os

load_dotenv()

DATA_DIR = "./dataset"

# Run etl.py after running setup_tbl.py
def elt():
    # Create database connection
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

        # Insert records
        data_files = glob.glob(os.path.join(DATA_DIR, '*.parquet'))

        for datafile in data_files:
            datafile_path = os.path.abspath(datafile)
            read_file = pd.read_parquet(datafile_path, engine='pyarrow')
            
            # Option 1: Insert each record (row by row)
            # for row in list(read_file[list(read_file.columns)].values):
            #     cur.execute(insert_trip_record_sql, row)

            # Option 2: Insert all records at a time
            records = list(read_file[list(read_file.columns)].values)
            psycopg2.extras.execute_batch(cur, insert_trip_record_sql, records)

        conn.commit()
        cur.close()

        print("Records inserted successfully")

    except Exception as error:

        print(error)

    finally:    
        # Close communication with the database
        if conn is not None:
            conn.close()

if __name__ == "main":
    elt()

