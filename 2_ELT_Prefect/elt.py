from dotenv import load_dotenv
import glob
import psycopg2
import psycopg2.extras
import pandas as pd
import os
from sql import greenTaxi_drop_table_sql, greenTaxi_create_table_sql, extension_uuid_sql, insert_trip_record_sql
from prefect import flow, task
from prefect_dbt.cli.commands import DbtCoreOperation

load_dotenv()

DATA_DIR = "./dataset"

@task
# Run initial setup.
# Set up a local PostgreSQL Database.
def db_connect(name = "db connection"):
    # Set connection to the newly created database 'TaxiDB'
    try:
        # Set up connect to database
        conn = psycopg2.connect(database=os.getenv('DB_NAME'),
                                user=os.getenv('DB_USER'),
                                password=os.getenv('DB_PASS'),
                                host=os.getenv('DB_HOST'),
                                port=os.getenv('DB_PORT'))
        
        conn.autocommit = True

        print("Database connected successfully")

        # Create a cursor
        cur = conn.cursor()

        # Create table 'greentaxi'
        cur.execute(extension_uuid_sql)
        cur.execute(greenTaxi_drop_table_sql)
        cur.execute(greenTaxi_create_table_sql)

        conn.commit()
        cur.close()

        print("A new data table created successfully")

    except Exception as error:

        print(error)
    
    finally:    
        # Close database connection
        if conn is not None:
            conn.close()

@task
# Run 'data loading' task
def elt(name = "load data"):
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
            
            # Insert all records at a time
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

@flow
# Create ELT workflow
def run_etl_workflow(name = "elt workflow'"):
    # Connect to database
    db_connect()

    # Load files to PostgreSQL database
    elt()

    #Transform data with dbt
    dbt_workflow = DbtCoreOperation(
        commands=["cd analytics", "dbt debug", "dbt seed", "dbt run"],
        project_dir="./analytics",
        profiles_dir="./analytics"
    ).run()
    return dbt_workflow

# Run entire workflow
run_etl_workflow()
