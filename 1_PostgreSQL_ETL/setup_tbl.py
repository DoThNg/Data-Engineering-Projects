from dotenv import load_dotenv
import psycopg2
import os
from sql import greenTaxi_drop_table_sql, greenTaxi_create_table_sql, extension_uuid_sql

load_dotenv()
conn = None

# Set up a local PostgreSQL Database before running setup_tbl.py.

def db_connect():
    # Set connection to the newly created database 'Taxi-DB'
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

if __name__ == "__main__":
    db_connect()




