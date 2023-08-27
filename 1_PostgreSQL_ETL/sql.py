# List of tables to create

# 1. CREATE GREEN-TAXI TABLE
extension_uuid_sql = ("""
                    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                  """)

greenTaxi_drop_table_sql = ("""
                            DROP TABLE IF EXISTS greentaxi;
                        
                        """)

greenTaxi_create_table_sql = ("""   
                          CREATE TABLE greentaxi
                                (trip_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
                                vendor_id float, 
                                lpep_pickup_datetime timestamp, 
                                lpep_dropoff_datetime timestamp,
                                store_and_fwd_flag text, 
                                ratecode_id float, 
                                pu_loc_id float, 
                                do_loc_id float,
                                passenger_count float, 
                                trip_distance float, 
                                fare_amount float, 
                                extra float, 
                                mta_tax float,
                                tip_amount float, 
                                tolls_amount float, 
                                ehail_fee float, 
                                improvement_surcharge float,
                                total_amount float, 
                                payment_type float, 
                                trip_type float, 
                                congestion_surcharge float);
                        """)

# 2. INSERT RECORDS

insert_trip_record_sql = ("""

                INSERT INTO greentaxi (vendor_id, lpep_pickup_datetime, lpep_dropoff_datetime,
                                        store_and_fwd_flag, ratecode_id, pu_loc_id, do_loc_id,
                                        passenger_count, trip_distance, fare_amount, extra, mta_tax,
                                        tip_amount, tolls_amount, ehail_fee, improvement_surcharge,
                                        total_amount, payment_type, trip_type, congestion_surcharge)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                           
                """)
