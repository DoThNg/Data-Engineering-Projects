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
                              lpep_pickup_datetime timestamp, 
                              lpep_dropoff_datetime timestamp,
                              rate_code_des varchar(25), 
                              passenger_count integer, 
                              trip_distance float, 
                              fare_amount float, 
                              extra float, 
                              mta_tax float,
                              tip_amount float, 
                              tolls_amount float, 
                              improvement_surcharge float,
                              total_amount float, 
                              pmt_type_des varchar(15), 
                              trip_type varchar(12), 
                              congestion_surcharge float,
                              pu_hour integer,
                              do_hour integer,
                              travel_day varchar(10),
                              fee_per_mile float);
                            """)

# 2. INSERT RECORDS
insert_trip_record_sql = ("""
                          INSERT INTO greentaxi (lpep_pickup_datetime, lpep_dropoff_datetime, rate_code_des, 
                                                passenger_count, trip_distance, fare_amount, extra, mta_tax,
                                                tip_amount, tolls_amount, improvement_surcharge,
                                                total_amount, pmt_type_des, trip_type, congestion_surcharge, 
                                                pu_hour, do_hour, travel_day, fee_per_mile)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                         """)
