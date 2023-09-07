# 1. CREATE GREEN-TAXI TABLE
extension_uuid_sql = ("""
                    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                      """)

yellowTaxi_drop_table_sql = ("""
                            DROP TABLE IF EXISTS yellowtaxi;  
                            """)

yellowTaxi_create_table_sql = ("""
                               CREATE TABLE yellowtaxi
                                (trip_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
                                tpep_pickup_datetime timestamp,
                                tpep_dropoff_datetime timestamp,
                                passenger_count integer, 
                                trip_distance float, 
                                rate_code_des varchar(25), 
                                pmt_type_des varchar(15),
                                fare_amount float, 
                                extra float, 
                                mta_tax float,
                                tip_amount float,
                                tolls_amount float,  
                                improvement_surcharge float,
                                total_amount float,  
                                congestion_surcharge float,
                                airport_fee float,
                                pu_hour integer,
                                do_hour integer,
                                travel_day varchar(10),
                                fee_per_mile float);
                               """)

# 2. INSERT RECORDS
insert_trip_record_sql = ("""

                INSERT INTO yellowtaxi (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, rate_code_des, 
                                        pmt_type_des, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                                        total_amount, congestion_surcharge, airport_fee, pu_hour, do_hour, travel_day, fee_per_mile)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                           
                """)
