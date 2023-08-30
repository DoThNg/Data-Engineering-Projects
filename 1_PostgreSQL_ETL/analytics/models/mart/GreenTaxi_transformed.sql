WITH greentaxi_tbl AS (
    
    SELECT     
        trip_id,
        vendor_id,
	CASE
	    WHEN trip_type = 1 THEN 'Dispatch'  
	    WHEN trip_type = 2 THEN 'Street-hail'
	    ELSE 'NA'
	END AS trip_type_des,
	CASE
	    WHEN payment_type = 1 THEN 'Credit card'
	    WHEN payment_type = 2 THEN 'Cash'
	    WHEN payment_type = 3 THEN 'No charge'
	    WHEN payment_type = 4 THEN 'Dispute'
	    WHEN payment_type = 5 THEN 'Unknown'
	    WHEN payment_type = 6 THEN 'Voided trip'
	    ELSE 'NA'
	END AS payment_type_des,
	CASE
	    WHEN ratecode_id = 1 THEN 'Standard rate'
	    WHEN ratecode_id = 2 THEN 'JFK'
	    WHEN ratecode_id = 3 THEN 'Newark'
	    WHEN ratecode_id = 4 THEN 'Nassau or Westchester'
	    WHEN ratecode_id = 5 THEN 'Negotiated fare'
	    WHEN ratecode_id = 6 THEN 'Group ride'
	    ELSE 'NA'
	END AS ratecode_id_des,
        pu_loc_id, 
        do_loc_id,
        store_and_fwd_flag, 
        lpep_pickup_datetime, 
        lpep_dropoff_datetime,
        passenger_count, 
        trip_distance, 
        fare_amount, 
        extra, 
        mta_tax,
        tip_amount, 
        tolls_amount, 
        ehail_fee, 
        improvement_surcharge,
        total_amount, 
        congestion_surcharge
    
    FROM {{ ref('stg_greentaxi') }}
)

SELECT 
    trip_id,
    vendor_id,
    trip_type_des,
    payment_type_des,
    ratecode_id_des, 
    pu_loc_id, 
    do_loc_id,
    lpep_pickup_datetime, 
    lpep_dropoff_datetime,
    passenger_count, 
    trip_distance, 
    fare_amount, 
    extra, 
    mta_tax,
    tip_amount, 
    tolls_amount, 
    ehail_fee, 
    improvement_surcharge,
    total_amount, 
    congestion_surcharge

FROM greentaxi_tbl
