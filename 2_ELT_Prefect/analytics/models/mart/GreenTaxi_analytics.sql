WITH greentaxi_transformed_cte AS (
    
    SELECT * FROM {{ ref('GreenTaxi_transformed') }}

),

taxizone_cte AS (

    SELECT * FROM {{ ref('TaxiZone') }}
),

greentaxi_cte AS (
    SELECT 
        trip_id,
        vendor_id,
        trip_type_des,
        payment_type_des,
        ratecode_id_des, 
        taxizone_cte.borough AS pu_borough,
        taxizone_cte.zone_loc AS pu_zone_loc,
        taxizone_cte.service_zone AS pu_service_zone, 
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

FROM greentaxi_transformed_cte
LEFT JOIN taxizone_cte ON greentaxi_transformed_cte.pu_loc_id = taxizone_cte.location_id
)

SELECT
        trip_id,
        vendor_id,
        trip_type_des,
        payment_type_des,
        ratecode_id_des, 
        pu_borough,
        pu_zone_loc,
        pu_service_zone, 
        taxizone_cte.borough AS do_borough,
        taxizone_cte.zone_loc AS do_zone_loc,
        taxizone_cte.service_zone AS do_service_zone,
        trip_distance, 
        fare_amount, 
        extra, 
        mta_tax,
        tip_amount, 
        tolls_amount, 
        improvement_surcharge,
        total_amount, 
        congestion_surcharge,
        lpep_pickup_datetime, 
        lpep_dropoff_datetime

FROM greentaxi_cte
LEFT JOIN taxizone_cte ON greentaxi_cte.do_loc_id = taxizone_cte.location_id