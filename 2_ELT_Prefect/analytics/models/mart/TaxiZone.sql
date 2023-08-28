WITH taxi_zone_tbl AS (

    SELECT     
        "LocationID" AS location_id,
        "Borough" AS borough,
        "Zone" AS zone_loc,
        "service_zone"  
    FROM {{ ref('taxi_zone_lookup') }}    
)

SELECT * FROM taxi_zone_tbl