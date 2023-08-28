WITH greentaxi_src AS (

    SELECT * FROM {{ source('taxidb', 'greentaxi') }}

)

SELECT * FROM greentaxi_src