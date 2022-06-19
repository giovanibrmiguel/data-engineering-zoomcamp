{{ config(materialized='table') }}

SELECT
    CAST(locationid AS integer) AS location_id,
    borough,
    zone,
    replace(service_zone,'Boro','Green') as service_zone
FROM
    {{ source('staging','taxi_zones') }}