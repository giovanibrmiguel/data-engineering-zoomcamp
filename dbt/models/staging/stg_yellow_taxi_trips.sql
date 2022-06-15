{{ config(materialized='view') }}

WITH yellow_taxi_trips AS (
    SELECT
        *,
        row_number() over(partition by vendorid, tpep_pickup_datetime) AS rn
    FROM
        {{ source('staging','yellow_taxi_trips') }}
    WHERE
        vendorid IS NOT NULL
)
SELECT
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} AS trip_id,
    cast(vendorid as integer) AS vendor_id,
    cast(ratecodeid as integer) AS rate_code_id,
    cast(pulocationid as integer) AS  pickup_location_id,
    cast(dolocationid as integer) AS dropoff_location_id,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_ts
FROM
    yellow_taxi_trips
WHERE
    rn = 1
-- dbt build --m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}