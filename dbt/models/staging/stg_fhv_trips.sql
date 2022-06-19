{{ config(materialized='view') }}

SELECT
   -- identifiers
    {{ dbt_utils.surrogate_key(['hvfhs_license_num', 'dispatching_base_num', 'pickup_datetime']) }} AS trip_id,
    hvfhs_license_num as hvfhs_license_code,
    dispatching_base_num as dispatching_base_code,
    originating_base_num as originating_base_code,
    cast(pulocationid as integer) AS  pickup_location_id,
    cast(dolocationid as integer) AS dropoff_location_id,
    
    -- trip info
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag,
    wav_match_flag,
    cast(trip_miles as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    cast(trip_time as numeric) as trip_time,
    
    -- payment info
    cast(base_passenger_fare as numeric) as base_passenger_fare,
    cast(tolls as numeric) as tolls,
    cast(bcf as numeric) as bcf,
    cast(sales_tax as numeric) as sales_tax,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    cast(driver_pay as numeric) as driver_pay,

    -- timestamps
    cast(request_datetime as timestamp) as request_ts,
    cast(on_scene_datetime as timestamp) as on_scene_ts,
    cast(pickup_datetime as timestamp) as pickup_ts,
    cast(dropoff_datetime as timestamp) as dropoff_ts
FROM
    {{ source('staging','fhv_trips') }}