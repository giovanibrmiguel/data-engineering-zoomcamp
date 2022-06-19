{{ config(materialized='table') }}

SELECT
    trip_id,
    hvfhs_license_code,
    dispatching_base_code,
    originating_base_code,
    pickup_location_id,
    dropoff_location_id,
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag,
    wav_match_flag,
    trip_distance,
    trip_time,
    base_passenger_fare,
    tolls,
    bcf,
    sales_tax,
    congestion_surcharge,
    driver_pay,
    request_ts,
    on_scene_ts,
    pickup_ts,
    dropoff_ts
FROM
    {{ ref('stg_fhv_trips') }}