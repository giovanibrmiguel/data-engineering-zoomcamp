import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

CREATE_BQ_TBL_QUERY_YELLOW_TAXI = (
    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.yellow_taxi_trips \
            ( \
            VendorID INT64, \
            tpep_pickup_datetime TIMESTAMP, \
            tpep_dropoff_datetime TIMESTAMP, \
            passenger_count FLOAT64, \
            trip_distance FLOAT64, \
            RatecodeID FLOAT64, \
            store_and_fwd_flag STRING, \
            PULocationID INT64, \
            DOLocationID INT64, \
            payment_type INT64, \
            fare_amount FLOAT64, \
            extra FLOAT64, \
            mta_tax FLOAT64, \
            tip_amount FLOAT64, \
            tolls_amount FLOAT64, \
            improvement_surcharge FLOAT64, \
            total_amount FLOAT64, \
            congestion_surcharge FLOAT64, \
            airport_fee INT64 \
            ) \
    PARTITION BY DATE(tpep_pickup_datetime) \
    OPTIONS( \
    partition_expiration_days=10000 \
    ) \
    AS ( \
        SELECT  \
            * \
        REPLACE(NULL AS airport_fee) \
        FROM  \
            {BIGQUERY_DATASET}.yellow_taxi_tripdata_external_table \
    );" \
)

CREATE_BQ_TBL_QUERY_GREEN_TAXI = (
    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.green_taxi_trips \
            ( \
            VendorID INT64, \
            lpep_pickup_datetime TIMESTAMP, \
            lpep_dropoff_datetime TIMESTAMP, \
            store_and_fwd_flag STRING, \
            RatecodeID FLOAT64, \
            PULocationID INT64, \
            DOLocationID INT64, \
            passenger_count FLOAT64, \
            trip_distance FLOAT64, \
            fare_amount FLOAT64, \
            extra FLOAT64, \
            mta_tax FLOAT64, \
            tip_amount FLOAT64, \
            tolls_amount FLOAT64, \
            ehail_fee FLOAT64, \
            improvement_surcharge FLOAT64, \
            total_amount FLOAT64, \
            payment_type FLOAT64, \
            trip_type FLOAT64, \
            congestion_surcharge FLOAT64 \
            ) \
    PARTITION BY DATE(lpep_pickup_datetime) \
    OPTIONS( \
    partition_expiration_days=10000 \
    ) \
    AS ( \
        SELECT  \
            * \
        REPLACE(NULL AS ehail_fee) \
        FROM  \
            {BIGQUERY_DATASET}.green_taxi_tripdata_external_table \
    );" \
)

CREATE_BQ_TBL_QUERY_TAXI_ZONES = (
    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.taxi_zones \
    AS ( \
        SELECT  \
            * \
        FROM  \
            {BIGQUERY_DATASET}.taxi_zones_tripdata_external_table \
    );" \
)

CREATE_BQ_TBL_QUERY_FHVHV = (
    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.fhv_trips \
            ( \
            hvfhs_license_num STRING, \
            dispatching_base_num STRING, \
            originating_base_num STRING, \
            request_datetime TIMESTAMP, \
            on_scene_datetime TIMESTAMP, \
            pickup_datetime TIMESTAMP, \
            dropoff_datetime TIMESTAMP, \
            PULocationID INT64, \
            DOLocationID INT64, \
            trip_miles FLOAT64, \
            trip_time INT64, \
            base_passenger_fare FLOAT64, \
            tolls FLOAT64, \
            bcf FLOAT64, \
            sales_tax FLOAT64, \
            congestion_surcharge FLOAT64, \
            airport_fee INT64, \
            tips FLOAT64, \
            driver_pay FLOAT64, \
            shared_request_flag STRING, \
            shared_match_flag STRING, \
            access_a_ride_flag STRING, \
            wav_request_flag STRING, \
            wav_match_flag STRING \
            ) \
    PARTITION BY DATE(pickup_datetime) \
    OPTIONS( \
    partition_expiration_days=10000 \
    ) \
    AS ( \
        SELECT  \
            * \
        REPLACE(NULL AS airport_fee) \
        FROM  \
            {BIGQUERY_DATASET}.fhvhv_tripdata_external_table \
    );" \
)

DATASET = "tripdata"
DATASET_PREFIX = "output"
VEHICLE_RANGE = {'yellow_taxi': CREATE_BQ_TBL_QUERY_YELLOW_TAXI, 'green_taxi': CREATE_BQ_TBL_QUERY_GREEN_TAXI,  'fhvhv': CREATE_BQ_TBL_QUERY_FHVHV, 'taxi_zones': CREATE_BQ_TBL_QUERY_TAXI_ZONES}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for vehicle, query in VEHICLE_RANGE.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{vehicle}_files_task',
            source_bucket=BUCKET,
            source_object=f'{INPUT_PART}/{DATASET_PREFIX}_{vehicle}_*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{vehicle}/{vehicle}_{DATASET}',
            move_object=True
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{vehicle}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{vehicle}_{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{vehicle}/*"],
                },
            },
        )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{vehicle}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            }
        )

        move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job