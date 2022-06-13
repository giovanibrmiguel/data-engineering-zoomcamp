import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging
from airflow.utils.dates import days_ago
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
OUTPUT_FILE = 'output_taxi_zones.csv'
OUTPUT_PARQUET_FILE = 'output_taxi_zones.parquet'
PARQUET_FILE = OUTPUT_FILE.replace('.csv', '.parquet')
TABLE_NAME_TEMPLATE = 'taxi_zones'

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

local_workflow = DAG(
    dag_id="taxi_zones_ingestion_gcs_dag",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 2, 1),
    max_active_runs=1,
    tags=['dtc-de'],
    catchup = False
)

with local_workflow:
    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'curl -sSL {URL} > {AIRFLOW_HOME}/{OUTPUT_FILE}'
    )
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE}",
        },
    )
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_PARQUET_FILE}",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_PARQUET_FILE}",
        },
    )

    download_dataset_task >> local_to_gcs_task