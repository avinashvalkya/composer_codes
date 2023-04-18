from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def copy_file_to_folder():
    source_bucket_name = 'source-bucket-name'
    source_folder_path = 'source-folder-path/'
    source_file_name = 'file-to-copy.txt'
    destination_bucket_name = 'destination-bucket-name'
    destination_folder_path = 'destination-folder-path/'

    # Instantiate a client
    storage_client = storage.Client()

    # Get the source bucket
    source_bucket = storage_client.bucket(source_bucket_name)

    # Get the source blob
    source_blob = source_bucket.blob(source_folder_path + source_file_name)

    # Get the destination bucket
    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Copy the source blob to the destination bucket and folder
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_folder_path + source_file_name
    )

    print(f"File {source_file_name} copied from {source_bucket_name}/{source_folder_path} to {destination_bucket_name}/{destination_folder_path}")

with DAG('copy_file_dag', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    copy_file_task = PythonOperator(
        task_id='copy_file_task',
        python_callable=copy_file_to_folder,
        dag=dag
    )
