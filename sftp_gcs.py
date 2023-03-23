from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pysftp
from google.cloud import storage

def transfer_sftp_to_gcp():
    # Connect to SFTP server and retrieve file
    sftp = pysftp.Connection(host='sftp.example.com', username='your_username', password='your_password')
    sftp.get('/path/to/your/file.csv', '/tmp/file.csv')
    sftp.close()

    # Upload file to GCP bucket
    client = storage.Client()
    bucket = client.bucket('your_gcp_bucket_name')
    blob = bucket.blob('file.csv')
    blob.upload_from_filename('/tmp/file.csv')

# Define DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 23),
    'retries': 0
}

dag = DAG('sftp_to_gcp', default_args=default_args, schedule_interval=None)

# Define PythonOperator
transfer_operator = PythonOperator(
    task_id='transfer_sftp_to_gcp',
    python_callable=transfer_sftp_to_gcp,
    dag=dag)

# Set dependencies
transfer_operator

