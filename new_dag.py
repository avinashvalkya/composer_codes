from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from google.cloud import bigquery

bq_client=bigquery.Client()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 20),
}

dag = DAG(
    'my_bq_dag',
    default_args=default_args,
    schedule_interval=None,
)

def query_and_store_results(ds, **kwargs):
    bq_hook = BigQueryHook()
    query = """
    SELECT INTF_SYS_NAME FROM golden-imprint-374915.gcp_dataeng_demos.SYS_INTF_METADATA
    """
    results = bq_hook.get_pandas_df(sql=query, dialect="standard")
    kwargs['ti'].xcom_push(key='results', value=results.to_dict())

store_results_task = PythonOperator(
    task_id='store_results',
    provide_context=True,
    python_callable=query_and_store_results,
    dag=dag
)

retrieve_results_task = BashOperator(
    task_id='retrieve_results',
    bash_command='echo {{ ti.xcom_pull(task_ids="store_results", key="results") }}',
    dag=dag
)

store_results_task >> retrieve_results_task
