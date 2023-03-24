from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bigquery_branch_operator',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def query_bigquery_table():
    bq_hook = BigQueryHook()
    query = """
        SELECT INGEST_PTRN,INTF_NAME
        FROM golden-imprint-374915.gcp_dataeng_demos.SYS_INTF_METADATA
    """
    results = bq_hook.get_pandas_df(sql=query, dialect="standard")
    tasks = []
    for index, row in results.iterrows():
        INTF_NAME = row['INTF_NAME']
        INGEST_PTRN = row['INGEST_PTRN']
        
        if INGEST_PTRN == 'FILE-FILE':
            task_id = f'process_{INGEST_PTRN}'
            tasks.append(task_id)
            globals()[task_id] = BashOperator(
                task_id=task_id,
                bash_command='echo FILE',
                dag=dag
            )
        else:
            task_id = f'process_{INGEST_PTRN}'
            tasks.append(task_id)
            globals()[task_id] = BashOperator(
                task_id=task_id,
                bash_command='echo RELATIONALDB',
                dag=dag
            )
    return tasks

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=query_bigquery_table,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

for task_id in query_bigquery_table():
    globals()[task_id] >> end_task

branch_task >> globals()[query_bigquery_table()[0]]
