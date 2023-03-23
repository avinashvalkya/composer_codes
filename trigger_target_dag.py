from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 3, 23)
}

def _downloading():
    print('downloading')

with DAG('trigger_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    downloading = PythonOperator(
        task_id='downloading',
        python_callable=_downloading
    )

    # trigger_dag.py

    trigger_target = TriggerDagRunOperator(
            task_id='trigger_target',
            trigger_dag_id='target_dag',
            execution_date='{{ ds }}',
            conf={"my_param": "some_value"},
            reset_dag_run=True,
            wait_for_completion=True
        )

default_args = {
    'start_date': datetime(2023, 3, 23)
}

def _cleaning():
    print('Clearning from target DAG')

with DAG('target_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    storing = BashOperator(
        task_id='storing',
        # bash_command='sleep 10'
        bash_command='echo "{{ dag_run.conf["my_param"] }}"',
    )

    cleaning = PythonOperator(
        task_id='cleaning',
        python_callable=_cleaning
    )

    storing >> cleaning