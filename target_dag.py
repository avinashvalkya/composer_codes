from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 3, 27)
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
        bash_command='echo "{{ dag_run.conf["my_param"] }}" "{{ dag_run.conf["my_param2"] }}"',
    )

    cleaning = PythonOperator(
        task_id='cleaning',
        python_callable=_cleaning
    )

    storing >> cleaning