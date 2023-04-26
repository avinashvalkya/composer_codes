from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 26),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

input_files = ['/path/to/file1', '/path/to/file2', '/path/to/file3']
dag = DAG('file_processing', default_args=default_args, schedule_interval=None)

start_task = DummyOperator(task_id='start', dag=dag)

def check_file_empty(file_path):
    return 'error' if os.stat(file_path).st_size == 0 else 'print_file'

for file_path in input_files:
    task_name = f'process_file_{file_path}'
    
    # Create a BranchPythonOperator to check if file is empty
    check_file_task = BranchPythonOperator(
        task_id=f'{task_name}_check_file',
        python_callable=check_file_empty,
        op_kwargs={'file_path': file_path},
        dag=dag
    )
    
    # Create a branch for success or error
    branch_task = DummyOperator(task_id=f'{task_name}_branch', dag=dag)
    success_task = BashOperator(task_id=f'{task_name}_success', bash_command=f'cat {file_path}', dag=dag)
    error_task = DummyOperator(task_id=f'{task_name}_error', bash_command='echo "Error: File is empty"', dag=dag)

    # Set the dependencies between the tasks
    start_task >> check_file_task >> branch_task
    branch_task >> success_task
    branch_task >> error_task

    # Add the task to the list of dynamic tasks
    dynamic_tasks.append(check_file_task)

