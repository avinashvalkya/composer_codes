from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'start_date': datetime(2023, 3, 28)
}

dag = DAG(
    'dynamic_nested_branching_task_creation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

bq_hook = BigQueryHook()

# Define the BigQuery query to retrieve the tasks
bq_query = """
SELECT INTF_NAME, folder_path, file_name
FROM regal-stage-381903.gcp_dataeng_demos.SYS_INTF_METADATA
"""

# Execute the BigQuery query and retrieve the results
results = bq_hook.get_pandas_df(sql=bq_query, dialect="standard")

# Create a dummy task to serve as the starting point of the branching tasks
start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

# Create dictionaries to store the tasks and branches
tasks = {}
branches_interface = {}
branches_folder_path = {}

# Define the Python callable to create the file_name tasks
# def create_file_name_task(ti, **kwargs):
#     file_name = ti.xcom_pull(task_ids='file_name_task', key='file_name')
#     task_name = f'process_{file_name}'
#     bash_command = f'echo {task_name}'
#     task = BashOperator(
#         task_id=task_name,
#         bash_command=bash_command,
#         dag=dag
#     )
#     tasks[task_name] = task
#     return task_name

def create_file_name_task(file_name):
    # file_name = ti.xcom_pull(task_ids='file_name_task', key='file_name')
    # task_name = f'process_{file_name}'
    # bash_command = f'echo {task_name}'
    print(f'The value of my_param is: {file_name}')

    # task = BashOperator(
    #     task_id=task_name,
    #     bash_command=bash_command,
    #     dag=dag
    # )
    # tasks[task_name] = task

    return file_name

# Loop through the results and create the interface branches
for interface in results['INTF_NAME'].unique():
    branch_interface_id = f'branch_interface_{interface}'
    branch_interface = DummyOperator(
        task_id=branch_interface_id,
        dag=dag
    )
    branches_interface[interface] = branch_interface
    start_task >> branch_interface

    # Loop through the results for this interface and create the folder_path branches
    for folder_path in results[results['INTF_NAME']==interface]['folder_path'].unique():
        branch_folder_path_id = f'branch_folder_path_{interface}_{folder_path}'
        branch_folder_path = DummyOperator(
            task_id=branch_folder_path_id,
            dag=dag
        )
        branches_folder_path[(interface, folder_path)] = branch_folder_path
        branch_interface >> branch_folder_path

        # Loop through the results for this interface and folder_path and create the file_name tasks
        for index, row in results[(results['INTF_NAME']==interface) & (results['folder_path']==folder_path)].iterrows():
            file_name = row['file_name']
            file_name_task_id = f'file_name_task_{interface}_{folder_path}_{file_name}'
            file_name_task = PythonOperator(
                task_id=file_name_task_id,
                provide_context=True,
                python_callable=create_file_name_task,
                op_kwargs={'file_name': file_name},
                dag=dag
            )
            tasks[file_name_task_id] = file_name_task
            branch_folder_path >> file_name_task

# Define the final task to end the DAG
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "DAG completed."',
    dag=dag
)

# Set the final task dependency on all the dynamic tasks created
for task_name in tasks:
    tasks[task_name] >> end_task

if __name__ == '__main__':
    dag.cli()
