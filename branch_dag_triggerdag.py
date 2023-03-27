from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'start_date': datetime(2023, 3, 27)
}

dag = DAG(
    'dynamic_branching_task_creation_new',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

bq_hook = BigQueryHook()

# Define the BigQuery query to retrieve the tasks
bq_query = """
SELECT INTF_NAME, INGEST_PTRN
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
branches = {}

# Loop through the results and create a BashOperator for each task
for index, row in results.iterrows():
    # task_name = row['task_name']
    task_name=row['INTF_NAME']
    # bash_command = row['bash_command']
    bash_command = f'echo process_{task_name}'
    # branch_name = row['branch_name']
    branch_name=row['INGEST_PTRN']

    # Check if the branch already exists, and create it if it doesn't
    if branch_name not in branches:
        branch = DummyOperator(
            task_id=branch_name,
            dag=dag
        )
        branches[branch_name] = branch

        # Set the branch dependency on the start task
        start_task >> branch
    if(branch_name=='FILE-FILE'):
        bash_command=bash_command+" FILE-FILE"
    elif(branch_name=='FILE-RELATIONALDB'):
        bash_command=bash_command+" FILE-RELATIONALDB"    
    # Create the BashOperator for the task
    # task = BashOperator(
    #     task_id=task_name,
    #     bash_command=bash_command,
    #     dag=dag
    # )
    task = TriggerDagRunOperator(
            task_id=task_name,
            trigger_dag_id='target_dag',
            execution_date='{{ ds }}',
            conf={"my_param": "some_value","my_param2": "some_value2","my_param3": "some_value3"},
            reset_dag_run=True,
            wait_for_completion=True,
            dag=dag
        )    
    tasks[task_name] = task

    # Set the task dependency on the appropriate branch
    branches[branch_name] >> task

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