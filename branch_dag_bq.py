from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dynamic_branching_task_creation_new',
    default_args=default_args,
    schedule_interval='@once'
)

bq_hook = BigQueryHook()

# Define the BigQuery query to retrieve the tasks
bq_query = """
select JOB_TRIGGER_METADATA.INGEST_PTRN,JOB_TRIGGER_METADATA.DATA_FLOW_DRCTN,JOB_TRIGGER_METADATA.INTF_NAME from
(select * from golden-imprint-374915.gcp_dataeng_demos.JOB_TRIGGER_METADATA where TRIGGER_STATUS='I') JOB_TRIGGER_METADATA,
golden-imprint-374915.gcp_dataeng_demos.SYS_INTF_METADATA SYS_INTF_METADATA
where SYS_INTF_METADATA.INTF_SYS_NAME=JOB_TRIGGER_METADATA.INTF_SYS_NAME 
and SYS_INTF_METADATA.INTF_SYS_ID=JOB_TRIGGER_METADATA.INTF_SYS_ID
and SYS_INTF_METADATA.INTF_NAME=JOB_TRIGGER_METADATA.INTF_NAME
ORDER BY SYS_INTF_METADATA.INTF_PRTY
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
    task = BashOperator(
        task_id=task_name,
        bash_command=bash_command,
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
