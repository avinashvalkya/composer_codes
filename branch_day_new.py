from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 3, 20)
}

dag = DAG('dynamic_branching_dag', default_args=default_args)

# Define the SQL query to get the column values from BigQuery
sql_query = """
    SELECT INGEST_PTRN FROM golden-imprint-374915.gcp_dataeng_demos.SYS_INTF_METADATA LIMIT 1
"""

# Define the BigQueryOperator to execute the SQL query
get_column_values = BigQueryOperator(
    task_id='get_column_values',
    sql=sql_query,
    use_legacy_sql=False,
    dag=dag
)

# Define the function to generate the task ID for the branch task
def generate_task_id(column_value):
    return f"branch_task_{column_value}"

# Define the function to determine the branch based on the column value
def determine_branch(column_value):
    if column_value == 'FILE-FILE':
        return generate_task_id('FILE-FILE')
    elif column_value == 'FILE-RELATIONALDB':
        return generate_task_id('FILE-RELATIONALDB')
    else:
        return 'default_branch_task'

# Define the BranchPythonOperator to determine the branch
determine_branch_task = BranchPythonOperator(
    task_id='determine_branch_task',
    python_callable=determine_branch,
    op_kwargs={'column_value': str(get_column_values.output['rows'][0]['INGEST_PTRN'])},
    dag=dag
)

# Define the branch tasks
branch_task_value_1 = DummyOperator(
    task_id=generate_task_id('FILE-FILE'),
    dag=dag
)

branch_task_value_2 = DummyOperator(
    task_id=generate_task_id('FILE-RELATIONALDB'),
    dag=dag
)

default_branch_task = DummyOperator(
    task_id='default_branch_task',
    dag=dag
)

# Set the dependencies between tasks
get_column_values >> determine_branch_task >> [branch_task_value_1, branch_task_value_2, default_branch_task]
