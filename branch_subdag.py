from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdag import subdag

def determine_next_task(**kwargs):
    # Determine the next task to run based on some condition
    if some_condition:
        return "task_a"
    else:
        return "task_b"

subdag_ids = ["subdag1", "subdag2", "subdag3"]

with DAG("my_dynamic_dag") as dag:
    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=determine_next_task,
        provide_context=True
    )

    subdag_tasks = []
    for subdag_id in subdag_ids:
        subdag_task = SubDagOperator(
            task_id=f"{subdag_id}_subdag",
            subdag=subdag(dag.dag_id, f"{subdag_id}_subdag", ...)
        )
        subdag_tasks.append(subdag_task)

    branch_task >> subdag_tasks
