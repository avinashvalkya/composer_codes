from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

@dag(default_args={'start_date': datetime(2023, 4, 25)}, schedule_interval=None, catchup=False)
def example_branching_operator_taskflow():
    
    @task
    def check_condition():
        x = 10
        if x > 5:
            return "greater_than_5"
        else:
            return "less_than_5"
    
    with TaskGroup('branch_tasks') as branch_tasks:
        
        @task
        def greater_than_5():
            print("x is greater than 5")
        
        @task
        def less_than_5():
            print("x is not greater than 5")
    
    condition_output = check_condition()
    
    branch = {
        'greater_than_5': greater_than_5,
        'less_than_5': less_than_5
    }
    
    branch_task = branch_tasks.get(branch[condition_output])
    branch_task()
