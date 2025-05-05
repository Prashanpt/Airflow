# Sample Dag - 1 (Zero_Dag)

# We will create sample dag, which will have two simpls tasks

#Importing necessary operators and modules

from airflow import DAG     
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# standard argments which be applicable for all the tasks in the dag.
args={
       'owner':'prashant',
       'retries':2,
        'retry_delay': timedelta(minutes=5)
}

# Defining our Dag

with DAG(
    dag_id="First_dag_by_pt",   # By this name  only, our dag will be visible in the airflow environment
    default_args=args,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
) as dag:

    # task - 1
    print_hello=BashOperator(
        task_id="First_task",
        bash_command='echo Hello',
        )
    
    # task - 2
    print_world= BashOperator(
        task_id='second_task',
        bash_command='echo World',
    )
 # order in which our task will be executed
 
    print_hello>>print_world


