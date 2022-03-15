#Importing Libraries:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime

# These args will get passed on to the python operator
default_args = {
    'owner': 'rushal1209',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'email': ['rushalsuvarna.1209@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the python function:
def my_function1(x):
    return x + " is a must have tool for Data Engineers."

def my_function2(x):
    return x + " is learning Airflow"

# define the DAG
dag = DAG(
    '1_PythonOperator',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval=timedelta(days=1)
)

# define the first task
start =DummyOperator(
    task_id='start',
    dag=dag
)

callf1 = PythonOperator(
    task_id ='callf1',
    python_callable = my_function1,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag
)

callf2 = PythonOperator(
    task_id ='callf2',
    python_callable = my_function2,
    op_kwargs = {"x" : "Rushal"},
    dag=dag
)

end =DummyOperator(
    task_id='end',
    dag=dag
)

start>>callf1 >> callf2>>end