from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 2, 22),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag= DAG("Dummy", default_args=default_args, schedule_interval="@daily", catchup=False) # backlogs

def printString():
    print("This is a Test String")

t1=PythonOperator(task_id="printString", python_callable=printString, dag=dag)