from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from s3_upload import pushS3
import boto3


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG("S3_UPLOAD",default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    t1=PythonOperator(task_id="S3-Using-Python", python_callable=pushS3)