from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)

from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)


from s3_upload import pushS3
import boto3

JOB_FLOW_OVERRIDES = {
    "Name": "Movie review classifier",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}
default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG("S3_UPLOAD",default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    t1=PythonOperator(task_id="S3-Using-Python", python_callable=pushS3)

# Create an EMR cluster
with DAG("S3_UPLOAD",default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
	create_emr_cluster = EmrCreateJobFlowOperator(
    		task_id="create_emr_cluster",
    		default_args = default_args,
    		job_flow_overrides=JOB_FLOW_OVERRIDES,
    		aws_conn_id="aws_default",
    		emr_conn_id="emr_default",
    		dag=dag,
	)

# Terminate the EMR cluster
with DAG("S3_UPLOAD",default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
	terminate_emr_cluster = EmrTerminateJobFlowOperator(
	    	task_id="terminate_emr_cluster",
    		default_args = default_args,
    		job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    		aws_conn_id="aws_default",
    		dag=dag,
	)

t1>>create_emr_cluster>>terminate_emr_cluster