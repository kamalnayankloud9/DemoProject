# import requests
# import csv
# import json
import  boto3
from auth import ACCESS_KEY,SECRET_KEY

def pushS3():
    ''' pushing data to S3 bucket'''
    client=boto3.client('s3',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    client.create_bucket(Bucket='kamal-airflow-data-engineering-csv-1.0')
    with open("/usr/local/airflow/dags/data.csv","rb") as f:
        client.upload_fileobj(f,"kamal-airflow-data-engineering-csv-1.0","data.csv")

    print("Upload Completed")
