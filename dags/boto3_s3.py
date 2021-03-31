from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

import boto3

dag = DAG(
        dag_id = 'boto3_s3_list',
        start_date = datetime(2021,3,9),
        schedule_interval = '@once',
        catchup = False
    )

def connect_to_s3_via_boto3():

    # boto3로 s3 연동 (AWS CLI가 설정되어 있는 상태)
    
    session = boto3.session.Session(profile_name='sunny')
    
    # Retrieve the list of existing buckets
    s3 = session.client('s3')
    response = s3.list_buckets()
    
    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

    return response['Buckets']

connect_s3 = PythonOperator(
        task_id = 'connect_s3',
        python_callable = connect_to_s3_via_boto3,
        dag = dag
        )

connect_s3

