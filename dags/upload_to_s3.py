from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

dag = DAG(
        dag_id = 'upload_to_s3',
        start_date = datetime(2021,3,9),
        schedule_interval = '@once',
        catchup = False
    )


####################
# 변수 세팅
####################
s3_bucket = Variable.get("sunny_s3_bucket")
s3_conn_id = 'sunny_aws_s3'
upload_file= 'test.csv'
s3_key = 'sunnytest/'+upload_file


####################
# ETL 구현
####################

def extract_spotify(**context):

	# api 인증

	# api 호출

	return

def transform(**context):

	return


def upload_s3(**context):

	s3_hook = S3Hook(s3_conn_id)
	s3_hook.load_file(filename='/var/lib/airflow/test/'+upload_file,key=s3_key,bucket_name=s3_bucket)






######################
# dag 구성
######################
extract_spotify = PythonOperator(
	task_id = 'extract_spotify',
	python_callable = extract_spotify,
	provide_context = True,
	dag = dag
	)



upload_s3 = PythonOperator(
        task_id = 'upload_s3',
        python_callable = upload_s3,
	provide_context = True,
        dag = dag
        )


extract_spotify
