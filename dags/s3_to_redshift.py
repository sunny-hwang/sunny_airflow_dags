from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


####################
# 변수 세팅
####################
s3_bucket = 'sunny-python-crawler'
s3_conn_id = 'sunny_aws_s3'
s3_prefix = 'gmarket'

####################
# DAG 세팅
####################

dag = DAG(
        dag_id = 's3_to_redshift',
        start_date = datetime(2021,3,30),
        schedule_interval = '@once',
        catchup = False
    )


####################
# ETL 구현
####################

	
s3_hook = S3Hook(s3_bucket)
prefix_key = s3_prefix
keys = s3_hook.list_keys(bucket_name=s3_bucket, prefix=prefix_key)

json_keys={'xpath':[], 'data':[]}
for k in keys:
	if 'xpath.json' in k:
        	json_keys['xpath'].append(k)
	elif 'data.json' in k:
		json_keys['data'].append(k)

######################
# dag 구성
######################

#plugins 사용

s3toredshift_xpath = S3ToRedshiftOperator(
	schema='raw_data',
	table='product_xpath',
	s3_bucket=s3_bucket,
	s3_key=json_keys['xpath'],
	copy_options="json 'auto'",
	aws_conn_id='aws_s3_default',
	
	task_id='S3_to_Redshift'+"_"+"pruduct_xpath",
	dag=dag
)

s3toredshift_data = S3ToRedshiftOperator(
        schema='raw_data',
        table='product_channel_data',
        s3_bucket=s3_bucket,
        s3_key=json_keys['data'],
        copy_options="json 'auto'",
        aws_conn_id='aws_s3_default',

        task_id='S3_to_Redshift'+"_"+"pruduct_channel_data",
        dag=dag
)



s3toredshift_xpath >> s3toredshift_data
