from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


def print_hello():
	print("hello!")
	return "hello!"

def print_goodbye():
	print("goodbye!")
	return "goodbye!"


#DAG 설정
dag = DAG(
	dag_id = 'my_first_dag',
	start_date = datetime(2021,3,7),
	schedule_interval = '*/10 * * * *'
    )

#DAG Task 작성
print_hello = PythonOperator(
	task_id = 'print_hello',
	#python_callable param points to the function you want to run 
	python_callable = print_hello,
	#dag param points to the DAG that this task is a part of
	dag = dag
    )

#print_goodbye = PythonOperator(
#	task_id = 'print_goodbye',
#	python_callable = print_goodbye,
#	dag = dag
#    )

#Assign the order of the tasks in our DAG
#print_hello >> print_goodbye
print_hello
