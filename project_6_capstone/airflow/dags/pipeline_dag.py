import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='run_pipeline',
    default_args=args,
    schedule_interval=None
)

AWS_CONNECTION_NAME = 'aws_conn'

def copy_files(**kwargs):

    s3_hook = S3Hook(AWS_CONNECTION_NAME)
    s3_hook.load_file()
    print(kwargs)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='copy_files_task',
    provide_context=True,
    python_callable=copy_files,
    dag=dag,
)
