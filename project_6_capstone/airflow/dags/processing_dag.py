import os
import time
import tempfile
import logging
import requests
from pathlib import Path
from builtins import range
from pprint import pprint
import datetime
from airflow.utils.dates import days_ago

from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.operators.livy_operator import LivyOperator

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


RAW_BUCKET_NAME_VARIABLE = "raw_data_bucket"
STAGING_BUCKET_NAME_VARIABLE = "staging_data_bucket"
SCRIPT_BUCKET_NAME_VARIABLE = "script_bucket"
AWS_CONNECTION_ID = "aws"
EMR_CLUSTER_NAME_VARIABLE = "emr_cluster_name"


# the actual content of this file is copied to a subdirectory .ipynb_checkpoints by airflow to run it, that's why we use parent.parent
PROJECT_ROOT_FOLDER = Path(__file__).parent.parent.absolute()


# Request schema https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.add_job_flow_steps
# https://stackoverflow.com/questions/36706512/how-do-you-automate-pyspark-jobs-on-emr-using-boto3-or-otherwise
SPARK_STEPS = [
    {
        'Name': 'spark_preprocessing',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                 # for some reason, setting up the SparkSession with the spark.jars.packages doesn't actually load that package. 
                 # # The Spark session may have already been created maybe. Try again with livy
                '--packages', 'saurfang:spark-sas7bdat:2.1.0-s_2.11',
                '--py-files', 's3://{}/spark/lib.zip'.format(Variable.get(SCRIPT_BUCKET_NAME_VARIABLE)),
                's3://{}/spark/jobs/run_data_ingestion.py'.format(Variable.get(SCRIPT_BUCKET_NAME_VARIABLE)),
                '--raw-data-path', "s3://{}".format(Variable.get(RAW_BUCKET_NAME_VARIABLE)),
                '--staging-data-path', "s3://{}".format(Variable.get(STAGING_BUCKET_NAME_VARIABLE))
            ],
        },
    }
]

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime.now()
}

dag = DAG(
    dag_id='processing_dag',
    default_args=args,
    schedule_interval=None,
    catchup=False,
    concurrency=3
)

processing_task = EmrAddStepsOperator(
    task_id='processing_task',
    job_flow_name=Variable.get(EMR_CLUSTER_NAME_VARIABLE),
    aws_conn_id=AWS_CONNECTION_ID,
    steps=SPARK_STEPS,
    cluster_states=["WAITING"],
    do_xcom_push=True,  # we need this so that the job_flow_id is exported as an xcom variable so it can be used by the sensor
    dag=dag
)

task_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('processing_task', key='job_flow_id') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='processing_task', key='return_value')[0] }}",
    aws_conn_id=AWS_CONNECTION_ID,
    dag=dag
)

processing_task >> task_checker
