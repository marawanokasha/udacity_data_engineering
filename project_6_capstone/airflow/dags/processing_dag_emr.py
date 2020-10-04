######################################################
# DAG for executing the `run_data_processing.py` spark job using EMR
######################################################

import datetime
from pathlib import Path

from airflow.contrib.operators.emr_add_steps_operator import \
    EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from capstone.constants import Connections, DAGVariables

SPARK_JOB_FILE = "jobs/run_data_processing.py"

SPARK_TASK_ID = 'processing_task'

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
                # The Spark session may have already been created maybe.
                '--packages', 'saurfang:spark-sas7bdat:2.1.0-s_2.11',
                '--py-files', 's3://{}/spark/lib.zip'.format(Variable.get(DAGVariables.SCRIPT_BUCKET_NAME_VARIABLE)),
                's3://{}/spark/{}'.format(Variable.get(DAGVariables.SCRIPT_BUCKET_NAME_VARIABLE), SPARK_JOB_FILE),
                '--raw-data-path', "s3://{}".format(Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE)),
                '--staging-data-path', "s3://{}".format(Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE))
            ],
        },
    }
]

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime.now()
}

dag = DAG(
    dag_id='processing_dag_emr',
    default_args=args,
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

processing_task = EmrAddStepsOperator(
    task_id=SPARK_TASK_ID,
    job_flow_name=Variable.get(DAGVariables.EMR_CLUSTER_NAME_VARIABLE),
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    steps=SPARK_STEPS,
    cluster_states=["WAITING"],
    # we need this so that the job_flow_id is exported as an xcom variable so it can be used by the sensor
    do_xcom_push=True,
    dag=dag
)

task_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('%s', key='job_flow_id') }}" % SPARK_TASK_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='%s', key='return_value')[0] }}" % SPARK_TASK_ID,
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    dag=dag
)

processing_task >> task_checker
