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
from capstone.constants import Connections, DAGVariables
from capstone.spark_utils import get_emr_add_steps_spec

SPARK_JOB_FILE = "jobs/run_data_processing.py"

SPARK_TASK_ID = 'processing_task'

SAURFANG_PACKAGE = 'saurfang:spark-sas7bdat:2.1.0-s_2.11'

SPARK_JOB_PARAMS = [
    '--raw-data-path', "s3://{}".format(Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE)),
    '--staging-data-path', "s3://{}".format(Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE))
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
    job_flow_id=Variable.get(DAGVariables.EMR_CLUSTER_ID_VARIABLE),
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    steps=get_emr_add_steps_spec("data_processing", SPARK_JOB_FILE, SPARK_JOB_PARAMS, SAURFANG_PACKAGE),
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
