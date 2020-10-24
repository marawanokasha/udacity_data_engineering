######################################################
# DAG for executing the `run_data_processing.py` spark job using Livy
######################################################

from pathlib import Path
import datetime
import sys

from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from capstone.constants import DAGVariables, Connections

SPARK_JOB_FILE = "jobs/run_data_processing.py"


# the actual content of this file is copied to a subdirectory .ipynb_checkpoints by airflow to run it, that's why we use parent.parent
PROJECT_ROOT_FOLDER = Path(__file__).parent.parent.absolute()

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime.now()
}

dag = DAG(
    dag_id='processing_dag_livy',
    default_args=args,
    schedule_interval=None,
    catchup=False,
    concurrency=1
)

processing_task = LivyOperator(
    task_id='processing_task',
    file='s3://{}/spark/{}'.format(Variable.get(DAGVariables.SCRIPT_BUCKET_NAME_VARIABLE), SPARK_JOB_FILE),
    py_files=['s3://{}/spark/lib.zip'.format(Variable.get(DAGVariables.SCRIPT_BUCKET_NAME_VARIABLE))],
    args=[
        '--raw-data-path', "s3://{}".format(Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE)),
        '--staging-data-path', "s3://{}".format(Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE))
    ],
    conf={
        # we need this one unfortunately, putting just in the SparkSession in code doesn't work
        "spark.jars.packages": "saurfang:spark-sas7bdat:2.1.0-s_2.11",
        # not really needed, delete after seeing what it does
        "spark.driver.extraJavaOptions" : "-Dlog4jspark.root.logger=DEBUG,console"
    },
    livy_conn_id=Connections.LIVY_CONNECTION_ID,
    polling_interval=5,
    dag=dag
)
