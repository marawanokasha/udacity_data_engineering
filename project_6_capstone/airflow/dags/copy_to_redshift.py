import datetime
import os

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import \
    EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from capstone.constants import Connections, DAGVariables
from capstone.spark_utils import get_emr_add_steps_spec

SPARK_JOB_FILE = "jobs/run_copy_for_redshift.py"

SPARK_TASK_ID = 'copy_to_s3_for_redshift_task'

SPARK_JOB_PARAMS = [
    '--staging-data-path', "s3://{}".format(Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE))
]

COPY_TO_REDSHIFT_COMMAND = """
    COPY {table}
    FROM 's3://{s3_bucket}/{s3_key}'
    IAM_ROLE '{iam_role}'
    JSON 'auto'
"""

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime.now()
}

dag = DAG(
    dag_id='copy_to_redshift',
    default_args=args,
    schedule_interval=None,
    description='Copy the final data for analysis in Redshift',
    catchup=False
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id=Connections.REDSHIFT_CONNECTION_ID,
    sql=open(os.path.join(os.path.dirname(__file__), "..", "sql", "create_tables.sql"), "r").read()
)

copy_to_s3_task = EmrAddStepsOperator(
    task_id=SPARK_TASK_ID,
    job_flow_id=Variable.get(DAGVariables.EMR_CLUSTER_ID_VARIABLE),
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    steps=get_emr_add_steps_spec("copy_to_s3_for_redshift", SPARK_JOB_FILE, SPARK_JOB_PARAMS, None),
    cluster_states=["WAITING"],
    # we need this so that the job_flow_id is exported as an xcom variable so it can be used by the sensor
    do_xcom_push=True,
    dag=dag
)

copy_to_s3_task_checker = EmrStepSensor(
    task_id='wait_for_emr_processing',
    job_flow_id="{{ task_instance.xcom_pull('%s', key='job_flow_id') }}" % SPARK_TASK_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='%s', key='return_value')[0] }}" % SPARK_TASK_ID,
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    dag=dag
)


copy_immigration_table = PostgresOperator(
    task_id='copy_immigration_table_to_redshift',
    dag=dag,
    postgres_conn_id=Connections.REDSHIFT_CONNECTION_ID,
    sql=COPY_TO_REDSHIFT_COMMAND.format(
        table='immigration',
        s3_bucket=Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE),
        s3_key='redshift_staging/immigration',
        iam_role=Variable.get(DAGVariables.REDSHIFT_IAM_ROLE_VARIABLE)
    )
)

copy_states_table = PostgresOperator(
    task_id='copy_states_table_to_redshift',
    dag=dag,
    postgres_conn_id=Connections.REDSHIFT_CONNECTION_ID,
    sql=COPY_TO_REDSHIFT_COMMAND.format(
        table='state',
        s3_bucket=Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE),
        s3_key='redshift_staging/state',
        iam_role=Variable.get(DAGVariables.REDSHIFT_IAM_ROLE_VARIABLE)
    )
)

copy_countries_table = PostgresOperator(
    task_id='copy_countries_table_to_redshift',
    dag=dag,
    postgres_conn_id=Connections.REDSHIFT_CONNECTION_ID,
    sql=COPY_TO_REDSHIFT_COMMAND.format(
        table='country',
        s3_bucket=Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE),
        s3_key='redshift_staging/country',
        iam_role=Variable.get(DAGVariables.REDSHIFT_IAM_ROLE_VARIABLE)
    )
)

copy_visa_type_table = PostgresOperator(
    task_id='copy_visa_type_table_to_redshift',
    dag=dag,
    postgres_conn_id=Connections.REDSHIFT_CONNECTION_ID,
    sql=COPY_TO_REDSHIFT_COMMAND.format(
        table='visa_type',
        s3_bucket=Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE),
        s3_key='redshift_staging/visa_type',
        iam_role=Variable.get(DAGVariables.REDSHIFT_IAM_ROLE_VARIABLE)
    )
)

copy_gdp_table = PostgresOperator(
    task_id='copy_gdp_table_to_redshift',
    dag=dag,
    postgres_conn_id=Connections.REDSHIFT_CONNECTION_ID,
    sql=COPY_TO_REDSHIFT_COMMAND.format(
        table='gdp',
        s3_bucket=Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE),
        s3_key='redshift_staging/gdp',
        iam_role=Variable.get(DAGVariables.REDSHIFT_IAM_ROLE_VARIABLE)
    )
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# start_operator >> create_tables >> [
#     copy_states_table, copy_countries_table, copy_gdp_table, copy_visa_type_table
# ] >> end_operator


start_operator >> create_tables >> \
    copy_to_s3_task >> copy_to_s3_task_checker >> \
    copy_immigration_table >> [
        copy_states_table, copy_countries_table, copy_gdp_table, copy_visa_type_table
    ] >> end_operator
