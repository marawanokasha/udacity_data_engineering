import datetime
import os

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import \
    EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
from airflow.operators.cassandra_plugin import CassandraExecutorOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from capstone.constants import Connections, DAGVariables
from capstone.spark_utils import get_emr_add_steps_spec

SPARK_JOB_FILE = "jobs/run_copy_to_cassandra.py"
CASSANDRA_PACKAGE = 'datastax:spark-cassandra-connector:2.4.0-s_2.11'
SPARK_TASK_ID = 'copy_to_cassandra_task'

SPARK_JOB_PARAMS = [
    '--staging-data-path', "s3://{}".format(Variable.get(DAGVariables.STAGING_BUCKET_NAME_VARIABLE)),
    '--cassandra-host', Variable.get(DAGVariables.CASSANDRA_HOST),
    '--cassandra-port', Variable.get(DAGVariables.CASSANDRA_PORT),
    '--cassandra-keyspace', Variable.get(DAGVariables.CASSANDRA_KEYSPACE)
]

CASSANDRA_STATEMENTS = [
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "create_keyspace.cql"), "r").read(),
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "use_keyspace.cql"), "r").read(),
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "create_immigration_stats.cql"), "r").read(),
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "create_country.cql"), "r").read(),
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "create_state.cql"), "r").read(),
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "create_visa_type.cql"), "r").read(),
    open(os.path.join(os.path.dirname(__file__), "..", "sql", "cql", "create_gdp.cql"), "r").read(),
]

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime.now()
}

dag = DAG(
    dag_id='copy_to_cassandra',
    default_args=args,
    schedule_interval=None,
    description='Copy data to Cassandra for use in the web app',
    catchup=False
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


create_tables = CassandraExecutorOperator(
    task_id='create_cassandra_tables',
    dag=dag,
    cassandra_conn_id=Connections.CASSANDRA_CONNECTION_ID,
    cql_statements=CASSANDRA_STATEMENTS,
    # template variable used by the templated field cql_statements
    params={
        'project_keyspace': Variable.get(DAGVariables.CASSANDRA_KEYSPACE)
    }
)


copy_to_cassandra_task = EmrAddStepsOperator(
    task_id=SPARK_TASK_ID,
    job_flow_id=Variable.get(DAGVariables.EMR_CLUSTER_ID_VARIABLE),
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    steps=get_emr_add_steps_spec("copy_to_cassandra", SPARK_JOB_FILE, SPARK_JOB_PARAMS, CASSANDRA_PACKAGE),
    cluster_states=["WAITING"],
    # we need this so that the job_flow_id is exported as an xcom variable so it can be used by the sensor
    do_xcom_push=True,
    dag=dag
)


copy_to_cassandra_task_checker = EmrStepSensor(
    task_id='wait_for_emr_processing',
    job_flow_id="{{ task_instance.xcom_pull('%s', key='job_flow_id') }}" % SPARK_TASK_ID,
    step_id="{{ task_instance.xcom_pull(task_ids='%s', key='return_value')[0] }}" % SPARK_TASK_ID,
    aws_conn_id=Connections.AWS_CONNECTION_ID,
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> \
    create_tables >> \
    copy_to_cassandra_task >> \
    copy_to_cassandra_task_checker >> \
    end_operator
