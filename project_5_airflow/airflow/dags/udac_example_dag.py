from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from dimensions_subdag import get_dimensions_subdag


BUCKET_REGION = 'us-west-2'
DAG_ID = 'udac_example_dag'
REDSHIFT_CONNECTION_ID = 'redshift'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG(DAG_ID,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *', # this means it should run every hour
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables', 
    dag=dag,
    postgres_conn_id=REDSHIFT_CONNECTION_ID,
    sql=open(os.path.join(os.path.dirname(__file__), "..", "create_tables.sql"), "r").read()
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    table="staging_events",
    json_path_file="s3://udacity-dend/log_json_path.json",
    s3_region=BUCKET_REGION,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    table="staging_songs",
    s3_region=BUCKET_REGION,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    table="songplays",
    sql_statement=SqlQueries.songplay_table_insert
)

dimensions_task_id = "Load_dimension_tables"
dimensions_subdag_task = SubDagOperator(
    subdag=get_dimensions_subdag(
        DAG_ID,
        dimensions_task_id,
        redshift_conn_id=REDSHIFT_CONNECTION_ID,
        default_args=default_args
    ),
    task_id=dimensions_task_id,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    test_cases=SqlQueries.data_quality_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> dimensions_subdag_task >> run_quality_checks

run_quality_checks >> end_operator
