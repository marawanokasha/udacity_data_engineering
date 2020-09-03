import os
from pathlib import Path
import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, Variable

from capstone.s3_utils import copy_files_to_s3, download_and_copy_files_to_s3
from capstone.constants import Connections, DAGVariables


RAW_FILES_FOLDER_VARIABLE = "raw_files_folder"
DATA_DESCRIPTION_FILE_VARIABLE = "raw_data_description_file"
GDP_DATA_URL_VARIABLE = "gdp_data_url"


PROJECT_ROOT_FOLDER = Path(__file__).parent.parent.parent.absolute()


args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime.now()
}

dag = DAG(
    dag_id='copy_files_dag',
    default_args=args,
    schedule_interval=None,
    catchup=False,
    concurrency=3
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


copy_raw_data = PythonOperator(
    task_id='copy_raw_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": Connections.S3_CONNECTION_ID,
        "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
        "path": Variable.get(RAW_FILES_FOLDER_VARIABLE),
        "prefix": "i94-data"
    }
)

copy_data_description = PythonOperator(
    task_id='copy_data_description',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": Connections.S3_CONNECTION_ID,
        "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
        "path": Variable.get(DATA_DESCRIPTION_FILE_VARIABLE)
    }
)

download_and_copy_gdp_data = PythonOperator(
    task_id='download_and_copy_gdp_data',
    provide_context=True,
    python_callable=download_and_copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": Connections.S3_CONNECTION_ID,
        "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
        "url": Variable.get(GDP_DATA_URL_VARIABLE)
    }
)

copy_country_codes_data = PythonOperator(
    task_id='copy_country_codes_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": Connections.S3_CONNECTION_ID,
        "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
        "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, "./data/country_codes.csv")).resolve()),
        "replace": True
    }
)

copy_states_data = PythonOperator(
    task_id='copy_states_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": Connections.S3_CONNECTION_ID,
        "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
        "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, "./data/states.csv")).resolve()),
        "replace": True
    }
)

copy_ports_data = PythonOperator(
    task_id='copy_ports_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": Connections.S3_CONNECTION_ID,
        "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
        "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, "./data/ports.csv")).resolve()),
        "replace": True
    }
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [
    copy_raw_data,
    copy_data_description, download_and_copy_gdp_data,
    copy_country_codes_data, copy_states_data, copy_ports_data
] >> end_operator
