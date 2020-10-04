import os
from pathlib import Path
import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, Variable

from capstone.s3_utils import copy_files_to_s3, download_and_copy_files_to_s3
from capstone.constants import Connections, DAGVariables


RAW_FILES_FOLDER_VARIABLE = "raw_files_folder"
GDP_DATA_URL_VARIABLE = "gdp_data_url"

LOCAL_FILES_COPY_TASKS = [
    ("copy_country_codes_data", "./data/country_codes.csv"),
    ("copy_country_iso_data", "./data/country_iso.csv"),
    ("copy_states_data", "./data/states.csv"),
    ("copy_ports_data", "./data/ports.csv"),
]

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


copy_immigration_data = PythonOperator(
    task_id='copy_immigration_data',
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

local_copy_tasks = []
for task_id, local_file_path in LOCAL_FILES_COPY_TASKS:
    local_copy_tasks.append(PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=copy_files_to_s3,
        dag=dag,
        op_kwargs={
            "connection_id": Connections.S3_CONNECTION_ID,
            "bucket_name": Variable.get(DAGVariables.RAW_BUCKET_NAME_VARIABLE),
            "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, local_file_path)).resolve()),
            "replace": True
        }
    ))

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

start_operator >> [
    copy_immigration_data,
    download_and_copy_gdp_data
] + local_copy_tasks >> end_operator
