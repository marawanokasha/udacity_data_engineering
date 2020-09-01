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

from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

S3_CONNECTION_ID = "s3"
RAW_FILES_FOLDER_VARIABLE = "raw_files_folder"
DATA_DESCRIPTION_FILE_VARIABLE = "raw_data_description_file"
RAW_BUCKT_NAME_VARIABLE = "raw_data_bucket"
GDP_DATA_URL_VARIABLE = "gdp_data_url"
PROJECT_ROOT_FOLDER = Path(__file__).parent.parent.absolute()


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

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


def copy_files_to_s3(connection_id: str, bucket_name: str, path: str, prefix=None, replace=False, **kwargs):
    """Copy All files recursively from a local root folder to an S3 bucket

    Args:
        connection_id (str): [description]
        root_folder_or_file_path (str): [description]
        prefix (str): [description]

    Returns:
        None
    """
    s3_hook = S3Hook(connection_id)

    if os.path.isdir(path):
        logging.info("Uploading the contents of the folder: {}".format(path))
        for root, directories, filenames in os.walk(path):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                key_name = full_path.replace("{}/".format(path), "", 1)
                if prefix:
                    key_name = os.path.join(prefix, key_name)
                _copy_to_s3(s3_hook, full_path, key_name, bucket_name, replace=replace)
    else:
        key_name = os.path.basename(path) if not prefix else os.path.join(prefix, os.path.basename(path))
        logging.info(key_name)
        _copy_to_s3(s3_hook, path, key_name, bucket_name, replace=replace)


def download_and_copy_files_to_s3(connection_id: str, bucket_name: str, url: str, prefix=None, replace=False, **kwargs) -> None:
    """Download a file from a URL and upload it to an S3 bucket

    Args:
        connection_id (str): [description]
        bucket_name (str): [description]
        url (str): [description]
        prefix ([type], optional): [description]. Defaults to None.
    """
    s3_hook = S3Hook(connection_id)
    logging.info("Downloading file from {}".format(url))

    with tempfile.NamedTemporaryFile() as tmpfile:
        r = requests.get(url)
        tmpfile.write(r.content)

        path = tmpfile.name
        key_name = os.path.basename(url) if not prefix else os.path.join(prefix, os.path.basename(url))
        _copy_to_s3(s3_hook, path, key_name, bucket_name, replace=replace)


def _copy_to_s3(s3_hook, path, key_name, bucket_name, replace=False):
    logging.info("Uploading file: {} from {} in bucket: {}".format(key_name, path, bucket_name))
    try:
        s3_hook.load_file(path, key_name, bucket_name=bucket_name, replace=replace)
        return True
    except ValueError:
        if not replace:
            logging.warn("Key: {} already exists in {}, not replacing".format(key_name, bucket_name))
            return False
        raise


copy_raw_data = PythonOperator(
    task_id='copy_raw_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": S3_CONNECTION_ID,
        "bucket_name": Variable.get(RAW_BUCKT_NAME_VARIABLE),
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
        "connection_id": S3_CONNECTION_ID,
        "bucket_name": Variable.get(RAW_BUCKT_NAME_VARIABLE),
        "path": Variable.get(DATA_DESCRIPTION_FILE_VARIABLE)
    }
)

download_and_copy_gdp_data = PythonOperator(
    task_id='download_and_copy_gdp_data',
    provide_context=True,
    python_callable=download_and_copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": S3_CONNECTION_ID,
        "bucket_name": Variable.get(RAW_BUCKT_NAME_VARIABLE),
        "url": Variable.get(GDP_DATA_URL_VARIABLE)
    }
)

copy_country_codes_data = PythonOperator(
    task_id='copy_country_codes_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": S3_CONNECTION_ID,
        "bucket_name": Variable.get(RAW_BUCKT_NAME_VARIABLE),
        # the actual content of this file is copied to a subdirectory .ipynb_checkpoints by airflow to run it, that's why we use parent.parent
        "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, "../../data/country_codes.csv")).resolve()),
        "replace": True
    }
)

copy_states_data = PythonOperator(
    task_id='copy_states_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": S3_CONNECTION_ID,
        "bucket_name": Variable.get(RAW_BUCKT_NAME_VARIABLE),
        "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, "../../data/states.csv")).resolve()),
        "replace": True
    }
)

copy_ports_data = PythonOperator(
    task_id='copy_ports_data',
    provide_context=True,
    python_callable=copy_files_to_s3,
    dag=dag,
    op_kwargs={
        "connection_id": S3_CONNECTION_ID,
        "bucket_name": Variable.get(RAW_BUCKT_NAME_VARIABLE),
        "path": str(Path(os.path.join(PROJECT_ROOT_FOLDER, "../../data/ports.csv")).resolve()),
        "replace": True
    }
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [
    copy_raw_data,
    copy_data_description, download_and_copy_gdp_data,
    copy_country_codes_data, copy_states_data, copy_ports_data
] >> end_operator
