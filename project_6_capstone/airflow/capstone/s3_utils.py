import os
import tempfile
import logging
import requests

from airflow.hooks.S3_hook import S3Hook


logger = logging.getLogger(__name__)


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
        logger.info("Uploading the contents of the folder: {} to S3".format(path))
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                key_name = full_path.replace("{}/".format(path), "", 1)
                if prefix:
                    key_name = os.path.join(prefix, key_name)
                logger.info("Uploading file: {} to key: {} in bucket: {} in S3".format(full_path, key_name))
                _copy_to_s3(s3_hook, full_path, key_name, bucket_name, replace=replace)
    else:
        key_name = os.path.basename(path) if not prefix else os.path.join(prefix, os.path.basename(path))
        logger.info("Uploading file: {} to key: {} in bucket: {} in S3".format(path, key_name))
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
    logger.info("Downloading file from {}".format(url))

    with tempfile.NamedTemporaryFile() as tmpfile:
        r = requests.get(url)
        tmpfile.write(r.content)

        path = tmpfile.name
        key_name = os.path.basename(url) if not prefix else os.path.join(prefix, os.path.basename(url))
        _copy_to_s3(s3_hook, path, key_name, bucket_name, replace=replace)


def _copy_to_s3(s3_hook, path: str, key_name: str, bucket_name: str, replace=False):
    """
    Copy a local file to an S3 bucket using an airflow S3 hook
    """

    logging.info("Uploading file: {} from {} in bucket: {}".format(key_name, path, bucket_name))
    try:
        s3_hook.load_file(path, key_name, bucket_name=bucket_name, replace=replace)
        return True
    except ValueError:
        if not replace:
            logger.warning("Key: {} already exists in {}, not replacing".format(key_name, bucket_name))
            return False
        raise
