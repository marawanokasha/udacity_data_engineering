#!/bin/bash

echo "====== Adding Connections"
airflow connections -a --conn_id s3 --conn_type s3 --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY
airflow connections -a --conn_id aws --conn_type aws --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY --conn_extra "{\"region_name\": \"$AWS_REGION\"}"
airflow connections -a --conn_id livy --conn_type http --conn_host localhost --conn_port 8998

echo "====== Adding Variables"
airflow variables --set raw_data_bucket udacity-capstone-raw-data
airflow variables --set staging_data_bucket udacity-capstone-staging-data
airflow variables --set script_bucket udacity-capstone-util-bucket

airflow variables --set raw_files_folder /data/18-83510-I94-Data-2016
airflow variables --set gdp_data_url https://datahub.io/core/gdp/r/gdp.csv

airflow variables --set emr_cluster_name udacity-capstone-emr-cluster
