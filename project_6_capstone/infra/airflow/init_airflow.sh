#!/bin/bash

echo "====== Adding Connections"
airflow connections -a --conn_id s3 --conn_type s3 --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY
airflow connections -a --conn_id aws --conn_type aws --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY --conn_extra "{\"region_name\": \"$AWS_REGION\"}"
airflow connections -a --conn_id livy --conn_type http --conn_host localhost --conn_port 8998
airflow connections -a --conn_id redshift --conn_type postgres --conn_host $REDSHIFT_HOST --conn_port $REDSHIFT_PORT --conn_schema $REDSHIFT_DB_NAME --conn_login $REDSHIFT_MASTER_USERNAME --conn_password $REDSHIFT_MASTER_PASSWORD

echo "====== Adding Variables"
airflow variables --set raw_data_bucket $RAW_DATA_BUCKET_NAME
airflow variables --set staging_data_bucket $STAGING_DATA_BUCKET_NAME
airflow variables --set script_bucket $UTIL_BUCKET_NAME

# variables for the initial data copy
airflow variables --set raw_files_folder /data/18-83510-I94-Data-2016
airflow variables --set gdp_data_url https://datahub.io/core/gdp/r/gdp.csv

airflow variables --set emr_cluster_id $EMR_CLUSTER_ID
airflow variables --set redshift_iam_role $REDSHIFT_IAM_ROLE

# initdb again so the examples don't show
airflow initdb