#/bin/bash

apt-get install -y libmysqlclient-dev libssl-dev libkrb5-dev  libsasl2-dev
conda update -y -n base conda
conda create -y -n airflow pip setuptools python=3.6
activate airflow
mkdir -p /root/airflow
export AIRFLOW_HOME=/root/airflow
pip install "boto3>=1.9,<2"
pip install psycopg2
SLUGIFY_USES_TEXT_UNIDECODE=yes pip install apache-airflow[async,devel,crypto,hdfs,hive,password,postgres,s3]