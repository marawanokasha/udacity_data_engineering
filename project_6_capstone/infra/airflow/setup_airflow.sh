#!/bin/bash

export AIRFLOW_HOME=/opt/airflow
printf "export AIRFLOW_HOME=$AIRFLOW_HOME\n" >> ~/.bashrc


echo "============= Installing requirements"
# on udacity's VM, installing pyyaml without --ignore-installed fails because it already exists in the default env. Pyyaml is used by airflow
pip install --ignore-installed PyYAML
# the [apache.livy] doesn't actually install anything for airflow 1.10, but it's here for when airflow 2.0 is released
pip install numpy werkzeug==0.15.4 apache-airflow[postgres,amazon,apache.livy,apache.cassandra,http]
# for now, until airflow 2.0 is released, the livy integration is released as a backport, so is http
pip install pip install apache-airflow-backport-providers-apache.livy apache-airflow-backport-providers-http apache-airflow-backport-providers-apache-cassandra

AIRFLOW_CF_FILE=$AIRFLOW_HOME/airflow.cfg
# we set the DAG_FOLDER to the airflow folder and not the airflow/dags folder so that other folders inside
# the airflow folder are automatically added to the PYTHONPATH by airflow
DAG_FOLDER=/home/workspace/airflow

echo "============= Initializing the DB for the first time"
# initialize the airflow DB, this creates the config files in $AIRFLOW_HOME
airflow initdb

echo "============= Setting the configuration options"
# set the correct dag directory
sed -i "s|dags_folder = .*|dags_folder = $DAG_FOLDER|" $AIRFLOW_CF_FILE
sed -i "s|plugins_folder = .*|plugins_folder = $DAG_FOLDER/plugins|" $AIRFLOW_CF_FILE
# set the correct port for the webserver
sed -i "s|web_server_port = 8080|web_server_port = 3000|" $AIRFLOW_CF_FILE

# remove examples
sed -i "s|load_examples = True|load_examples = False|" $AIRFLOW_CF_FILE
sed -i "s|load_default_connections = True|load_default_connections = False|" $AIRFLOW_CF_FILE

echo "AIRFLOW_HOME now set to $AIRFLOW_HOME"