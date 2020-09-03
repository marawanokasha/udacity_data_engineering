#/bin/bash

export AIRFLOW_HOME=/opt/airflow
printf "export AIRFLOW_HOME=$AIRFLOW_HOME\n" >> ~/.bashrc

# installing pyyaml without --ignore-installed fails because of some distutils problem. Pyyaml is used by airflow
pip install --ignore-installed PyYAML
# the [apache.livy] doesn't actually install anything for airflow 1.10, but it's here for when airflow 2.0 is released
pip install numpy werkzeug==0.15.4 apache-airflow[postgres,amazon,apache.livy]
# for now, until airflow 2.0 is released, the livy integration is released as a backport
pip install pip install apache-airflow-backport-providers-apache.livy

AIRFLOW_CF_FILE=$AIRFLOW_HOME/airflow.cfg
# we set the DAG_FOLDER to the airflow folder and not the airflow/dags folder so that other folders inside
# the airflow folder are automatically added to the PYTHONPATH by airflow
DAG_FOLDER=/home/workspace/airflow

# initialize the airflow DB, this creates the config files in $AIRFLOW_HOME
airflow initdb

# set the correct dag directory
sed -i "s|dags_folder = .*|dags_folder = $DAG_FOLDER|" $AIRFLOW_CF_FILE
# set the correct port for the webserver
sed -i "s|web_server_port = 8080|web_server_port = 3000|" $AIRFLOW_CF_FILE

# remove examples
sed -i "s|load_examples = True|load_examples = False|" $AIRFLOW_CF_FILE
sed -i "s|load_default_connections = True|load_default_connections = False|" $AIRFLOW_CF_FILE

# re-initialize the airflow DB so that the examples don't show
airflow initdb
