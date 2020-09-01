#/bin/bash

pip install numpy werkzeug==0.15.4 apache-airflow[postgres,aws]

export AIRFLOW_HOME=/opt/airflow

AIRFLOW_CF_FILE=$AIRFLOW_HOME/airflow.cfg
DAG_FOLDER=/home/workspace/airflow/dags

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

printf "export AIRFLOW_HOME=$AIRFLOW_HOME\n" >> ~/.bashrc
