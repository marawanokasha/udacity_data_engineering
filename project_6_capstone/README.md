## Installation

1. Install Airflow
```
export AIRFLOW_HOME=/opt/airflow
pip install werkzeug==0.15.4 apache-airflow[postgres,aws]

# set the correct dag directory
sed -i 's|dags_folder = .*|dags_folder = /home/workspace/airflow/dags|' /opt/airflow/airflow.cfg
# set the correct port for the webserver
sed -i 's|web_server_port = 8080|web_server_port = 3000|' /opt/airflow/airflow.cfg

# remove examples
sed -i 's|load_examples = True|load_examples = False|' /opt/airflow/airflow.cfg
sed -i 's|load_default_connections = True|load_default_connections = False|' /opt/airflow/airflow.cfg

# initialize the airflow DB
airflow initdb
```

2. Start Airflow
```
airflow scheduler -D & airflow webserver -D
```

3. Stop Airflow
```
cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill & rm $AIRFLOW_HOME/airflow-scheduler.pid
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill & rm $AIRFLOW_HOME/airflow-webserver.pid $AIRFLOW_HOME/airflow-webserver-monitor.pid
```

i94cit is not the same as i94res, could be because of multi-hop trips


Data checks:
- Date added to the files can't be > date arrived


Scope:
- Web application for displaying people arriving to the US on a monthly basis