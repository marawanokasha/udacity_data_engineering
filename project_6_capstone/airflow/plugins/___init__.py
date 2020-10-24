from __future__ import absolute_import, division, print_function

from airflow.plugins_manager import AirflowPlugin

import operators


# Defining the plugin class
class CassandraPlugin(AirflowPlugin):
    name = "cassandra_plugin"
    operators = [
        operators.CassandraExecutorOperator,
    ]
