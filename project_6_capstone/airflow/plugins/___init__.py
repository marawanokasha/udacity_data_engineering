from __future__ import absolute_import, division, print_function

from airflow.plugins_manager import AirflowPlugin

import operators


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.CassandraExecutorOperator,
    ]
