import os

CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "udacity_capstone")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", 9042)
