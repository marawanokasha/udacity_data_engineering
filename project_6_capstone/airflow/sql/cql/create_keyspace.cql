CREATE KEYSPACE IF NOT EXISTS {{params.project_keyspace}} 
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
