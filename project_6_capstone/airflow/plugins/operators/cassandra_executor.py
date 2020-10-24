from airflow.models import BaseOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.utils.decorators import apply_defaults


class CassandraExecutorOperator(BaseOperator):

    ui_color = '#F2A993'

    template_fields = ['cql_statements']

    @apply_defaults
    def __init__(self,
                 cassandra_conn_id="cassandra",
                 cql_statements=None,
                 *args, **kwargs):

        super(CassandraExecutorOperator, self).__init__(*args, **kwargs)
        self.cassandra_conn_id = cassandra_conn_id
        self.cql_statements = cql_statements

    def execute(self, context):
        cassandra = CassandraHook(self.cassandra_conn_id)
        session = cassandra.get_conn()

        for cql_statement in self.cql_statements:
            self.log.info(f'Executing the CQL statement: \n{cql_statement}')
            session.execute(cql_statement)
