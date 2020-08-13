from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    INSERT_SQL = "INSERT INTO {table} "

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table=None,
                 sql_statement=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info(f'Loading the fact table {self.table}')
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f'Now inserting the data into the {self.table} fact table')
        redshift.run(LoadFactOperator.INSERT_SQL.format(table=self.table) + self.sql_statement)
        
        
