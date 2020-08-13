from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    TRUNCATE_SQL = "TRUNCATE TABLE {table}"
    INSERT_SQL = "INSERT INTO {table} "

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table=None,
                 sql_statement=None,
                 truncate_before_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.truncate_before_insert = truncate_before_insert

    def execute(self, context):
        self.log.info(f'Loading the dimension table {self.table}')
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_before_insert:
            self.log.info(f'Truncating the dimension table {self.table} before insertion')
            redshift.run(LoadDimensionOperator.TRUNCATE_SQL.format(table=self.table))
        
        self.log.info(f'Now inserting the data into the {self.table} table')
        redshift.run(LoadDimensionOperator.INSERT_SQL.format(table=self.table) + self.sql_statement)
        
        
