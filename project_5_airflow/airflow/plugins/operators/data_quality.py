from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 test_cases=None,
                 *args, **kwargs):
        """
        :test_cases: tuple of (sql statement, test function) where the test function expects to be passed a list of list
                     of records that are the result of the SQL statement
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        self.log.info('Executing the Data Quality checks')
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        failures = 0
        for test_case in self.test_cases:
            statement, test_func = test_case
            
            self.log.info(f"Running Data Quality Check: {statement}")
            actual_result = redshift.get_records(statement)
            test_correct = test_func(actual_result)
            
            if not test_correct:
                failures += 1
                self.log.error(f"Data Quality Check failed.\n" 
                               f"Statement Was: {statement}\n"
                               f"Actual Result was {actual_result}")
            else:
                self.log.info("Check Passed")
                
        if failures > 0:
            raise Exception("One or more data quality checks failed, please check the logs for more details")
            
        
        