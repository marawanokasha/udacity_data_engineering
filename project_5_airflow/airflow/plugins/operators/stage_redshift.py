from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    COPY_QUERY = """
        COPY {table}
        FROM 's3://{s3_bucket}/{s3_key}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        JSON '{json_path_file}'
    """
    REGION_SUFFIX = " REGION '{region}'"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws",
                 s3_bucket="",
                 s3_key="",
                 # we use this because loading to redshift from an S3 bucket not in the same region 
                 # as the cluster leads to an error, which can be fixed by explicitly passing 
                 # the region for the bucket
                 s3_region=None,
                 json_path_file="auto",
                 table=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.json_path_file = json_path_file
        self.table = table

    def execute(self, context):
        self.log.info(f"Staging Data from {self.s3_bucket}/{self.s3_key} to the {self.redshift_conn_id} Redshift DB")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Clearing the existing data before copying")
        
        redshift_hook.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info("Copying the data into redshift")
        
        query = StageToRedshiftOperator.COPY_QUERY.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            access_key_id=aws_credentials.access_key,
            secret_access_key=aws_credentials.secret_key,
            json_path_file=self.json_path_file
        )
        if self.s3_region is not None:
            query += StageToRedshiftOperator.REGION_SUFFIX.format(region=self.s3_region)
            
        redshift_hook.run(query)
        
        
        
        
        





