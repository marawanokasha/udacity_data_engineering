class DAGVariables:
    RAW_BUCKET_NAME_VARIABLE = "raw_data_bucket"
    STAGING_BUCKET_NAME_VARIABLE = "staging_data_bucket"
    SCRIPT_BUCKET_NAME_VARIABLE = "script_bucket"


class Connections:
    S3_CONNECTION_ID = "s3"
    AWS_CONNECTION_ID = "aws"
    LIVY_CONNECTION_ID = "livy"
