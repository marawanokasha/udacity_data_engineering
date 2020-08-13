import json
import boto3
import botocore
import logging
import configparser
from aws_iac import iam_role_exists, delete_iam_role, \
                    redshift_cluster_exists, delete_redshift_cluster


logger = logging.getLogger(__name__)

    
def main():
    aws_config = configparser.ConfigParser()
    aws_config.read('aws.cfg')
    KEY=aws_config.get('AWS','key')
    SECRET= aws_config.get('AWS','secret')
    
    dwh_config = configparser.ConfigParser()
    dwh_config.read('dwh.cfg')
    REGION = dwh_config.get("CLUSTER_CONFIG", "REGION")
    DWH_ROLE_NAME = dwh_config.get("IAM_ROLE", "ROLE_NAME")
    
    DWH_CLUSTER_IDENTIFIER = dwh_config.get("CLUSTER_CONFIG","DWH_CLUSTER_IDENTIFIER")

    iam = boto3.resource(
        'iam',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    redshift = boto3.client(
        'redshift', 
        REGION,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    
    logger.info("Deleting infrastructure created for the analysis")
    
    if iam_role_exists(iam, DWH_ROLE_NAME):
        delete_iam_role(iam, DWH_ROLE_NAME)
    if redshift_cluster_exists(redshift, DWH_CLUSTER_IDENTIFIER):
        delete_redshift_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
    
    logger.info("Infrastructure deleted successfully")
    
    
if __name__ == "__main__":
    logging.basicConfig(
        format='[ %(asctime)s ] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )
    main()