import json
import boto3
import botocore
import logging
import configparser
from aws_iac import iam_role_exists, delete_iam_role, create_iam_role, \
                    create_redshift_cluster, redshift_cluster_exists, \
                    get_redshift_cluster_endpoint


logger = logging.getLogger(__name__)

    
def main():
    aws_config = configparser.ConfigParser()
    aws_config.read('aws.cfg')
    KEY=aws_config.get('AWS','key')
    SECRET= aws_config.get('AWS','secret')
    
    dwh_config = configparser.ConfigParser()
    dwh_config.read('dwh.cfg')
    DWH_ROLE_NAME = dwh_config.get("IAM_ROLE", "ROLE_NAME")
    
    REGION                 = dwh_config.get("CLUSTER_CONFIG", "REGION")
    DWH_CLUSTER_TYPE       = dwh_config.get("CLUSTER_CONFIG","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = dwh_config.get("CLUSTER_CONFIG","DWH_NUM_NODES")
    DWH_NODE_TYPE          = dwh_config.get("CLUSTER_CONFIG","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = dwh_config.get("CLUSTER_CONFIG","DWH_CLUSTER_IDENTIFIER")
    
    DWH_DB                 = dwh_config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = dwh_config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = dwh_config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = dwh_config.get("CLUSTER","DB_PORT")

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
    
    # roles are lightweight objects to create/destroy, so if the role 
    # already exists, delete it and recreate it to make sure that the policies
    # are correct
    if iam_role_exists(iam, DWH_ROLE_NAME):
        logger.info(f"Role {DWH_ROLE_NAME} already exists")
        delete_iam_role(iam, DWH_ROLE_NAME)
    else:
        logger.info(f"Role {DWH_ROLE_NAME} doesn't exist")
    role_arn = create_iam_role(
        iam, DWH_ROLE_NAME,
        "redshift.amazonaws.com",
        'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )
    
    # Redshift clusters are not that fast to create and we don't want to lose
    # whatever data we have in an existing cluster, so if a cluster exists, 
    # we'll just use that
    if redshift_cluster_exists(redshift, DWH_CLUSTER_IDENTIFIER):
        logger.info(f"Redshift Cluster {DWH_CLUSTER_IDENTIFIER} already exists, using that one")
        redshift_host = get_redshift_cluster_endpoint(redshift, DWH_CLUSTER_IDENTIFIER)
    else:
        redshift_host = create_redshift_cluster(
            redshift, role_arn,
            DWH_CLUSTER_TYPE, DWH_NODE_TYPE,
            int(DWH_NUM_NODES),
            DWH_CLUSTER_IDENTIFIER, 
            DWH_DB, int(DWH_PORT),
            DWH_DB_USER, DWH_DB_PASSWORD
        )
    
    dwh_config.set('CLUSTER', 'HOST', redshift_host)
    dwh_config.set('IAM_ROLE', 'ARN', role_arn)
    
    # write down the updated config to the config file so we can use it in later steps
    with open('dwh.cfg', 'w') as configfile:
        dwh_config.write(configfile)

    
if __name__ == "__main__":
    logging.basicConfig(
        format='[ %(asctime)s ] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )
    main()