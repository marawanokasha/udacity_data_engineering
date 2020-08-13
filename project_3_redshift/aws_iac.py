import logging
import json
import boto3
import botocore


logger = logging.getLogger(__name__)


def iam_role_exists(
    iam: boto3.resources.base.ServiceResource, 
    role_name: str):
    """
    Check if a role already exists
    """
    
    role = iam.Role(role_name)
    try:
        role_arn = role.arn
        return True
    except:
        return False

    
def delete_iam_role(
    iam: boto3.resources.base.ServiceResource, 
    role_name: str):
    """
    Delete a role and its policies
    """
    
    logger.info(f"Deleting the role {role_name} and its attached policies")
    role = iam.Role(role_name)
    for policy in role.attached_policies.all():
        role.detach_policy(PolicyArn=policy.arn)
    role.delete()

    
def create_iam_role(
    iam: botocore.client.BaseClient, 
    role_name: str, service: str, policy_arn: str):
    """
    Create a role for the redshift cluster to be able to access the S3 bucket
    """

    logger.info(f"Creating Role {role_name} with {policy_arn} policy")
    dwh_role = iam.create_role(
        RoleName=role_name, 
        AssumeRolePolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    # principal is who is allowed to impersonate this role
                    "Service": service
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }))
    
    dwh_role.attach_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )
    role_arn = dwh_role.arn
    
    return role_arn

    
def redshift_cluster_exists(
    redshift: botocore.client.BaseClient, 
    dwh_cluster_identifier: str):
    """
    Check if a redshift cluster already exists
    """
    
    try:
        get_redshift_cluster_endpoint(redshift, dwh_cluster_identifier)
        return True
    except:
        return False

    
    
def create_redshift_cluster(
    redshift: botocore.client.BaseClient, role_arn: str, 
    dwh_cluster_type: str,
    dwh_node_type: str, dwh_num_nodes: int,
    dwh_cluster_identifier: str, 
    dwh_db: str, dwh_port: int,
    dwh_user: str, dwh_password: str):
    """
    Create the redshift cluster for conducting ETL
    """

    logger.info(f"Creating Redshift Cluster {dwh_cluster_identifier} with default DB {dwh_db}")
    
    response = redshift.create_cluster(
        ClusterType=dwh_cluster_type,
        NodeType=dwh_node_type,
        NumberOfNodes=dwh_num_nodes,
        Port=dwh_port,

        DBName=dwh_db,
        ClusterIdentifier=dwh_cluster_identifier,
        MasterUsername=dwh_user,
        MasterUserPassword=dwh_password,

        IamRoles=[role_arn]
    )
    
    logger.info(f"Waiting for the cluster to be created....")
    
    # use a waiter to hang the code until the cluster is commissioned
    waiter = redshift.get_waiter('cluster_available')
    waiter.wait(ClusterIdentifier=dwh_cluster_identifier)
    
    logger.info(f"Cluster created successfully")
    
    return get_redshift_cluster_endpoint(redshift, dwh_cluster_identifier)

    
def delete_redshift_cluster(
    redshift: botocore.client.BaseClient, 
    dwh_cluster_identifier: str):
    """
    Delete a role and its policies
    """
    
    logger.info(f"Deleting the redshift cluster {dwh_cluster_identifier}")
    redshift.delete_cluster(
        ClusterIdentifier=dwh_cluster_identifier,  
        SkipFinalClusterSnapshot=True)
    
    logger.info(f"Waiting for the cluster to be deleted....")
    
    waiter = redshift.get_waiter('cluster_deleted')
    waiter.wait(ClusterIdentifier=dwh_cluster_identifier)
    
    logger.info(f"Cluster deleted successfully")
    
    return


def get_redshift_cluster_endpoint(redshift, dwh_cluster_identifier: str):
    """
    Get the public endpoint for the redshift cluster
    """
    
    cluster_properties = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
    return cluster_properties['Endpoint']['Address']