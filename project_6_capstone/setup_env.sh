#! /bin/bash

echo "---- Importing variables from the .env file..."
source ./.env
# export AWS_PROFILE=$AWS_PROFILE
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_REGION # need this one for CLI commands, overrides the value defined in the profile (which we don't use)
export AWS_REGION=$AWS_REGION

# Whether to also export the variables that are looked up from the cloud formation stacks
GET_STACK_VARS=1

for arg in "$@"
do
    case $arg in
        --no-cf)
        GET_STACK_VARS=0
        shift # Remove --initialize from processing
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        shift # Remove generic argument from processing
        ;;
    esac
done


if [[ "$GET_STACK_VARS" -eq 1 ]]; then
    echo "Importing variables from cloudformation..."
    
    if output=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text); then
        echo "---- Importing Data Stack variables"
        RAW_DATA_BUCKET_NAME=$(echo $output | grep -oP "OUTPUTS\s+rawBucketName\s+\K(.*)")
        STAGING_DATA_BUCKET_NAME=$(echo $output | grep -oP "OUTPUTS\s+stagingBucketName\s+\K(.*)")
        UTIL_BUCKET_NAME=$(echo $output | grep -oP "OUTPUTS\s+utilBucketName\s+\K(.*)")
    else
        echo "---- Data Stack not found, skipping" 
    fi

    if output=$(aws cloudformation describe-stacks --stack-name $PROCESSING_STACK_NAME --output text); then
        echo "---- Importing Processing Stack variables"
        EMR_HOST=$(echo $output | grep -oP "OUTPUTS\s+emrClusterPublicDNS\s+\K(.*)")
        EMR_LOGS_BUCKET_NAME=$(echo $output | grep -oP "OUTPUTS\s+emrLogsBucketName\s+\K(.*)")
    else
        echo "---- Processing Stack not found, skipping" 
    fi

    if output=$(aws cloudformation describe-stacks --stack-name $STORAGE_STACK_NAME --output text); then
        echo "---- Importing Storage Stack variables"
        REDSHIFT_HOST=$(echo $output | grep -oP "OUTPUTS\s+redshiftClusterPublicDNS\s+\K(.*)")
        REDSHIFT_PORT=$(echo $output | grep -oP "OUTPUTS\s+redshiftClusterPublicPort\s+\K(.*)")
    else
        echo "---- Storage Stack not found, skipping" 
    fi

    if output=$(aws cloudformation describe-stacks --stack-name $CASSANDRA_STACK_NAME --output text); then
        echo "---- Importing Cassandra Stack variables"
        CASSANDRA_HOST=$(echo $output | grep -oP "OUTPUTS[\s\w]+cassandraPublicIpAddress\s+\K(.*)")
    else
        echo "---- Cassandra Stack not found, skipping" 
    fi
    
    echo "Done"
fi