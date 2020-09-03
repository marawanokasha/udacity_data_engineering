#! /bin/bash

source ./.env
export AWS_PROFILE=$AWS_PROFILE

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
    
    echo "---- Importing Data Stack variables"
    RAW_DATA_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text | grep -oP "OUTPUTS\s+rawBucketName\s+\K(.*)")
    STAGING_DATA_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text | grep -oP "OUTPUTS\s+stagingBucketName\s+\K(.*)")
    UTIL_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text | grep -oP "OUTPUTS\s+utilBucketName\s+\K(.*)")

    echo "---- Importing Processing Stack variables"
    EMR_HOST=$(aws cloudformation describe-stacks --stack-name $PROCESSING_STACK_NAME --output text | grep -oP "OUTPUTS\s+emrClusterPublicDNS\s+\K(.*)")
    EMR_LOGS_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $PROCESSING_STACK_NAME --output text | grep -oP "OUTPUTS\s+emrLogsBucketName\s+\K(.*)")
    
    echo "Done"
fi