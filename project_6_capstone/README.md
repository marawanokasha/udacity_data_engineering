## Installation

### Infrastructure

1. Install the AWS CLI
    ```
    curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
    unzip awscli-bundle.zip
    ./awscli-bundle/install -b ~/bin/aws
    ```
1. Configure AWS CLI
    ```
    aws configure --profile udacity
    ```

1. Create the Infrastructure needed with cloudformation

    - From the AWS console, choose whichever region where you want to deploy the infrastrucutre in, go to EC2 -> Key Pairs and create a new key pair, ex. `udacity_ec2_key`, this is the SSH Key that will be used for the Spark and Redshift clusters

    - create the cloudformation stack. This will create the following resources:
        - S3 bucket for raw data
        - S3 bucket for staging data
        - VPC
        - Subnet for Redshift
        - Subnet for EMR
        - EMR cluster
        - Redshift cluster
    ```
    AWS_REGION=<your_region>
    AWS_SSH_KEY=<your_key>

    export AWS_PROFILE=udacity
    DATA_STACK_NAME=udacity-capstone-data-stack

    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $DATA_STACK_NAME \
        --template-file ./infra/aws/1_data.yml \
        --tags project=udacity-capstone \
        --parameter-overrides \
        rawBucketName=raw-data \
        stagingBucketName=staging-data
        
    UTIL_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text | grep -oP "OUTPUTS\s+utilBucketName\s+\K(.*)")

    # copy the EMR server initialization script to a bucket where EMR can download it from during bootstrapping
    aws s3 cp ./infra/spark/emr_server_setup.sh s3://$UTIL_BUCKET_NAME/

    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name udacity-capstone-processing-stack \
        --template-file ./infra/aws/2_processing.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags project=udacity-capstone \
        --parameter-overrides \
        sshKeyName=$AWS_SSH_KEY \
        resourcePrefix=udacity-capstone \
        bootstrapActionFilePath=s3://$UTIL_BUCKET_NAME/emr_server_setup.sh

    ```

1. Delete the Cloudformation stack after you are done

    ```
    aws cloudformation delete-stack --stack-name udacity-processing-capstone-stack
    
    # get the name of the emr logs bucket because we have to explicitly delete it since cloudformation can't delete a non-empty bucket
    EMR_LOGS_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name udacity-processing-capstone-stack --output text | grep -oP "OUTPUTS\s+emrLogsBucketName\s+\K(.*)")
    
    aws cloudformation delete-stack --stack-name udacity-data-capstone-stack
    # remove the bucket forcibly
    aws s3 rb --force s3://$EMR_LOGS_BUCKET_NAME
    ```
### Spark, Livy and Sparkmagic

1. Install Requirements
    ```
    sudo apt-get install openssh-client
    ```

1. Install Spark Magic
    ```
    pip install sparkmagic
    # enable the jupyter widgets
    jupyter nbextension enable --py --sys-prefix widgetsnbextension
    jupyter labextension install "@jupyter-widgets/jupyterlab-manager"
    # install the pyspark kernel so you don't have to use %%spark to execute all spark code
    cd $(pip show sparkmagic | grep Location: | cut -c 11-)
    jupyter-kernelspec install sparkmagic/kernels/pysparkkernel
    jupyter serverextension enable --py sparkmagic
    ```


    Local Install
    ```
    conda install -c conda-forge sparkmagic
    conda install -c conda-forge jupyter_nbextensions_configurator

    C:

    jupyter-kernelspec install sparkmagic/kernels/pysparkkernel
    jupyter serverextension enable --py sparkmagic
    ```
    
1. Create a bridge connection to the livy server
    ```
    HOST=<dns name of your EMR master>
    ssh -i ~/.ssh/udacity_ec2_key.pem -4 -NL 8998:$HOST:8998 hadoop@$HOST
    ```
    we use `-4` is because ipv6 is the default and it leads to an error `unable to bind to address`

1. Create a dynamic bridge to be able to see all internal UIs
    ```
    ssh -i ~/.ssh/udacity_ec2_key.pem -ND 8157 hadoop@$HOST
    ```
    
1. Upload the Spark Jobs and libraries to S3 so they can be used by the Spark jobs
    ```
    cd ./spark/src/lib && zip -FS -r ../../dist/lib.zip . && cd -
    aws s3 cp --recursive ./spark/src/jobs s3://$UTIL_BUCKET_NAME/spark/jobs/
    aws s3 cp ./spark/dist/lib.zip s3://$UTIL_BUCKET_NAME/spark/
    ```

### Airflow    
1. Install Airflow
    ```
    ./infra/airflow/setup_airflow.sh
    
    ```

1. Add Airflow connections and variables
    ```

    airflow connections -a --conn_id s3 --conn_type s3 --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY
    airflow connections -a --conn_id aws --conn_type aws --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_SECRET_ACCESS_KEY --conn_extra "{\"region_name\": \"$AWS_REGION\"}"
    airflow connections -a --conn_id livy --conn_type http --conn_host localhost --conn_port 8998
    
    airflow variables --set raw_data_bucket udacity-capstone-raw-data
    airflow variables --set staging_data_bucket udacity-capstone-staging-data
    airflow variables --set script_bucket udacity-capstone-util-bucket

    airflow variables --set raw_files_folder /data/18-83510-I94-Data-2016
    airflow variables --set gdp_data_url https://datahub.io/core/gdp/r/gdp.csv
    airflow variables --set raw_data_description_file /home/workspace/I94_SAS_Labels_Descriptions.SAS

    airflow variables --set emr_cluster_name udacity-capstone-emr-cluster

    ```

1. Start Airflow
    ```
    airflow scheduler -D & airflow webserver -D
    ```

1. Stop Airflow
    ```
    cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill & rm $AIRFLOW_HOME/airflow-scheduler.pid
    cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill & rm $AIRFLOW_HOME/airflow-webserver.pid $AIRFLOW_HOME/airflow-webserver-monitor.pid
    ```

Data checks:
- Date added to the files can't be > date arrived


Scope:
- Web application for displaying people arriving to the US on a monthly basis