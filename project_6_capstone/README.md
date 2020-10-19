## Installation

### Configuration

All the different elements in this project rely on a unified set of environment variables, that should be placed in the file `.env` at the root. Check the file `.env.template` for the values needed. In order to source the environment and prepare the environment variables, run:
```
source ./setup_env.sh
```

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

    source ./setup_env.sh --no-cf

    # networking infrastructure
    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $NETWORKING_STACK_NAME \
        --template-file ./infra/aws/0_networking.yml \
        --tags project=udacity-capstone

    # data infrastructure
    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $DATA_STACK_NAME \
        --template-file ./infra/aws/1_data.yml \
        --tags project=udacity-capstone \
        --parameter-overrides \
        rawBucketName=raw-data \
        stagingBucketName=staging-data

    UTIL_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text | grep -oP "OUTPUTS\s+utilBucketName\s+\K(.*)")

    # copy the EMR server initialization script to a bucket where EMR can download it during bootstrapping
    aws s3 cp ./infra/spark/emr_server_setup.sh s3://$UTIL_BUCKET_NAME/

    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $PROCESSING_STACK_NAME \
        --template-file ./infra/aws/2_processing.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags project=udacity-capstone \
        --parameter-overrides \
        resourcePrefix=udacity-capstone \
        sshKeyName=$AWS_SSH_KEY \
        bootstrapActionFilePath=s3://$UTIL_BUCKET_NAME/emr_server_setup.sh

    
    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $STORAGE_STACK_NAME \
        --template-file ./infra/aws/3_storage.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags project=udacity-capstone \
        --parameter-overrides \
        resourcePrefix=udacity-capstone \
        databaseName=$REDSHIFT_DB_NAME \
        masterUsername=$REDSHIFT_MASTER_USERNAME \
        masterPassword=$REDSHIFT_MASTER_PASSWORD

    
    # cassandra infrastructure
    # agree to the terms in https://aws.amazon.com/marketplace/pp/prodview-7l2i3ngqrah46 to get access to the cassandra IAM
    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $CASSANDRA_STACK_NAME \
        --template-file ./infra/aws/4_cassandra.yml \
        --tags project=udacity-capstone \
        --parameter-overrides \
        sshKeyName=$AWS_SSH_KEY

    ```

1. Delete the Cloudformation stack after you are done

    ```
    aws cloudformation delete-stack --stack-name $STORAGE_STACK_NAME
    aws cloudformation delete-stack --stack-name $PROCESSING_STACK_NAME
    
    # get the name of the emr logs bucket because we have to explicitly delete it since cloudformation can't delete a non-empty bucket
    EMR_LOGS_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $PROCESSING_STACK_NAME --output text | grep -oP "OUTPUTS\s+emrLogsBucketName\s+\K(.*)")
    
    aws cloudformation delete-stack --stack-name $DATA_STACK_NAME
    # remove the bucket forcibly
    aws s3 rb --force s3://$EMR_LOGS_BUCKET_NAME
    ```

### Spark, Livy and Sparkmagic

1. Install Requirements
    ```
    sudo apt-get install openssh-client
    ```

1. Install Spark Magic

    - Linux installation:
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


    - Windows installation:
    ```
    conda install -c conda-forge sparkmagic
    conda install -c conda-forge jupyter_nbextensions_configurator

    # go to the sparkmagic location as given by pip show sparkmagic
    cd C:/<spark magic location>

    jupyter-kernelspec install sparkmagic/kernels/pysparkkernel
    jupyter serverextension enable --py sparkmagic
    ```
    
1. Create a bridge connection to the livy server
    ```
    ssh -i ~/.ssh/udacity_ec2_key.pem -4 -NL 8998:$EMR_HOST:8998 hadoop@$EMR_HOST
    ```
    we use `-4` is because ipv6 is the default and it leads to an error `unable to bind to address`

1. Create a dynamic bridge to be able to see all internal UIs
    ```
    ssh -i ~/.ssh/udacity_ec2_key.pem -ND 8157 hadoop@$EMR_HOST
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
1. Prepare the environment
    ```
    source ./setup_env.sh
    ```

1. Add Airflow connections and variables
    ```
    source ./infra/airflow/init_airflow.sh
    ```

1. Start Airflow
    ```
    airflow scheduler -D & airflow webserver -D
    ```

1. Start tunnel for Livy
    ```
    # install the SSH client if needed
    sudo apt-get install -y openssh-client
    ssh -i ~/.ssh/udacity_ec2_key.pem -4 -NL 8998:$EMR_HOST:8998 hadoop@$EMR_HOST
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