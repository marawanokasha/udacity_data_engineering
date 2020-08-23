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
        - VPC
        - Subnet for Redshift
        - Subnet for EMR
        - EMR cluster
        - Redshift cluster
        - S3 bucket for raw data
        - S3 bucket for staging data
    ```
    AWS_REGION=<your_region>
    AWS_SSH_KEY=<your_key>

    export AWS_PROFILE=udacity
    DATA_STACK_NAME=udacity-capstone-data-stack

    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name $DATA_STACK_NAME \
        --template-file ./infra/1_data.yml \
        --tags project=udacity-capstone \
        --parameter-overrides \
        rawBucketName=raw-data \
        stagingBucketName=staging-data
        
    UTIL_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $DATA_STACK_NAME --output text | grep -oP "OUTPUTS\s+utilBucketName\s+\K(.*)")

    aws s3 cp ./infra/spark/emr_server_setup.sh s3://$UTIL_BUCKET_NAME/

    aws cloudformation deploy \
        --region $AWS_REGION \
        --stack-name udacity-capstone-processing-stack \
        --template-file ./infra/2_processing.yml \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags project=udacity-capstone \
        --parameter-overrides \
        sshKeyName=$AWS_SSH_KEY \
        resourcePrefix=udacity-capstone \
        bootstrapActionFilePath=s3://$UTIL_BUCKET_NAME/emr_server_setup.sh

    aws cloudformation describe-stacks --stack-name udacity-capstone-processing-stack > stack_output.json
    ```

1. Delete the Cloudformation stack after you are done

    ```
    aws cloudformation delete-stack --stack-name udacity-processing-capstone-stack
    
    aws cloudformation delete-stack --stack-name udacity-data-capstone-stack
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
    HOST=ec2-18-237-104-30.us-west-2.compute.amazonaws.com
    ssh -i ~/.ssh/udacity_ec2_key.pem -4 -NL 8998:$HOST:8998 hadoop@$HOST
    ```
    we use `-4` is because ipv6 is the default and it leads to an error `unable to bind to address`

    ```
    ssh -v -i ./udacity_ec2_key.pem -4 -N \
        -L 8020:$HOST:8020 \
        -L 8025:$HOST:8025 \
        -L 8032:$HOST:8032 \
        -L 8030:$HOST:8030 \
        -L 19888:$HOST:19888 \
        -L 20888:$HOST:20888 \
        hadoop@$HOST
    ```
1. Create a dynamic bridge to be able to see all internal UIs
    ```
    ssh -i ~/.ssh/udacity_ec2_key.pem -ND 8157 hadoop@$HOST
    ```

1. Edit the `core-site.xml` and `yarn-site.xml` to use localhost instead of the internal dns names
    ```
    sed -i 's|ip-10-0-1-107.us-west-2.compute.internal|localhost|g' /etc/hadoop/conf/yarn-site.xml
    sed -i 's|ip-10-0-1-107.us-west-2.compute.internal|localhost|g' /etc/hadoop/conf/yarn-site.xml
    ```

### Airflow    
1. Install Airflow
    ```
    ./infra/airflow/setup_airflow.sh
    
    ```

1. Add Airflow connections and variables
    ```

    AWS_ACCESS_KEY_ID=<YOUR ACCESS TOKEN>
    AWS_ACCESS_KEY_SECRET=<YOUR ACCESS TOKEN SECRET>
    airflow connections -a --conn_id s3 --conn_type s3 --conn_login $AWS_ACCESS_KEY_ID --conn_password $AWS_ACCESS_KEY_SECRET
    airflow variables --set raw_data_bucket udacity-capstone-raw-data
    airflow variables --set raw_files_folder /data/18-83510-I94-Data-2016
    airflow variables --set gdp_data_url https://datahub.io/core/gdp/r/gdp.csv
    airflow variables --set raw_data_description_file /home/workspace/I94_SAS_Labels_Descriptions.SAS

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