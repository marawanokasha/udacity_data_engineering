#!/bin/bash
sudo python3 -m pip install boto3 \
                    scikit-learn pandas  scipy numpy \
                    matplotlib seaborn \
                    IPython jupyter \
                    jupyter-contrib-nbextensions \
                    jupyter-nbextensions-configurator
                    
# for pandas to be able to read directly from S3
sudo python3 -m pip install s3fs

# for pandas to read from parquet
sudo python3 -m pip install pyarrow


sudo yum install -y htop


jupyter contrib nbextension install --user
jupyter nbextension enable --user --py widgetsnbextension

# sudo su
# jupyter nbextension enable --system --py widgetsnbextension
# jupyter contrib nbextension install --system



###############################################
# Amazon Keyspaces
#
# Downloading and adding the amazon certificate to a keystore so it could be used when connecting to keyspaces
# Check https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html for more

curl https://www.amazontrust.com/repository/AmazonRootCA1.pem -O

KEYSPACES_KEYSTORE_PASS=rUvKYVb_7CF734J_

openssl x509 -outform der -in AmazonRootCA1.pem -out amazon_cert_file.der
keytool -import -alias cassandra -noprompt -storepass $KEYSPACES_KEYSTORE_PASS -keystore cassandra_truststore.jks -file amazon_cert_file.der

cp ./cassandra_truststore.jks /home/hadoop/cassandra_truststore.jks