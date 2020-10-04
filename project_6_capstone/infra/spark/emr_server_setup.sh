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