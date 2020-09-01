#!/bin/bash
sudo python3 -m pip install pandas matplotlib scipy numpy scikit-learn seaborn IPython jupyter boto3 autovizwidget hdijupyterutils jupyter-contrib-nbextensions jupyter-nbextensions-configurator ipywidgets nodejs

jupyter contrib nbextension install --user
jupyter nbextension enable --user --py widgetsnbextension

# sudo su
# jupyter nbextension enable --system --py widgetsnbextension
# jupyter contrib nbextension install --system