import json
import logging
import os
import sys

import numpy as np
import pandas as pd
from flask import Flask, request
from flask import jsonify

logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/map_data', methods=['GET'])
def get_map_data():
    """
    Get data for displaying the map values
    :return:
    """
    data = pd.read_csv("./map_data.csv")
    data = [{"id": row["country_code_iso_2"], "value": row["count"]} for row in data.to_dict('records') if not pd.isna(row["country_code_iso_2"])]
    return jsonify(data)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    # Start server
    app.run(host='0.0.0.0', port=5000, debug=True)
