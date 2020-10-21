import logging
import datetime

import pandas as pd
from flask import Flask, request
from flask import jsonify
import joblib

logger = logging.getLogger(__name__)

app = Flask(__name__)


model = joblib.load("./ml_model/model.pkl")
pipeline = joblib.load("./ml_model/pipeline.pkl")

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


@app.route('/predict_overstay', methods=['POST'])
def predict_overstay():
    """
    Get data for displaying the map values
    :return:
    """
    client_data = request.get_json()
    print(client_data)

    arrival_date = datetime.datetime.strptime(client_data['arrival_date'], '%Y-%m-%d')
    print(arrival_date)

    features = {}
    features['country_citizenship'] = client_data['country_citizenship']
    features['country_residence'] = client_data['country_residence']
    features['destination_state'] = client_data['destination_state']
    features['gender'] = client_data['gender']
    features['age'] = int(client_data['age'])
    features['visa_type'] = client_data['visa_type']
    features['num_previous_stays'] = int(client_data['num_previous_stays'])

    features['month'] = arrival_date.month
    features['country_citizenship_gdp'] = 100000
    features['country_residence_gdp'] = 100000

    inference_data = pd.DataFrame([features])

    print(inference_data)
    X = pipeline.transform(inference_data)
    print(X)
    y = model.predict_proba(X)

    probability_overstay = y[0,1]

    return jsonify({
        "probability": float(probability_overstay)
    })


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    # Start server
    app.run(host='0.0.0.0', port=5000, debug=True)
