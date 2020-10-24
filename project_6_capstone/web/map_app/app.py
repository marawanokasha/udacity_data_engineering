import datetime
import logging

import joblib
import pandas as pd
from flask import Flask, jsonify, render_template, request

from data import (get_cassandra_session, get_countries, get_immigration_stats,
                  get_states, get_visa_types)

logger = logging.getLogger(__name__)

app = Flask(__name__)


model = joblib.load("./ml_model/model.pkl")
pipeline = joblib.load("./ml_model/pipeline.pkl")


@app.route('/')
def index():
    session = get_cassandra_session()
    # return app.send_static_file('index.html')
    countries = sorted(get_countries(session).to_dict('records'), key=lambda x: x['country_name'])
    states = sorted(get_states(session).to_dict('records'), key=lambda x: x['state_name'])
    visa_types = sorted(get_visa_types(session).to_dict('records'), key=lambda x: x['visa_type'])

    return render_template('index.html', countries=countries, states=states, visa_types=visa_types)


@app.route('/map_data', methods=['GET'])
def get_map_data():
    """
    Get data for displaying the map values
    :return:
    """
    session = get_cassandra_session()
    data = get_immigration_stats(session)
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

    probability_overstay = y[0, 1]

    return jsonify({
        "probability": float(probability_overstay)
    })


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    # Start server
    app.run(host='0.0.0.0', port=5000, debug=True)
