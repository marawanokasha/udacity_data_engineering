import sys
import os
import logging

from flask import Flask, render_template, request
from PIL import Image
import numpy as np

app = Flask(__name__)
config = InferenceConfig()

UPLOADS_FOLDER = "./uploads"
app.config['UPLOAD_FOLDER'] = UPLOADS_FOLDER

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

predictor = SegmentationPredictorFactory.create_predictor(config)
serializer = ImageBase64Serializer()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handle the form post by saving the uploaded file and then sending it through the predictor class
    to get the output segmentation results

    :return:
    """
    logging.info("Received Uploaded File")
    file = request.files['image']
    f = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(f)

    img = Image.open(f)
    img = np.array(img)

    logging.info(img.shape)

    logging.info("Segmenting")
    result = predictor.get_segmentation_result(img)
    logging.info("Finished Segmentation")

    predicted_image = result["raw_prediction"]

    return render_template('index.html', init=True,
                           inimg=serializer.serialize(img),
                           result=serializer.serialize(predicted_image))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    # Start server
    app.run(host='0.0.0.0', port=7000, debug=False)
