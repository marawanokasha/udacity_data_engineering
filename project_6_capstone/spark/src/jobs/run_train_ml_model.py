import logging
import argparse
from io import BytesIO

import boto3
import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (OneHotEncoder,
                                   StandardScaler)

logger = logging.getLogger(__name__)


def train_model(data_df: pd.DataFrame):
    """
    Train the model on the driver
    """

    logger.info("Starting the process for training the overstays model")

    numeric_features = [
        'country_citizenship_gdp',
        'country_residence_gdp',
        'num_previous_stays'
    ]
    categorical_features = [
        'country_citizenship',
        'country_residence',
        'destination_state',
        'gender',
        'visa_type',
    ]

    ordinal_features = [
        'age',
        'month'
    ]

    all_features = numeric_features + categorical_features + ordinal_features

    preprocessing_pipeline = ColumnTransformer([
        (
            "numerical_features",
            Pipeline(steps=[
                 # most of the countries that don't have a GDP are small countries, so 0 is a more logical choice
                ('imp', SimpleImputer(strategy='constant', fill_value=0)),
                ('sts', StandardScaler()),
            ]),
            numeric_features
        ),
        (
            "ordinal_features",
            Pipeline(steps=[
                ('imp', SimpleImputer(strategy='constant', fill_value=0)),
            ]),
            ordinal_features
        ),
        (
            "categorical_features",
            Pipeline(steps=[
                # we use missing_values=None because that's the representation we get when reading from parquet, not np.nan
                ('imp', SimpleImputer(strategy='constant', fill_value='MISSING', missing_values=None)),
                ('one_hot_encoder', OneHotEncoder(sparse=True, dtype=int)),
            ]),
            categorical_features
        )
    ])

    logger.info("Fitting the pipeline and transforming the data")

    TARGET_COLUMN = 'is_overstay'
    X = preprocessing_pipeline.fit_transform(data_df[all_features])
    Y = data_df[TARGET_COLUMN].values

    logger.info("Ended the data transformation")
    param_grid = {
        "n_estimators": [10],
        "max_depth": [20],
        "max_features": ["sqrt"],
        # "min_samples_split": [2, 3, 10],
        "min_samples_leaf": [100],
        # "bootstrap"        : [True, False],
        "criterion": ["gini"],
        # "class_weight"        : ["balanced", None],
    }

    logger.info("Fitting the model....")

    clf = RandomForestClassifier(n_jobs=5, verbose=2)
    gs = GridSearchCV(clf, param_grid=param_grid, n_jobs=2, scoring='roc_auc', verbose=2)
    gs.fit(X, Y)

    logger.info("CV Results")
    logger.info(gs.cv_results_)
    
    logger.info("Best Params")
    logger.info(gs.best_params_)

    logger.info("Best Score")
    logger.info(gs.best_score_)

    logger.info("Finished fitting the model")

    return gs.best_estimator_, preprocessing_pipeline


def save_to_s3(obj, bucket_name, key_name):
    """
    Save the object to S3
    """

    fb = BytesIO()
    joblib.dump(obj, fb)
    fb.seek(0)

    s3_client = boto3.client('s3')
    s3_client.upload_fileobj(fb, bucket_name, key_name)


def main(ml_data_path: str, output_bucket: str, output_prefix: str):

    logger.info(f"Reading the ML Data from {ml_data_path}")

    data_df = pd.read_parquet(ml_data_path)
    # for CSV, if you want to read from a folder, the folder needs to have a single file and you need to use *
    # data_df = pd.read_csv("{}/*".format(ml_data_path))

    model, preprocessing_pipeline = train_model(data_df)

    logger.info("Saving the model and pipeline to S3")

    save_to_s3(model, output_bucket, "{}/model.pkl".format(output_prefix))
    save_to_s3(preprocessing_pipeline, output_bucket, "{}/pipeline.pkl".format(output_prefix))

    logger.info("Process finished")


if __name__ == "__main__":

    logging.basicConfig(
        format='[%(asctime)s] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", required=True, type=str, help="Path containing the ML data")
    parser.add_argument("--output-bucket", required=True, type=str, help="Bucket to save the model artifacts in")
    parser.add_argument("--output-prefix", required=True, default='', type=str, help="Prefix in the S3 bucket to save the artifacts in")
    args = parser.parse_args()

    main(args.data_path, args.output_bucket, args.output_prefix)
