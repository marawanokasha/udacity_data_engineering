"""
Spark script for processing the raw data and writing the staging data to be used by both the ML model 
training and the export to Redshift and Cassandra
"""
import argparse
import logging
import os

from ml_data_processing import create_ml_data
from processing import (clean_immigration_data,
                        create_staging_immigration_data, create_visa_type_data,
                        read_dimension_data, read_sas_data)
from pyspark.sql import SparkSession

VALID_COLUMNS = [
    'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate',
    'i94mode', 'i94addr', 'depdate',
    'i94bir', 'i94visa', 'count', 'dtadfile',
    'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'biryear',
    'dtaddto', 'gender', 'insnum', 'airline', 'admnum', 'fltno', 'visatype'
]


FINAL_COLUMNS = [
    "admnum", "year", "month", "day",
    "arrival_date", "departure_date", "permitted_date", "unrestricted_stay",
    "country_citizenship", "country_residence", "port_name",
    "destination_state",
    "age", "gender", "num_previous_stays",
    "visa_type", "visa_category",
    "is_overstay"
]


SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:2.7.0",
    "saurfang:spark-sas7bdat:2.1.0-s_2.11"
])


logger = logging.getLogger(__name__)


def main(immigration_data_path: str, dimension_root_path: str, output_path: str):

    spark = SparkSession \
        .builder\
        .appName("create-staging-data") \
        .config("spark.jars.packages", SPARK_PACKAGES)\
        .getOrCreate()

    # if we are not using an EMR cluster with a role that has access to the S3 bucket, we need to set the AWS credentials
    # so Spark is able to access the data in the S3 bucket
    # spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    # spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])

    raw_data = read_sas_data(spark, immigration_data_path, columns_to_read=VALID_COLUMNS)

    clean_df = clean_immigration_data(raw_data)
    country_ids_df, country_iso_df, gdp_df, states_df, port_df = read_dimension_data(spark, dimension_root_path)
    joined_df = create_staging_immigration_data(clean_df, country_ids_df, country_iso_df, states_df, port_df, final_columns=FINAL_COLUMNS)
    ml_data_df = create_ml_data(joined_df, gdp_df)
    visa_type_df = create_visa_type_data(joined_df)

    joined_df = joined_df.repartition(20)

    logger.info(f"Writing the immigration data to {output_path}")
    (
        joined_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")  # we partition by day so that if we want to run this every day, we don't overwrite the month partition
        .parquet(os.path.join(output_path, "immigration_data"))
    )

    logger.info(f"Writing the Country data to {output_path}")
    (
        country_iso_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_path, "country_data"))
    )

    logger.info(f"Writing the State data to {output_path}")
    (
        states_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_path, "state_data"))
    )

    logger.info(f"Writing the GDP data to {output_path}")
    (
        gdp_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_path, "gdp_data"))
    )

    logger.info(f"Writing the Visa type data to {output_path}")
    (
        visa_type_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_path, "visa_type_data"))
    )

    logger.info(f"Writing the ML data to {output_path}")
    (
        ml_data_df
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_path, "ml_data"))
    )

    # if you want to write the ML data to a CSV file instead
    # (
    #     ml_data_df
    #     .coalesce(1)
    #     .write
    #     .mode("overwrite")
    #     .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") # Avoid creation of _SUCCESS files
    #     .option("header","true")
    #     .csv(os.path.join(STAGING_BUCKET_URL, "ml_data_csv"))
    # )

    logger.info(f"Finished Preprocessing, exiting")

    spark.stop()


if __name__ == "__main__":

    logging.basicConfig(
        format='[%(asctime)s] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-data-path", required=True, type=str, help="Path containing the raw data")
    parser.add_argument("--staging-data-path", required=True, type=str, help="Path to put the staging data in")
    args = parser.parse_args()

    immigration_data_path = f"{args.raw_data_path}/i94-data"

    main(immigration_data_path, args.raw_data_path, args.staging_data_path)
