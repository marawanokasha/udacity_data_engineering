"""
Spark script for copying data to S3 as a staging location for Redshift
"""
import argparse
import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


IMMIGRATION_TABLE = "immigration"
COUNTRY_TABLE = "country"
STATE_TABLE = "state"
VISA_TYPE_TABLE = "visa_type"
GDP_TABLE = "gdp"


def main(
        immigration_data_path: str, country_data_path: str, states_data_path: str,
        visa_type_data_path: str, gdp_data_path: str, output_path: str):

    spark = SparkSession \
        .builder\
        .appName("copy-for-redshift") \
        .getOrCreate()

    immigration_data = (
        spark
        .read
        .parquet(immigration_data_path)
    )
    country_data = (
        spark
        .read
        .parquet(country_data_path)
    )
    state_data = (
        spark
        .read
        .parquet(states_data_path)
    )
    visa_type_data = (
        spark
        .read
        .parquet(visa_type_data_path)
    )
    gdp_data = (
        spark
        .read
        .parquet(gdp_data_path)
    )

    logger.info(f"Writing the JSON staging data for redshift to {output_path}")
    (
        immigration_data
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")  # we partition by day so that if we want to run this every day, we don't overwrite the month partition
        .json(os.path.join(output_path, IMMIGRATION_TABLE))
    )

    (
        country_data
        .write
        .mode("overwrite")
        .json(os.path.join(output_path, COUNTRY_TABLE))
    )

    (
        state_data
        .write
        .mode("overwrite")
        .json(os.path.join(output_path, STATE_TABLE))
    )

    (
        visa_type_data
        .write
        .mode("overwrite")
        .json(os.path.join(output_path, VISA_TYPE_TABLE))
    )

    (
        gdp_data
        .write
        .mode("overwrite")
        .json(os.path.join(output_path, GDP_TABLE))
    )

    logger.info(f"Finished S3 Copy for Redshift, exiting")

    spark.stop()


if __name__ == "__main__":

    logging.basicConfig(
        format='[%(asctime)s] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-data-path", required=True, type=str, help="Path to put the staging data in")
    args = parser.parse_args()

    immigration_data_path = f"{args.staging_data_path}/immigration_data"
    country_data_path = f"{args.staging_data_path}/country_data"
    state_data_path = f"{args.staging_data_path}/state_data"
    visa_type_data_path = f"{args.staging_data_path}/visa_type_data"
    gdp_data_path = f"{args.staging_data_path}/gdp_data"

    output_data_path = f"{args.staging_data_path}/redshift_staging"

    main(immigration_data_path, country_data_path, state_data_path, visa_type_data_path, gdp_data_path, output_data_path)
