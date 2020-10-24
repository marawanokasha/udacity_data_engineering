"""
Spark script for copying data to S3 as a staging location for Redshift
"""
import argparse
import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


IMMIGRATION_STATS_TABLE = "immigration_stats"
COUNTRY_TABLE = "country"
STATE_TABLE = "state"
VISA_TYPE_TABLE = "visa_type"
GDP_TABLE = "gdp"


CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"


def main(immigration_data_path: str, country_data_path: str, states_data_path: str,
         visa_type_data_path: str, gdp_data_path: str,
         cassandra_host: str, cassandra_port: str, cassandra_keyspace: str):

    spark = SparkSession \
        .builder\
        .appName("copy-to-cassandra") \
        .getOrCreate()

    spark.conf.set("spark.cassandra.connection.host", cassandra_host)
    spark.conf.set("spark.cassandra.connection.port", cassandra_port)

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

    # only keep the fields we need for the query we'll be issuing
    immigration_stats_data = (
        immigration_data
        # inner join to remove the entries without a country which we can't show on the map
        .join(country_data, immigration_data.country_citizenship == country_data.country_code, how="inner")
        .select("country_code_iso_2", "year", "month", "day")
        .groupBy("country_code_iso_2", "year", "month", "day")
        .count()
    )

    logger.info(f"Writing data to cassandra host: {cassandra_host}")
    (
        immigration_stats_data
        .write
        .format(CASSANDRA_FORMAT)
        .option("keyspace", cassandra_keyspace)
        .option("table", IMMIGRATION_STATS_TABLE)
        .mode("append")
        .save()
    )

    (
        country_data
        .write
        .format(CASSANDRA_FORMAT)
        .option("keyspace", cassandra_keyspace)
        .option("table", COUNTRY_TABLE)
        .mode("append")
        .save()
    )

    (
        state_data
        .write
        .format(CASSANDRA_FORMAT)
        .option("keyspace", cassandra_keyspace)
        .option("table", STATE_TABLE)
        .mode("append")
        .save()
    )

    (
        visa_type_data
        .write
        .format(CASSANDRA_FORMAT)
        .option("keyspace", cassandra_keyspace)
        .option("table", VISA_TYPE_TABLE)
        .mode("append")
        .save()
    )

    (
        gdp_data_path
        .write
        .format(CASSANDRA_FORMAT)
        .option("keyspace", cassandra_keyspace)
        .option("table", GDP_TABLE)
        .mode("append")
        .save()
    )

    logger.info("Finished Copy to Cassandra, exiting")

    spark.stop()


if __name__ == "__main__":

    logging.basicConfig(
        format='[%(asctime)s] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-data-path", required=True, type=str, help="Path to put the staging data in")
    parser.add_argument("--cassandra-host", required=True, type=str, help="Cassandra host")
    parser.add_argument("--cassandra-port", required=True, type=str, help="Port for the Cassandra host")
    parser.add_argument("--cassandra-keyspace", required=True, type=str, help="Keyspace in Cassandra to save the data in")
    args = parser.parse_args()

    immigration_data_path = f"{args.staging_data_path}/immigration_data"
    country_data_path = f"{args.staging_data_path}/country_data"
    state_data_path = f"{args.staging_data_path}/state_data"
    visa_type_data_path = f"{args.staging_data_path}/visa_type_data"
    gdp_data_path = f"{args.staging_data_path}/gdp_data"

    cassandra_host = args.cassandra_host
    cassandra_port = args.cassandra_port
    cassandra_keyspace = args.cassandra_keyspace

    main(immigration_data_path, country_data_path, state_data_path, visa_type_data_path, gdp_data_path,
         cassandra_host, cassandra_port, cassandra_keyspace)
