from urllib.parse import urlparse
import logging
import os
import argparse
import boto3
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, FloatType, LongType

from data_manipulation import get_date_from_i94_date_int, normalize_gender, normalize_match_flag, \
    normalize_mode, normalize_visa_category


VALID_COLUMNS = [
    'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate',
    'i94bir', 'i94visa', 'count', 'dtadfile',
    'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'biryear',
    'dtaddto', 'gender', 'insnum', 'airline', 'admnum', 'fltno', 'visatype'
]


FINAL_COLUMNS = [
    "year", "month", "day",
    "arrival_date", "departure_date", "permitted_date", "unrestricted_stay",
    "country_citizenship", "country_residence", "port_name",
    "destination_state",
    "age", "gender", "num_previous_stays",
    "visa_type", "visa_category",
    "is_overstay"
]

SAS_FORMAT_READER = "com.github.saurfang.sas.spark"


def read_sas_data(spark, path: str, columns_to_read=None):

    logging.info("Reading the immigration data")

    url_components = urlparse(path)

    if url_components.scheme == 's3':
        s3_client = boto3.client('s3')

        s3_objects = s3_client.list_objects(Bucket=url_components.netloc, Prefix=url_components.path.lstrip("/"))

        df = None
        for key in s3_objects['Contents']:
            file_key = key['Key']
            print("Reading File: {}".format(file_key))
            logging.info("Reading File: {}".format(file_key))
            file_df = (
                spark.read
                .format(SAS_FORMAT_READER)
                .load("s3://{}/{}".format(url_components.netloc, file_key))
            )
            if columns_to_read:
                file_df = file_df.select(VALID_COLUMNS)
            if df:
                df = df.union(file_df)
            else:
                df = file_df
    else:
        df = (
            spark.read
            .format(SAS_FORMAT_READER)
            .load("s3://{}/{}".format(url_components.netloc, file_key))
        )
        if columns_to_read:
            df.select(columns_to_read)

    return df


def read_dimension_data(spark: SparkSession, root_path: str):

    logging.info("Reading the dimension data")

    gdp_df = spark.read.option("header", True).format("csv").load(f"{root_path}/gdp.csv")
    country_ids_df = spark.read.option("header", True).format("csv").load(f"{root_path}/country_ids.csv")
    states_df = spark.read.option("header", True).format("csv").load(f"{root_path}/states.csv")
    ports_df = spark.read.option("header", True).format("csv").load(f"{root_path}/ports.csv")

    country_ids_df_cleaned = (
        country_ids_df
        .withColumn("country_id", F.col("country_id").cast(IntegerType()))
        .withColumn("country_name", F.initcap(F.col("country_name")))  # title case
        .withColumn("country_name_lower", F.lower(F.col("country_name")))
    )
    gdp_df_cleaned = (
        gdp_df
        .withColumnRenamed("Country Name", "country_name")
        .withColumnRenamed("Country Code", "country_code")
        .withColumnRenamed("Value", "gdp_value")
        # .filter(F.col("Year") == "2016")
        .withColumn("gdp_value", F.col("gdp_value").cast(FloatType()).cast(LongType()))
        .withColumn("country_name", F.initcap(F.col("country_name")))  # title case
        .withColumn("country_name_lower", F.lower(F.col("country_name")))
        .drop("Year")
    )

    return country_ids_df_cleaned, gdp_df_cleaned, ports_df


def clean_immigration_data(raw_data):

    logging.warning("Cleaning the immigration data")

    USEFUL_COLUMNS = [
        "admnum", "i94yr", "i94mon",
        "arrdate", "depdate", "dtaddto",
        "i94cit", "i94res", "i94port", "i94addr",
        "matflag", "i94mode",
        "i94bir", "gender", "visatype", "i94visa",
        "entdepa", "entdepu", "entdepd"
    ]

    clean_df = raw_data.select(USEFUL_COLUMNS)

    clean_df = (
        clean_df
        .withColumnRenamed('i94yr', 'year')
        .withColumnRenamed('i94mon', 'month')
        .withColumnRenamed('i94res', 'country_residence_id')
        .withColumnRenamed('i94cit', 'country_citizenship_id')
        .withColumnRenamed('i94port', 'entry_port')
        .withColumnRenamed('i94addr', 'destination_state')
        .withColumnRenamed('i94mode', 'travel_mode')
        .withColumnRenamed('i94bir', 'age')
        .withColumnRenamed('i94visa', 'visa_category')
        .withColumnRenamed('visatype', 'visa_type')
        .withColumnRenamed('matflag', 'dates_match_flag')
        .withColumnRenamed('arrdate', 'arrival_date')
        .withColumnRenamed('depdate', 'departure_date')
        .withColumnRenamed('dtaddto', 'permitted_date')
    )

    same_user_window = (
        Window
        .partitionBy("admnum")
        .orderBy("arrival_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    clean_df = (
        clean_df
        .withColumn('admnum', F.col("admnum").cast(IntegerType()))
        .withColumn('year', F.col("year").cast(IntegerType()))
        .withColumn('month', F.col("month").cast(IntegerType()))
        .withColumn('unrestricted_stay', F.when(F.col('permitted_date') == 'D/S', True).otherwise(False))
        .withColumn('arrival_date', get_date_from_i94_date_int(F.col("arrival_date")))
        .withColumn('departure_date', get_date_from_i94_date_int(F.col("departure_date")))
        .withColumn('permitted_date', F.to_date('permitted_date', 'MMddyyyy'))
        .withColumn('day', F.dayofmonth("arrival_date"))
        .withColumn('age', F.col("age").cast(IntegerType()))
        .withColumn('gender', normalize_gender(F.col("gender")))
        .withColumn('dates_match_flag', normalize_match_flag(F.col("dates_match_flag")))
        .withColumn('travel_mode', normalize_mode(F.col("travel_mode")))
        .withColumn('visa_category', normalize_visa_category(F.col("visa_category").cast(IntegerType())))
        .withColumn('country_citizenship_id', F.col("country_citizenship_id").cast(IntegerType()))
        .withColumn('country_residence_id', F.col("country_residence_id").cast(IntegerType()))
        .withColumn('num_previous_stays', F.count("*").over(same_user_window))
    )

    logging.info("Adding the ML Label")
    # add the label column for the ML problem, check the Data Exploration notebook for the explanation of why
    # these conditions were chosen
    clean_df = clean_df.withColumn(
        "is_overstay",
        F.when(
            (
                (clean_df.dates_match_flag == False) &
                (clean_df.unrestricted_stay == False) &
                ((F.isnull(clean_df.departure_date)) & ((F.isnull(clean_df.entdepd)))) &
                (F.isnull(clean_df.entdepu))
            ),
            True
        ).otherwise(False)
    )
    return clean_df


def main(immigration_data_path: str, dimension_root_path: str, output_path: str):

    spark = SparkSession \
        .builder\
        .appName("create-staging-data") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.1.0-s_2.11")\
        .getOrCreate()

    print(spark.conf.get("spark.jars.packages"))
    
    raw_data = read_sas_data(spark, immigration_data_path, VALID_COLUMNS)

    clean_df = clean_immigration_data(raw_data)

    country_df, gdp_df, port_df = read_dimension_data(spark, dimension_root_path)

    joined_df = (
        clean_df
        .alias("immigration")
        .join(
            F.broadcast(country_df.alias("cit_df")),
            F.col("immigration.country_citizenship_id") == F.col("cit_df.country_id"),
            "leftouter"
        )
        .join(
            F.broadcast(country_df.alias("res_df")),
            F.col("immigration.country_residence_id") == F.col("res_df.country_id"),
            "leftouter"
        )
        .join(
            F.broadcast(port_df.alias("port_df")),
            F.col("immigration.entry_port") == F.col("port_df.port_id"),
            "leftouter"
        )
        .selectExpr("immigration.*", "cit_df.country_name as country_citizenship", "res_df.country_name as country_residence", "port_df.port_name")
    )

    joined_df = joined_df.select(FINAL_COLUMNS)

    joined_df = joined_df.repartition(8)

    logging.info(f"Writing the immigration data to {output_path}")
    (
        joined_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")  # we partition by day so that if we want to run this every day, we don't overwrite the month partition
        .parquet(os.path.join(output_path, "immigration_data"))
    )

    logging.info(f"Writing the GDP data to {output_path}")
    (
        gdp_df
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_path, "gdp_data"))
    )
    
    logging.info(f"Finished Preprocessing, exiting")

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
