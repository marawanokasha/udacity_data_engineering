from urllib.parse import urlparse
import logging
import boto3
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, FloatType, LongType

from feature_utils import get_date_from_i94_date_int, normalize_gender, normalize_match_flag, \
    normalize_mode, normalize_visa_category


SAS_FORMAT_READER = "com.github.saurfang.sas.spark"


logger = logging.getLogger(__name__)


def read_sas_data(spark, path: str, columns_to_read=None):
    """
    Read the original immigration data stored in SAS format
    """

    logger.info("Reading the immigration data")

    url_components = urlparse(path)

    if url_components.scheme == 's3':
        s3_client = boto3.client('s3')

        s3_objects = s3_client.list_objects(Bucket=url_components.netloc, Prefix=url_components.path.lstrip("/"))

        df = None
        for key in s3_objects['Contents']:
            file_key = key['Key']
            # print("Reading File: {}".format(file_key))
            logger.info("Reading File: {}".format(file_key))
            file_df = (
                spark.read
                .format(SAS_FORMAT_READER)
                .load("s3://{}/{}".format(url_components.netloc, file_key))
            )
            if columns_to_read:
                file_df = file_df.select(columns_to_read)
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
    """
    Read the dimension data for the immigration data from the root path
    """
    logger.info("Reading the dimension data")

    gdp_df = spark.read.option("header", True).format("csv").load(f"{root_path}/gdp.csv")
    country_ids_df = spark.read.option("header", True).format("csv").load(f"{root_path}/country_ids.csv")
    country_iso_df = spark.read.option("header", True).format("csv").load(f"{root_path}/country_iso.csv")
    states_df = spark.read.option("header", True).format("csv").load(f"{root_path}/states.csv")
    ports_df = spark.read.option("header", True).format("csv").load(f"{root_path}/ports.csv")

    country_ids_df_cleaned = (
        country_ids_df
        .withColumn("country_id", F.col("country_id").cast(IntegerType()))
        .withColumn("country_name", F.initcap(F.col("country_name")))  # title case
        .withColumn("country_name_lower", F.lower(F.col("country_name")))
    )

    country_iso_df_cleaned = (
        country_iso_df
        .withColumnRenamed("name", "country_name")
        .withColumnRenamed("alpha-2", "iso_code_2")
        .withColumnRenamed("alpha-3", "iso_code_3")
        .withColumnRenamed("region", "continent")
        .select("country_name", "iso_code_2", "iso_code_3", "continent")
    )
    gdp_df_cleaned = (
        gdp_df
        .withColumnRenamed("Country Name", "country_name")
        .withColumnRenamed("Country Code", "country_code")
        .withColumnRenamed("Value", "gdp_value")
        .withColumnRenamed("Year", "year")
        .withColumn("gdp_value", F.col("gdp_value").cast(FloatType()).cast(LongType()))
        .withColumn("country_name", F.initcap(F.col("country_name")))  # title case
        .withColumn("country_name_lower", F.lower(F.col("country_name")))
    )

    return country_ids_df_cleaned, country_iso_df_cleaned, gdp_df_cleaned, states_df, ports_df


def clean_immigration_data(raw_data):
    """
    Clean the immigration data by doing type conversions, renaming columns
    and adding some derived features
    """

    logger.warning("Cleaning the immigration data")

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

    # window for grouping the different visits of a single user
    same_user_window = (
        Window
        .partitionBy("admnum")
        .orderBy("arrival_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    clean_df = (
        clean_df
        .withColumn('admnum', F.col("admnum").cast(LongType()))
         # when admnum = 0, set it to null, so we don't take it into account in the num_previous_stays window
        .withColumn('admnum', F.when(F.col('admnum') == 0, F.lit(None)).otherwise(F.col('admnum')))
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
        .withColumn('num_previous_stays', F.count("admnum").over(same_user_window))
    )

    logger.info("Adding the ML Label")
    # add the label column for the ML problem, check the Data Exploration notebook for the explanation of why
    # these conditions were chosen
    clean_df = clean_df.withColumn(
        "is_overstay",
        F.when(
            (
                (clean_df.dates_match_flag == False) & # the departure and permitted dates don't match
                (clean_df.unrestricted_stay == False) & # the visa type is not one that grants unrestricted stay (i.e the match flag would then be wrong), ex. F2 visa
                 # the traveller hasn't departed yet, we use an & because in most of the cases when one is filled an the other is not, the visa a WT visa and the entdepd is a W (Waiver), 
                 # so the visa probably got extended somewhow and they shdouldn't be considered overstayers
                ((F.isnull(clean_df.departure_date)) & ((F.isnull(clean_df.entdepd)))) &
                (F.isnull(clean_df.entdepu)) # the visa's status wasn't updated
            ),
            True
        ).otherwise(False)
    )
    return clean_df


def create_staging_immigration_data(immigration_df, country_df, country_iso_df, states_df, port_df, final_columns):
    """
    Create the staging immigration data to be used for later steps by joining the different raw dataframes
    """

    joined_df = (
        immigration_df
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
            F.broadcast(states_df.alias("states_df")),
            F.col("immigration.destination_state") == F.col("states_df.state_id"),
            "leftouter"
        )
        .join(
            F.broadcast(port_df.alias("port_df")),
            F.col("immigration.entry_port") == F.col("port_df.port_id"),
            "leftouter"
        )
        .join(
            F.broadcast(country_iso_df.alias("cit_iso_df")),
            F.trim(F.col("cit_df.country_name")) == F.col("cit_iso_df.country_name"),  # trimming because the original names contain trailing spaces
            "inner"  # inner join will lead to the removal of invalid and dissolved countries (ex. Netherlands Antilles, Yugoslavia), about 60 records
        )
        .join(
            F.broadcast(country_iso_df.alias("res_iso_df")),
            F.trim(F.col("res_df.country_name")) == F.col("res_iso_df.country_name"),
            "inner"  # inner join will lead to removal of invalid and dissolved countries (ex. Netherlands Antilles, Yugoslavia), about 1849 records
        )
        # remove invalid states
        .withColumn("destination_state", F.when(F.isnull('states_df.state_id'), F.lit(None)).otherwise(F.col('immigration.destination_state')))
        .selectExpr(
            "immigration.*",
            "destination_state",
            "cit_iso_df.iso_code_3 as country_citizenship",
            "res_iso_df.iso_code_3 as country_residence",
            "TRIM(port_df.port_name) as port_name"
        )
    )

    joined_df = joined_df.select(final_columns)

    return joined_df
