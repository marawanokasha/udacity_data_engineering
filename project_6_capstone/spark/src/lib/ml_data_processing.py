from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, FloatType, LongType


def create_ml_data(immigration_df, gdp_df):
    """
    Create the ML Data Dataframe to be used for training the model
    """

    # get only the latest GDP figures
    year_window = Window.partitionBy("country_code").orderBy(F.desc("Year"))
    latest_gdp_df = (
        gdp_df
        .withColumn("row", F.row_number().over(year_window))
        .where(F.col("row") == 1)
        .drop("row")
    )

    ml_data = (
        immigration_df
        .alias("immigration")
        .join(
            F.broadcast(latest_gdp_df.alias("gdp_df_citizenship")),
            (
                # (F.col("immigration.year") == F.col("gdp_df_citizenship.year")) & # no need for this condition because the GDP list doesn't contain 2016 GDPs for all countries
                (
                    (F.col("immigration.country_citizenship") == F.col("gdp_df_citizenship.country_code"))
                    #(F.levenshtein(F.col("immigration.country_citizenship"), F.col("gdp_df_citizenship.country_name")) < 6)
                )
            ),
            "leftouter"
        )
        .join(
            F.broadcast(latest_gdp_df.alias("gdp_df_residence")),
            (
                # (F.col("immigration.year") == F.col("gdp_df_residence.year")) &
                (
                    (F.col("immigration.country_residence") == F.col("gdp_df_residence.country_code"))
                    #(F.levenshtein(F.col("immigration.country_residence"), F.col("gdp_df_residence.country_name")) < 6)
                )
            ),
            "leftouter"
        )
        .select(
            "immigration.*",
            F.col("gdp_df_citizenship.gdp_value").alias("country_citizenship_gdp"),
            F.col("gdp_df_residence.gdp_value").alias("country_residence_gdp"),
        )
    )

    cleaned_ml_data = ml_data.select(
        "month",
        "day",
        "country_citizenship",
        "country_citizenship_gdp",
        "country_residence",
        "country_residence_gdp",
        "age",
        "gender",
        "num_previous_stays",
        "visa_type",
        # "port_name",
        "destination_state",
        "is_overstay",
    )

    return cleaned_ml_data
