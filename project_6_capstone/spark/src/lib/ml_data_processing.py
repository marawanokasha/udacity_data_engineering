import logging

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def create_ml_data(immigration_df, gdp_df):
    """
    Create the ML Data Dataframe to be used for training the model
    """

    logger.info("Creating the ML Data")

    ml_data = (
        immigration_df
        .alias("immigration")
        .join(
            F.broadcast(gdp_df.alias("gdp_df_citizenship")),
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
            F.broadcast(gdp_df.alias("gdp_df_residence")),
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
