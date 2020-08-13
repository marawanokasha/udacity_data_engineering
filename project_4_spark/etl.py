import configparser
import argparse
import logging
import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

logger = logging.getLogger(__name__)

SONGS_OUTPUT_FILE = "songs_table.parquet"
ARTISTS_OUTPUT_FILE = "artists_table.parquet"
USERS_OUTPUT_FILE = "users_table.parquet"
TIME_OUTPUT_FILE = "time_table.parquet"
SONGPLAYS_OUTPUT_FILE = "songplays_table.parquet"


SONGS_INPUT_FILE = "song_data/*/*/*/*.json"
LOGS_INPUT_FILE = "log_data"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("sparkify-ETL")  \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read the raw song data and create song and artist dimension tables which are written to 
    the output_data location as parquet files
    """
    # get filepath to song data file
    song_data = f"{input_data}{SONGS_INPUT_FILE}"
    
    logger.info(f"Default Parallelism is {spark.sparkContext.defaultParallelism}")
    
    # use an explicit schema to force the use of ints instead of longs and floats instead of doubles
    # which leads to a better compression. We could have also just done a cast during selection
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_name", StringType()),
        StructField("artist_latitude", FloatType()),
        StructField("artist_longitude", FloatType()),
        StructField("artist_location", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", FloatType()),
        StructField("year", IntegerType())
    ])
    
    logger.info(f"Reading raw Song data from source: {song_data}")
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)
    
    logger.info(f"Raw Song Data has {df.rdd.getNumPartitions()} partitions")
    logger.info(f"Partition size is {df.rdd.glom().map(len).collect()}")

    logger.info("Extracting the song dimension table")
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    logger.info(f"Song dimension table has {songs_table.rdd.getNumPartitions()} memory partitions")
    
    logger.info("Writing back the song dimension table partitioned by year and artist_id")
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table
     .write
     .mode("overwrite")
     .partitionBy("year", "artist_id")
     .option("spark.sql.parquet.compression.codec", "snappy")
     .parquet(f"{output_data}{SONGS_OUTPUT_FILE}")
    )

    logger.info("Extracting the artist dimension table")
    
    # extract columns to create artists table
    # we have to drop duplicates here because the same artist can have multiple songs
    artists_table = df.select(
        "artist_id",
        F.col("artist_name").alias("name"),
        F.col("artist_location").alias("location"),
        F.col("artist_latitude").alias("latitude"),
        F.col("artist_longitude").alias("longitude")
    ).drop_duplicates(["artist_id"])
    
    logger.info(f"Artist dimension table has {artists_table.rdd.getNumPartitions()} memory partitions")
    logger.info(f"Artist dimension table partition size is {artists_table.rdd.glom().map(len).collect()}")
    
    logger.info(f"Repartitioning the artists table to reduce partition skewness")
    artists_table = artists_table.repartition(5)
    
    logger.info(f"Artist dimension table has {artists_table.rdd.getNumPartitions()} memory partitions")
    logger.info(f"Artist dimension table partition size is {artists_table.rdd.glom().map(len).collect()}")
    
    
    logger.info("Writing back the artist dimension table")
    
    # write artists table to parquet files
    (artists_table
     .write
     .mode("overwrite")
     .option("spark.sql.parquet.compression.codec", "snappy")
     .parquet(f"{output_data}{ARTISTS_OUTPUT_FILE}")
    )
    
    logger.info("Finished processing the raw song data")


def process_log_data(spark, input_data, output_data):
    """
    Read the raw log data and create user and time dimension tables and a songplays fact table 
    which are all then written to the output_data location as parquet files
    """
    
    # get filepath to log data file
    log_data = f"{input_data}{LOGS_INPUT_FILE}"

    logger.info(f"Default Parallelism is {spark.sparkContext.defaultParallelism}")
    logger.info("Reading Log data from source")
    
    # read log data file
    df = spark.read.json(log_data)
    
    logger.info(f"Raw Log Data has {df.rdd.getNumPartitions()} partitions")
    logger.info(f"Partition size is {df.rdd.glom().map(len).collect()}")

    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    logger.info("Extracting the users dimension table")
    
    # extract columns for users table    
    users_table = df.select(
        F.col("userId").alias("user_id"),
        F.col("firstName").alias("first_name"),
        F.col("lastName").alias("last_name"),
       "gender",
        "level"
    ).drop_duplicates(["user_id"])

    logger.info(f"Users dimension table has {users_table.rdd.getNumPartitions()} memory partitions")
    logger.info(f"Users dimension table partition size is {users_table.rdd.glom().map(len).collect()}")
    logger.info("Writing back the users dimension table")
    
    # write users table to parquet files
    (users_table.write
     .mode("overwrite")
     .parquet(f"{output_data}{USERS_OUTPUT_FILE}")
    )

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x//1000, IntegerType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime("timestamp"))
    
    logger.info("Extracting the time dimension table")
    
    # extract columns to create time table
    time_table = df.select(
        F.col("timestamp").alias("start_time"),
        F.hour("datetime").alias("hour"),
        F.dayofmonth("datetime").alias("day"),
        F.weekofyear("datetime").alias("week"),
        F.month("datetime").cast("int").alias("month"),
        F.year("datetime").cast("int").alias("year"),
        F.date_format("datetime", "%w").cast("int").alias("weekday")
    )
    
    logger.info(f"Time dimension table has {time_table.rdd.getNumPartitions()} memory partitions")
    logger.info(f"Time dimension table partition size is {time_table.rdd.glom().map(len).collect()}")
    logger.info("Writing back the time dimension table partitioned by year and month")
    
    # write time table to parquet files partitioned by year and month
    (time_table
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(f"{output_data}{TIME_OUTPUT_FILE}")
    )

    logger.info("Reading the raw song data so we can correlate song plays to songs")
    
    # read in song data to use for songplays table
    song_df = spark.read.json(f"{input_data}{SONGS_INPUT_FILE}")

    logger.info(f"Song raw data table has {song_df.rdd.getNumPartitions()} partitions")
    logger.info(f"Song raw data table partition size is {song_df.rdd.glom().map(len).collect()}")
    
    logger.info("Extracting the songplays Fact table by joining the logs DF to the song dimension table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        df
        .join(song_df, on = ((df.song == song_df.title) & (df.artist == song_df.artist_name)))
        .select(
            F.monotonically_increasing_id().alias("songplay_id"),
            F.col("timestamp").alias("start_time"),
            F.col("userId").alias("user_id"),
            F.col("level"),
            song_df.song_id,
            song_df.artist_id,
            F.col("sessionId").cast("int").alias("session_id"),
            F.col("location"),
            F.col("userAgent").alias("user_agent"),
        )
    )

    logger.info(f"Songplays fact table has {songplays_table.rdd.getNumPartitions()} memory partitions")
    logger.info(f"Songplays fact table partition size is {songplays_table.rdd.glom().map(len).collect()}")
    logger.info("Writing back the songplays fact table")
    
    # write songplays table to parquet files partitioned by year and month 
    # HOW ??? should I add those columns to the table or do a memory repartitioning first with them
    # What's the point of a memory repartitioning the data, is it just to make it more unifom across the paritions?
    # there will still be no read-time benefit because we are not partitioning on disk, so we'd still have to load 
    # the whole parquet files into memory
    (songplays_table
     .write
     .mode("overwrite")
     .parquet(f"{output_data}{SONGPLAYS_OUTPUT_FILE}")
    )

    logger.info("Finished processing the raw log data")

def main(local=True):

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    
    if local:
        logger.info("Using local data")
        input_data = "./data/"
        output_data = "./output_data/"
    else:
        logger.info("Using remote data from S3")
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://udacity-lesson3-project-bucket/"
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    # stop the spark session so the spark application ends
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        format='[ %(asctime)s ] %(filename)s(%(lineno)d) %(levelname)s - %(message)s',
        level=logging.INFO
    )
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", default=False, 
                        help="Whether to run the ETL pipeline locally or using the remote S3 data")
    args = parser.parse_args()
    
    main(args.local)
