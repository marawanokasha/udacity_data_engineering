import sys
import logging
from pyspark.sql import SparkSession


print(sys.argv)
logging.info("hellllllllllllllllllloooooooooooooooooooooo")
logging.warning("2222222222222222222222222222222222222222")
print("pppppppppppppppppppppppppppppppp")

from data_manipulation import normalize_visa_category


print(sys.argv)
logging.info("hellllllllllllllllllloooooooooooooooooooooo")
logging.warning("2222222222222222222222222222222222222222")
print("pppppppppppppppppppppppppppppppp")
spark = SparkSession \
    .builder\
    .appName("test-spark-app") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.1.0-s_2.11")\
    .getOrCreate()

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")

print("uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
logging.info("oooooooooooooooooollllllllllllllllehhhhhhhhhhhhhhhhhhhh")
spark.stop()
