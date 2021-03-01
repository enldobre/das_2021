from pyspark.sql.streaming import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Load DataFrame with Schema

def load_from_csv(spark, path):
    df = spark \
        .read \
        .format("csv") \
        .option("inferSchema","true") \
        .option("nullValue", "null") \
        .option("header", "true") \
        .load(path)
    print("Data loaded into PySpark", "\n")
    return df


def write_part_parquet(dataframe, path, key):
    dataframe \
        .write \
        .mode("append") \
        .partitionBy(key) \
        .parquet \
        (path)

def write_parquet(dataframe, path):
    dataframe \
        .write \
        .mode("append") \
        .parquet \
        (path)