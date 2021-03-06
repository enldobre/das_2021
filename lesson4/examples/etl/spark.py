from pyspark.sql import SparkSession


def initialize_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Etl Job") \
        .getOrCreate()
    print("Spark Initialized", "\n")
    return spark
