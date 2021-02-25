from pyspark.sql import SparkSession


def initialize_spark():
    spark = SparkSession.builder \
        .master('local[*]') \
        .appName('test') \
        .getOrCreate()
    return spark
