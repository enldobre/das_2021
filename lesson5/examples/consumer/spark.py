from pyspark.sql import SparkSession


def initialize_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Streaming Etl Job") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    print("Spark Initialized", "\n")
    return spark
