from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, ShortType, DateType

schema = StructType([
    StructField("Ip", StringType(), True),
    StructField("User", StringType(), True)
])

log_schema = StructType([
    StructField("remote_host", StringType(), True),
    StructField("request_method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("request_url", StringType(), True),
    StructField("datetime", StringType(), True)
])