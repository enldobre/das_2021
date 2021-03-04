from pyspark.sql.streaming import *


# Load DataFrame with Schema
def load_from_csv(spark,path):
    df = spark \
        .read \
        .format("csv") \
        .option("inferSchema","true") \
        .option("header", "true") \
        .load(path)
    print("Data loaded into PySpark", "\n")
    return df


# Write DataFrame To Parquet
def write_to_parquet(dataframe, path):
    dataframe \
        .write \
        .mode("append") \
        .parquet(path)
