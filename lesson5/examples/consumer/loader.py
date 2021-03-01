from pyspark.sql.streaming import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Load DataFrame with Schema

def load_from_csv(spark, schema, path):
    df = spark \
        .read \
        .format("csv") \
        .schema(schema) \
        .option("header", "true") \
        .load(path)
    print("Data loaded into PySpark", "\n")
    return df


# Load DataFrame From Kafka Stream
def load_from_kafka(spark, bootstrap, topic):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("subscribe", topic) \
        .load()
    print("Data loaded into PySpark", "\n")
    return df


# Write DataFrame To Parquet
def write_to_parquet(dataframe, path):
    dataframe \
        .write \
        .mode("append") \
        .parquet(path)


# Write Stream To Parquet
def write_stream_to_parquet(dataframe, path):
    dataframe \
        .writeStream \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", "/checkpoint/2") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


# Write Stream To Console
def write_to_console(dataframe):
    dataframe.writeStream \
        .queryName("random_query") \
        .outputMode("append") \
        .format("console") \
        .option("checkpointLocation", "/checkpoint/3") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()


# write to MultipleSinks
def write_foreach(dataframe, foreach_batch_function):
    dataframe.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("update") \
        .option("checkpointLocation", "/checkpoint/4") \
        .start() \
        .awaitTermination()


def foreach_batch_function(df, epoch_id):
    print(epoch_id)
    df.show()
    write_to_parquet(df, "/output/")


def stream_from_kafka(host, ip):
    sc = SparkContext("local[*]", "Classic Word Count")
    ssc = StreamingContext(sc, 10)
    lines = ssc.socketTextStream(host, ip)
    words = lines.flatMap(lambda line: line.split(" "))
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
