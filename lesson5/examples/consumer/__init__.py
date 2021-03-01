from lesson5.examples.consumer.loader import *
from lesson5.examples.consumer.transformations import extract_logs
from spark import initialize_spark
from lesson5.examples.consumer.schema import schema
from pyspark.sql.functions import *


def main():
    """Entry point for the application script"""

    session = initialize_spark()
    user_path = "/training/data_academy_spark/lesson5/examples/resources/users.csv"
    static_data = load_from_csv(session,schema,user_path)
    bootstrap = "localhost:9092"
    topic = "sample"
    df = load_from_kafka(session, bootstrap, topic)
    df.printSchema()
    data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    log_data =extract_logs(data)
    full_data = log_data.join(static_data,log_data.host ==static_data.Ip ,"left")
    count_per_user = full_data \
        .filter(col('User').isNotNull())
    agg_data = count_per_user.groupBy(col('User')).count()
    write_foreach(agg_data, foreach_batch_function)


if __name__ == '__main__':
    main()
