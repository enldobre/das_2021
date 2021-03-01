**Requirements:**  
confluent-kafka --- add package to requirements.txt  
spark-streaming-kafka -- include line in spark-defaults.conf  
spark.jars.packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  
**Environment:**  
Install docker and start Zookeper/Kafka  
docker-compose up -d  
docker ps

# Exercises & examples:
Spark streaming Exercises:
1. Traditional spark streaming
2. Structured Streaming
3. Creating a source stream
4. Creating a sink stream/write to multiple sinks
5. Data Transformations and Aggregations
6. Enhance a spark stream with a static source
7. Watermarking
