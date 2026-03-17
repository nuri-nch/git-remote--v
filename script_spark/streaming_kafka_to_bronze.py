# cargado en:
# spark@8a6af2d88399:/opt/spark/work-dir$ cat /opt/spark/streaming_kafka_to_bronze.py

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaWeatherStream") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","172.31.36.50:9092") \
    .option("subscribe","weather_stream") \
    .option("startingOffsets","latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path","s3a://pi4.bronce/kafka_stream/") \
    .option("checkpointLocation","s3a://pi4.bronce/checkpoints/kafka_stream/") \
    .start()

query.awaitTermination()
