from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_unixtime,
    year,
    month,
    input_file_name,
    regexp_extract,
    lower,
    lit,
)

spark = (
    SparkSession.builder.appName("ETL_Weather_Normalization")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    )
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(">>> Leyendo datos historicos desde Bronze")

df_hist = spark.read.option("multiline", "true").json("s3a://pi4.bronce/historicos/*")

print(">>> Leyendo datos de streaming")

df_stream = spark.read.json("s3a://pi4.bronce/stream/*/*/*")

# Detectar ciudad desde streaming
df_stream = df_stream.withColumn("file_path", input_file_name())

df_stream = df_stream.withColumn(
    "location", regexp_extract("file_path", "stream/(.*?)/", 1)
)

# Normalizar históricos
df_hist_norm = df_hist.select(
    col("dt").cast("long"),
    col("lat").cast("double"),
    col("lon").cast("double"),
    col("main.temp").cast("double").alias("temp"),
    col("main.humidity").cast("integer").alias("humidity"),
    col("city_name"),
)

df_hist_norm = df_hist_norm.withColumn("location", lower(col("city_name"))).drop(
    "city_name"
)

# Agregar columnas faltantes para mantener esquema
df_hist_norm = (
    df_hist_norm.withColumn("pressure", lit(None).cast("integer"))
    .withColumn("clouds", lit(None).cast("integer"))
    .withColumn("wind_speed", lit(None).cast("double"))
    .withColumn("uvi", lit(None).cast("double"))
)

# Normalizar streaming
df_stream_norm = df_stream.select(
    col("_airbyte_data.current.dt").cast("long").alias("dt"),
    col("_airbyte_data.lat").cast("double").alias("lat"),
    col("_airbyte_data.lon").cast("double").alias("lon"),
    col("_airbyte_data.current.temp").cast("double").alias("temp"),
    col("_airbyte_data.current.humidity").cast("integer").alias("humidity"),
    col("_airbyte_data.current.pressure").cast("integer").alias("pressure"),
    col("_airbyte_data.current.clouds").cast("integer").alias("clouds"),
    col("_airbyte_data.current.wind_speed").cast("double").alias("wind_speed"),
    col("_airbyte_data.current.uvi").cast("double").alias("uvi"),
    col("location"),
)

# Unir históricos y streaming
df_final = df_hist_norm.unionByName(df_stream_norm)

# Crear columnas de fecha
df_final = df_final.withColumn("date", from_unixtime("dt"))

df_final = df_final.withColumn("year", year("date")).withColumn("month", month("date"))
print(">>> Guardando datos particionados en Silver")

df_final.write.mode("overwrite").partitionBy("location", "year", "month").parquet(
    "s3a://pi4.silver/weather_normalized/"
)
spark.stop()

print(">>> ETL FINALIZADO")
