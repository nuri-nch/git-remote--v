# cargado en:
# spark@8a6af2d88399:/opt/spark/work-dir$ cat /opt/spark/etl_weather_s3.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month, hour, dayofmonth

def main():

    spark = SparkSession.builder \
        .appName("ETL_Weather_Silver") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.sql.parquet.compression.codec","snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:

        print(">>> Leyendo datos HISTORICOS desde Bronze")

        df_hist = spark.read.option("multiline","true").json(
            "s3a://pi4.bronce/historicos/"
        )

        print(">>> Leyendo datos STREAM desde Bronze")

        df_stream = spark.read.json(
            "s3a://pi4.bronce/stream/*/onecall/"
        )

        print(">>> Normalizando HISTORICOS")

        df_hist_norm = df_hist.select(
            col("dt").cast("long").alias("dt"),
            col("lat").cast("double").alias("lat"),
            col("lon").cast("double").alias("lon"),
            col("main.temp").cast("double").alias("temp"),
            col("main.humidity").cast("integer").alias("humidity"),
            col("wind.speed").cast("double").alias("wind_speed"),
            col("clouds.all").cast("integer").alias("clouds")
        )

        print(">>> Normalizando STREAM")

        df_stream_norm = df_stream.select(
            col("_airbyte_data.current.dt").cast("long").alias("dt"),
            col("_airbyte_data.lat").cast("double").alias("lat"),
            col("_airbyte_data.lon").cast("double").alias("lon"),
            col("_airbyte_data.current.temp").cast("double").alias("temp"),
            col("_airbyte_data.current.humidity").cast("integer").alias("humidity"),
            col("_airbyte_data.current.wind_speed").cast("double").alias("wind_speed"),
            col("_airbyte_data.current.clouds").cast("integer").alias("clouds")
        )

        print(">>> Unificando datasets")

        df_final = df_hist_norm.unionByName(df_stream_norm)

        print(">>> Generando columnas temporales")

        df_final = df_final.withColumn(
            "timestamp",
            from_unixtime(col("dt"))
        )

        df_final = df_final.withColumn("year", year("timestamp"))
        df_final = df_final.withColumn("month", month("timestamp"))
        df_final = df_final.withColumn("day", dayofmonth("timestamp"))
        df_final = df_final.withColumn("hour", hour("timestamp"))

        print(">>> Guardando datos en SILVER")

        df_final.write \
            .mode("overwrite") \
            .partitionBy("year","month") \
            .parquet("s3a://pi4.silver/weather/")

        print(">>> ETL completado correctamente")

    except Exception as e:

        print("ERROR:", str(e))
        sys.exit(1)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
