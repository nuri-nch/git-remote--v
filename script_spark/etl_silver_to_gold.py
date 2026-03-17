
# cargado en:
# spark@8a6af2d88399:/opt/spark/work-dir$ cat /opt/spark/etl_silver_to_gold.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, expr

def main():

    spark = SparkSession.builder \
        .appName("ETL_Silver_to_Gold") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:

        print(">>> Leyendo capa SILVER")

        df = spark.read.parquet("s3a://pi4.silver/weather/")

        df = df.withColumn(
            "location",
            expr("""
            CASE
                WHEN lat > 0 THEN 'Riohacha'
                ELSE 'Patagonia'
            END
            """)
        )

        print(">>> Calculando potencial solar")

        df = df.withColumn(
            "solar_potential",
            col("temp") * (1 - col("clouds")/100)
        )

        print(">>> Calculando potencial eólico")

        df = df.withColumn(
            "wind_potential",
            col("wind_speed")
        )

        print(">>> Score energético")

        df = df.withColumn(
            "energy_score",
            col("solar_potential") + col("wind_potential")
        )

        print(">>> Dataset solar por hora")

        solar_hour = df.groupBy(
            "location","hour","month"
        ).agg(
            avg("solar_potential").alias("avg_solar")
        )

        solar_hour.write.mode("overwrite") \
            .parquet("s3a://pi4.gold/solar_potential")

        print(">>> Dataset patrones de viento")

        wind_patterns = df.groupBy(
            "location","month"
        ).agg(
            avg("wind_potential").alias("avg_wind")
        )

        wind_patterns.write.mode("overwrite") \
            .parquet("s3a://pi4.gold/wind_patterns")

        print(">>> Impacto climático")

        climate = df.groupBy(
            "clouds","humidity"
        ).agg(
            avg("energy_score").alias("avg_energy")
        )

        climate.write.mode("overwrite") \
            .parquet("s3a://pi4.gold/climate_impact")

        print(">>> Ranking diario de energía")

        daily = df.groupBy(
            "location","year","month","day"
        ).agg(
            avg("energy_score").alias("daily_energy")
        )

        daily.write.mode("overwrite") \
            .parquet("s3a://pi4.gold/daily_energy_rank")

        print(">>> GOLD generado correctamente")

    except Exception as e:

        print("ERROR:", str(e))
        sys.exit(1)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
