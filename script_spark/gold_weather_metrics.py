from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min

spark = SparkSession.builder \
    .appName("Gold_Weather_Metrics") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider") \
    .getOrCreate()

print("Leyendo datos desde SILVER")

df = spark.read.parquet("s3a://pi4.silver/weather_normalized/")

# ======================
# Potencial solar promedio
# ======================

solar = df.groupBy("location","year","month") \
    .agg(avg("uvi").alias("avg_solar_potential"))

solar.write.mode("overwrite") \
.parquet("s3a://pi4.gold/solar_potential/")

# ======================
# Potencial eólico promedio
# ======================

wind = df.groupBy("location","year","month") \
    .agg(avg("wind_speed").alias("avg_wind_potential"))

wind.write.mode("overwrite") \
.parquet("s3a://pi4.gold/wind_potential/")

# ======================
# Índice energético renovable
# ======================

energy = df.withColumn(
    "renewable_index",
    col("uvi") + col("wind_speed")
)

max_energy = energy.groupBy("location") \
    .agg(max("renewable_index").alias("max_energy"))

min_energy = energy.groupBy("location") \
    .agg(min("renewable_index").alias("min_energy"))

max_energy.write.mode("overwrite") \
.parquet("s3a://pi4.gold/max_energy/")

min_energy.write.mode("overwrite") \
.parquet("s3a://pi4.gold/min_energy/")

print("Gold generado correctamente")

spark.stop()
