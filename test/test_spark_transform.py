from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def test_spark_session():
    """
    Verifica que Spark puede iniciarse correctamente
    """
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    assert spark is not None
    spark.stop()


def test_dataframe_transformation():
    """
    Verifica una transformación de datos con PySpark
    """

    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    # datos simulados (como si vinieran de Kafka o una API)
    data = [(20, 50), (25, 60)]

    df = spark.createDataFrame(data, ["temperature", "humidity"])

    # transformación típica de ETL
    df_transformed = df.withColumn("temp_fahrenheit", col("temperature") * 9 / 5 + 32)

    result = df_transformed.collect()

    assert result[0]["temp_fahrenheit"] == 68
    assert result[1]["temp_fahrenheit"] == 77

    spark.stop()
