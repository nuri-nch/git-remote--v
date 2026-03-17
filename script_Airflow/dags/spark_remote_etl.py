
# cargado en:
# [ec2-user@ip-172-31-41-109 airflow_dags]$ cat ~/airflow_dags/spark_remote_etl.py

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 0
}


with DAG(
    'orquestador_remoto_spark',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:


    # 1️⃣ Verificar que Bronze tenga datos
    check_bronze_data = BashOperator(
        task_id='check_bronze_data',
        bash_command='echo "Verificando datos en Bronze layer..."'
    )


    # 2️⃣ Ejecutar ETL Bronze → Silver
    spark_bronze_to_silver = SSHOperator(
        task_id='spark_bronze_to_silver',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
        --executor-memory 1g \
        --executor-cores 2 \
        /opt/spark/etl_weather_s3.py
        """
    )


    # 3️⃣ Ejecutar ETL Silver → Gold
    spark_silver_to_gold = SSHOperator(
        task_id='spark_silver_to_gold',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
        /opt/spark/etl_silver_to_gold.py
        """
    )


    # Orden del pipeline
    check_bronze_data >> spark_bronze_to_silver >> spark_silver_to_gold
