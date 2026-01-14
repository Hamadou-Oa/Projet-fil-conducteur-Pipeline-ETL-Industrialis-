"""
spark_session.py
Création et configuration de la SparkSession
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "ETL_Pipeline") -> SparkSession:
    """
    Crée et retourne une SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark
def get_spark_session(app_name: str = "ETL_Pipeline") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark