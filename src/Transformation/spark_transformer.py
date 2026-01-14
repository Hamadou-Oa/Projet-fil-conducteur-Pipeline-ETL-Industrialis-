"""
spark_transformer.py
Description: Transformations distribuées avec Apache Spark
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from src.utils.spark_session import get_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkTransformer:
    """
    Classe responsable des transformations distribuées avec Spark.
    """

    def __init__(self):
        self.spark = get_spark_session("Spark_ETL_Transformation")

    def to_spark_df(self, pandas_df):
        """
        Convertit un DataFrame Pandas en DataFrame Spark.
        """
        logger.info("Conversion Pandas DataFrame -> Spark DataFrame")
        return self.spark.createDataFrame(pandas_df)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Nettoyage des données :
        - suppression des valeurs nulles critiques
        - normalisation des champs
        """
        logger.info("Début du nettoyage des données")

        df_cleaned = (
            df
            .dropna(subset=["title"])
            .withColumn(
                "availability",
                when(col("availability").isNull(), "Unknown")
                .otherwise(col("availability"))
            )
        )

        logger.info("Nettoyage des données terminé")
        return df_cleaned

    def normalize_data(self, df: DataFrame) -> DataFrame:
        """
        Normalisation des données.
        """
        logger.info("Début de la normalisation des données")

        df_normalized = (
            df
            .withColumn("price", col("price").cast("double"))
            .withColumn("source", col("source").cast("string"))
        )

        logger.info("Normalisation terminée")
        return df_normalized

    def transform(self, dfs: list) -> DataFrame:
        """
        Méthode principale de transformation.
        """
        logger.info("Début des transformations Spark")

        spark_dfs = [self.to_spark_df(df) for df in dfs]

        df_union = spark_dfs[0]
        for df in spark_dfs[1:]:
            df_union = df_union.unionByName(df, allowMissingColumns=True)

        df_cleaned = self.clean_data(df_union)
        df_final = self.normalize_data(df_cleaned)

        logger.info("Transformation Spark terminée avec succès")
        return df_final
