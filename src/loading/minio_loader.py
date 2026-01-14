from pyspark.sql import DataFrame
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MinIOLoader:
    """
    Classe responsable du chargement des données dans MinIO.
    """

    def __init__(self, bucket: str, dataset: str):
        self.bucket = bucket
        self.dataset = dataset

    def load(self, df: DataFrame):
        """
        Charge le DataFrame Spark dans MinIO au format Parquet
        avec partitionnement.
        """
        path = f"s3a://{self.bucket}/{self.dataset}"

        logger.info(f"Chargement des données vers MinIO : {path}")

        (
            df.write
            .mode("overwrite")
            .partitionBy("source")
            .parquet(path)
        )

        logger.info("Chargement des données terminé avec succès")
