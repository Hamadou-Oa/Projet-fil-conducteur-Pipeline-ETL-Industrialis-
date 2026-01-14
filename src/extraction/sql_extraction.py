"""
sql_extractor.py
Extraction des données depuis une base de données SQL
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SQLExtractor:
    """
    Classe responsable de l'extraction des données depuis une base SQL.
    """

    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        table: str,
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.table = table

    def _create_engine(self):
        """
        Création du moteur de connexion SQLAlchemy.
        """
        connection_url = (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_url)

    def extract(self) -> pd.DataFrame:
        """
        Extrait les données depuis la table SQL.
        """
        try:
            logger.info("Initialisation de la connexion à la base SQL")
            engine = self._create_engine()

            query = f"SELECT * FROM {self.table}"
            logger.info(f"Exécution de la requête : {query}")

            df = pd.read_sql(query, engine)

            logger.info(
                f"Extraction SQL réussie : {df.shape[0]} lignes, {df.shape[1]} colonnes"
            )

            df["source"] = "sql"

            return df

        except SQLAlchemyError as e:
            logger.error(f"Erreur SQL lors de l'extraction : {e}")
            raise

        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'extraction SQL : {e}")
            raise
