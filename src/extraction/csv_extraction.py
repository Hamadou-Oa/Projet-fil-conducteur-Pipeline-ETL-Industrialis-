"""
csv_extractor.py
Extraction des données depuis le fichier CSV
"""

import pandas as pd
from typing import Optional
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CSVExtractor:
    """
    Classe responsable de l'extraction des données depuis le fichier CSV.
    """

    def __init__(self, csv_url: str):
        self.csv_url = csv_url

    def extract(self) -> pd.DataFrame:
        """
        Téléchargé et lit le fichier CSV.
        """
        try:
            logger.info(f"Téléchargement du fichier CSV depuis : {self.csv_url}")
            df = pd.read_csv(self.csv_url)

            logger.info(
                f"Extraction CSV réussie : {df.shape[0]} lignes, {df.shape[1]} colonnes"
            )

            df["source"] = "csv"

            return df

        except pd.errors.ParserError as e:
            logger.error(f"Erreur de parsing du fichier CSV : {e}")
            raise

        except Exception as e:
            logger.error(f"Erreur lors de l'extraction CSV : {e}")
            raise
