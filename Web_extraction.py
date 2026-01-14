"""
web_extraction.py
Extraction des données depuis le site books.toscrape.com
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WebExtraction:
    """
    Classe responsable de l'extraction des données depuis un site web.
    """

    BASE_URL = "https://books.toscrape.com/"

    def fetch_page(self) -> str:
        """
        Télécharge le contenu HTML de la page cible.
        """
        try:
            response = requests.get(self.BASE_URL, timeout=10)
            response.raise_for_status()
            logger.info("Page web téléchargée avec succès")
            return response.text
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la récupération de la page web : {e}")
            raise

    def parse_books(self, html: str) -> List[Dict]:
        """
        Parse le HTML et extrait les informations des livres.
        """
        soup = BeautifulSoup(html, "html.parser")
        books_data = []

        books = soup.select("article.product_pod")
        logger.info(f"{len(books)} livres détectés sur la page")

        for book in books:
            try:
                title = book.h3.a["title"]
                price = book.select_one("p.price_color").text.replace("£", "")
                availability = book.select_one("p.availability").text.strip()

                books_data.append({
                    "title": title,
                    "price": float(price),
                    "availability": availability,
                    "source": "web"
                })
            except Exception as e:
                logger.warning(f"Erreur de parsing d’un livre : {e}")

        return books_data

    def extract(self) -> pd.DataFrame:
        """
        Méthode principale d’extraction.
        """
        html = self.fetch_page()
        books_data = self.parse_books(html)
        df = pd.DataFrame(books_data)

        logger.info("Extraction web terminée avec succès")
        return df
