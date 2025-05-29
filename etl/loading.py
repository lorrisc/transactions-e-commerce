import pandas as pd
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Une classe pour sauvegarder des DataFrames pandas dans différents formats de fichiers.

    Cette classe fournit des méthodes pour sauvegarder des données dans des formats
    tels que CSV, Parquet et JSON.
    """

    def save_to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Sauvegarde un DataFrame dans un fichier CSV.

        Args:
            df (pd.DataFrame): Le DataFrame à sauvegarder.
            output_path (str): Le chemin où le fichier CSV sera sauvegardé.

        Returns:
            None
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Données sauvegardées au format CSV : {output_path}")

    def save_to_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Sauvegarde un DataFrame dans un fichier Parquet.

        Args:
            df (pd.DataFrame): Le DataFrame à sauvegarder.
            output_path (str): Le chemin où le fichier Parquet sera sauvegardé.

        Returns:
            None
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Données sauvegardées au format Parquet : {output_path}")

    def save_to_json(self, series: pd.Series, output_path: str) -> None:
        """
        Sauvegarde une Series dans un fichier JSON.

        Args:
            series (pd.Series): La Series à sauvegarder.
            output_path (str): Le chemin où le fichier JSON sera sauvegardé.

        Returns:
            None
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        series.to_json(output_path, orient='index', date_format='iso')
        logger.info(f"Données sauvegardées au format JSON : {output_path}")
