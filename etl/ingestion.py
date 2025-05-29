import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataIngestor:
    """
    Une classe pour ingérer des données à partir de fichiers CSV ou Excel.

    Attributes:
        source_path (Path): Le chemin vers le répertoire source contenant les fichiers à lire.
    """

    def __init__(self, source_path: str):
        """
        Initialise DataIngestor avec le chemin source des fichiers.

        Args:
            source_path (str): Le chemin vers le répertoire contenant les fichiers à lire.
        """
        self.source_path = Path(source_path)

    def read_csv(self, filename: str) -> pd.DataFrame:
        """
        Lit un fichier CSV et retourne un DataFrame pandas.

        Args:
            filename (str): Le nom du fichier CSV à lire.

        Returns:
            pd.DataFrame: Un DataFrame contenant les données du fichier CSV.

        Raises:
            FileNotFoundError: Si le fichier spécifié n'existe pas.
        """
        file_path = self.source_path / filename
        if not file_path.exists():
            logger.error(f"Le fichier {file_path} n'existe pas.")
            raise FileNotFoundError(f"{file_path} introuvable.")
        df = pd.read_csv(file_path)
        logger.info(f"{len(df)} lignes chargées depuis {file_path}")
        return df

    def read_excel(self, filename: str) -> pd.DataFrame:
        """
        Lit un fichier Excel et retourne un DataFrame pandas.

        Args:
            filename (str): Le nom du fichier Excel à lire.

        Returns:
            pd.DataFrame: Un DataFrame contenant les données du fichier Excel.

        Raises:
            FileNotFoundError: Si le fichier spécifié n'existe pas.
        """
        file_path = self.source_path / filename
        if not file_path.exists():
            logger.error(f"Le fichier {file_path} n'existe pas.")
            raise FileNotFoundError(f"{file_path} introuvable.")
        df = pd.read_excel(file_path)
        logger.info(f"{len(df)} lignes chargées depuis {file_path}")
        return df
