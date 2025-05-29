import pandas as pd
import logging
import locale

logger = logging.getLogger(__name__)

class DataTransformer:
    def clean_customer_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie la colonne 'CustomerID' en remplaçant les valeurs manquantes par 0.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données à nettoyer.

        Returns:
            pd.DataFrame: Le DataFrame avec la colonne 'CustomerID' nettoyée.
        """
        if 'CustomerID' in df.columns:
            missing_before = df['CustomerID'].isna().sum()
            df['CustomerID'] = df['CustomerID'].fillna(0).astype(int)
            logger.info(f"'CustomerID' : {missing_before} valeurs manquantes remplacées par 0")
        else:
            logger.warning("'CustomerID' non trouvée dans le DataFrame")
        return df

    def remove_negative_quantities(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Supprime les lignes où la colonne 'Quantity' est négative.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données à nettoyer.

        Returns:
            pd.DataFrame: Le DataFrame sans les lignes de quantités négatives.
        """
        if 'Quantity' in df.columns:
            initial_rows = len(df)
            df = df[df['Quantity'] >= 0]
            removed_rows = initial_rows - len(df)
            logger.info(f"{removed_rows} lignes supprimées (Quantity < 0)")
        else:
            logger.warning("'Quantity' non trouvée dans le DataFrame")
        return df

    def remove_zero_or_negative_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Supprime les lignes où la colonne 'UnitPrice' est zéro ou négative.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données à nettoyer.

        Returns:
            pd.DataFrame: Le DataFrame sans les lignes de prix zéro ou négatifs.
        """
        if 'UnitPrice' in df.columns:
            initial_rows = len(df)
            df = df[df['UnitPrice'] > 0]
            removed_rows = initial_rows - len(df)
            logger.info(f"{removed_rows} lignes supprimées (UnitPrice <= 0)")
        else:
            logger.warning("'UnitPrice' non trouvée dans le DataFrame")
        return df

    def remove_a_customer(self, df: pd.DataFrame, customer_id: int) -> pd.DataFrame:
        """
        Supprime les lignes associées à un 'CustomerID' spécifique.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données à nettoyer.
            customer_id (int): L'ID du client à supprimer.

        Returns:
            pd.DataFrame: Le DataFrame sans les lignes du client spécifié.
        """
        if 'CustomerID' in df.columns:
            initial_rows = len(df)
            df = df[df['CustomerID'] != customer_id]
            removed_rows = initial_rows - len(df)
            logger.info(f"{removed_rows} lignes supprimées (CustomerID = {customer_id})")
        else:
            logger.warning("'CustomerID' non trouvée dans le DataFrame")
        return df

    def filter_int_stockcodes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtre les 'StockCode' qui peuvent être convertis en entiers.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données à filtrer.

        Returns:
            pd.DataFrame: Le DataFrame avec seulement les 'StockCode' int-like.
        """
        if 'StockCode' not in df.columns:
            logger.warning("'StockCode' non trouvée dans le DataFrame")
            return df

        def is_int(val):
            """
            Vérifie si une valeur peut être convertie en entier.

            Args:
                val: La valeur à vérifier.

            Returns:
                bool: True si la valeur est convertible en entier, sinon False.
            """
            try:
                int(val)
                return True
            except ValueError:
                return False

        is_int_mask = df['StockCode'].apply(is_int)
        nb_int = is_int_mask.sum()
        nb_non_int = len(df) - nb_int

        logger.info(f"{nb_int} valeurs int-like dans 'StockCode'")
        logger.info(f"{nb_non_int} valeurs non convertibles en int supprimées")

        df = df[is_int_mask].copy()
        df['StockCode'] = df['StockCode'].astype(int)
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Supprime les lignes avec des valeurs manquantes.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données à nettoyer.

        Returns:
            pd.DataFrame: Le DataFrame sans valeurs manquantes.
        """
        initial_count = len(df)
        df = df.dropna()
        logger.info(f"{initial_count - len(df)} lignes supprimées (valeurs manquantes)")
        return df

class DataEnricher:
    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrichit le DataFrame avec des colonnes supplémentaires.

        Args:
            df (pd.DataFrame): Le DataFrame à enrichir.

        Returns:
            pd.DataFrame: Le DataFrame enrichi avec de nouvelles colonnes.
        """
        # Colonne Revenu
        df['Revenu'] = df['UnitPrice'] * df['Quantity']
        logger.info("Colonne 'Revenu' ajoutée")

        # Nouvelles colonnes de dates
        locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
        df['AnneeMois'] = df['InvoiceDate'].dt.to_period('M')
        df['Mois'] = df['InvoiceDate'].dt.month
        df['Jour'] = df['InvoiceDate'].dt.day
        df['Semaine'] = df['InvoiceDate'].dt.isocalendar().week
        df['JourSemaine'] = df['InvoiceDate'].dt.dayofweek
        df['NomJour'] = df['InvoiceDate'].dt.strftime('%A')

        logger.info("Colonnes de date ajoutées: 'AnneeMois', 'Mois', 'Jour', 'Semaine', 'JourSemaine', 'NomJour'")

        return df

    def calculate_monthly_revenue(self, df: pd.DataFrame) -> pd.Series:
        """
        Calcule les revenus mensuels à partir du DataFrame.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données de revenu.

        Returns:
            pd.Series: Une série avec les revenus mensuels.
        """
        if 'AnneeMois' not in df.columns or 'Revenu' not in df.columns:
            logger.error("Colonnes 'AnneeMois' ou 'Revenu' manquantes")
            return pd.Series()

        revenus_par_mois = df.groupby('AnneeMois')['Revenu'].sum().sort_index()
        revenus_par_mois.index = revenus_par_mois.index.astype(str)
        logger.info("Revenus mensuels agrégés calculés")

        return revenus_par_mois

    def revenue_pivot_by_country_month(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crée une table pivot des revenus par pays et mois.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données de revenu.

        Returns:
            pd.DataFrame: Une table pivot avec les revenus par pays et mois.
        """
        required_cols = ['Country', 'AnneeMois', 'Revenu']
        if not all(col in df.columns for col in required_cols):
            logger.error(f"Colonnes manquantes. Nécessaires : {required_cols}")
            return pd.DataFrame()

        pivot = df.pivot_table(
            values='Revenu',
            index='Country',
            columns='AnneeMois',
            aggfunc='sum',
            fill_value=0
        )
        logger.info("Pivot table (Revenu par Country & AnneeMois) créée")
        return pivot
