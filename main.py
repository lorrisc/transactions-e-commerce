import logging
from etl import DataIngestor, DataTransformer, DataLoader, DataEnricher
from etl import config

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

def main():
    setup_logging()

    source_path = config.SOURCE_PATH
    input_file = config.INPUT_FILENAME
    output_file_data_csv = config.OUTPUT_FILE_DATA_CSV
    output_file_data_parquet = config.OUTPUT_FILE_DATA_PARQUET
    output_file_monthly_revenus = config.OUTPUT_FILE_MONTHLY_REVENUS
    output_file_pivot = config.OUTPUT_FILE_PIVOT

    # ETL
    ingestor = DataIngestor(source_path)
    transformer = DataTransformer()
    enricher = DataEnricher()
    loader = DataLoader()

    df = ingestor.read_excel(input_file)

    df = transformer.clean_customer_id(df)
    df = transformer.remove_negative_quantities(df)
    df = transformer.remove_zero_or_negative_prices(df)
    df = transformer.remove_a_customer(df, 0)
    df = transformer.filter_int_stockcodes(df)

    df = enricher.enrich_data(df)
    revenus_series = enricher.calculate_monthly_revenue(df)
    pivot_df = enricher.revenue_pivot_by_country_month(df)

    loader.save_to_csv(df, output_file_data_csv)
    loader.save_to_parquet(df, output_file_data_parquet)
    loader.save_to_json(revenus_series, output_file_monthly_revenus)
    loader.save_to_csv(pivot_df, output_file_pivot)

if __name__ == "__main__":
    main()
