from src.extract.extract_bbva_data import extract_bbva_data
from src.transform.bank_transformer import clean_bank_metrics
from src.load.load_raw import load_raw_data
from src.load.load_staging import load_staging_data
from src.load.bank_dimension import load_dim_bank
from src.load.channel_dimension import load_dim_channel
from src.load.date_dimension import load_dim_date
from src.load.load_fact import load_fact_table
from src.load.load_mart import MartLoader
from src.config.logger_confing import get_logger
from src.data_access.etl_run_repository import ETLRunRepository
from src.data_access.watermark_repository import WatermarkRepository
from src.data_quality.bank_quality_checks import run_bank_quality_checks
from dotenv import load_dotenv

load_dotenv()

logger = get_logger(__name__)
pipeline_name = "bbva_data_pipeline"


def run_pipeline():
    repo = ETLRunRepository()
    run_id = repo.start_run(pipeline_name)
    watermark_repo = WatermarkRepository()

    try:
        logger.info("Starting pipeline...")
        last_year = watermark_repo.get_last_year(pipeline_name)

        # EXTRACT
        df = extract_bbva_data(last_year)
        logger.info(f"Extracted {len(df)} rows")

        if df.empty:
            logger.info("No new data")
            return

        # TRANSFORM
        df_clean = clean_bank_metrics(df)

        # DATA QUALITY CHECKS
        run_bank_quality_checks(df)
        logger.info("Data quality checks passed")

        # RAW
        load_raw_data(df_clean)

        # STAGING
        load_staging_data(df_clean)

        # DIMENSIONS
        load_dim_bank()
        load_dim_channel()
        load_dim_date()

        # FACT
        load_fact_table()

        # MARTS
        mart_loader = MartLoader()
        mart_loader.load_all()

        # Close execution
        repo.finish_run(run_id, rows_loaded=len(df_clean))

        # Calculate new watermark
        new_watermark = int(df["year"].max())

        # Update watermark
        watermark_repo.update_last_year(pipeline_name, new_watermark)
        logger.info(f"Watermark updated to: {new_watermark}")

        logger.info("Pipeline finished successfully.")

    except Exception as e:
        repo.fail_run(run_id, str(e))
        logger.error("Pipeline failed", exc_info=True)

        raise


if __name__ == "__main__":
    run_pipeline()
