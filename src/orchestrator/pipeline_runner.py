from src.extract.extract_bbva_data import extract_data
from src.transform.bank_transformer import clean_bank_metrics
from src.load.load_raw import load_raw_data
from src.load.load_staging import load_staging_data
from src.load.bank_dimension import load_dim_bank
from src.load.channel_dimension import load_dim_channel
from src.load.date_dimension import load_dim_date
from src.load.load_mart import MartLoader
from src.config.logger_config import get_logger
from src.data_access.etl_run_repository import ETLRunRepository
from src.load.load_fact import BankMetricsLoader
from src.data_access.watermark_repository import WatermarkRepository
from src.config.pipeline_settings import DATA_PATH, PIPELINE_NAME
from src.data_quality.bank_quality_checks import run_bank_quality_checks
from dotenv import load_dotenv

load_dotenv()
logger = get_logger(__name__)


def run_pipeline():
    repo = ETLRunRepository()
    watermark_repo = WatermarkRepository()
    fact_loader = BankMetricsLoader()
    mart_loader = MartLoader()

    run_id = repo.start_run(PIPELINE_NAME)

    try:
        logger.info("Starting pipeline...")

        # WATERMARK
        last_year = watermark_repo.get_last_year(PIPELINE_NAME)

        # EXTRACT
        df = extract_data(DATA_PATH, last_year)
        logger.info(f"Extracted {len(df)} rows")

        if df.empty:
            logger.info("No new data to process")
            repo.finish_run(run_id, rows_loaded=0)
            return

        # TRANSFORM
        df_clean = clean_bank_metrics(df)

        # DATA QUALITY
        run_bank_quality_checks(df_clean)
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
        rows_loaded = fact_loader.load(run_id)

        # MARTS
        mart_loader.load_all()

        # UPDATE WATERMARK
        new_watermark = int(df["year"].max())
        watermark_repo.update_last_year(PIPELINE_NAME, new_watermark)

        logger.info(f"Watermark updated to: {new_watermark}")

        # FINISH RUN
        repo.finish_run(run_id, rows_loaded=rows_loaded)

        logger.info("Pipeline finished successfully.")

    except Exception as e:
        repo.fail_run(run_id, str(e))

        logger.error("Pipeline failed", exc_info=True)

        raise
