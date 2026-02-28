import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.fetcher import fetch_all_stocks
from processing.cleaner import clean
from processing.validator import validate
from processing.transformer import transform
from storage.db import init_db, save_raw, save_processed
from utils.logger import get_logger
from config import SCHEDULE_HOUR, SCHEDULE_MINUTE
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

logger = get_logger("main")


def run_pipeline():
    """
    Full pipeline:
    1. Fetch raw data
    2. Save raw snapshot
    3. Clean
    4. Validate
    5. Transform (feature engineering)
    6. Save processed data
    """
    logger.info("=" * 50)
    logger.info(f"Pipeline started at {datetime.utcnow()}")

    try:
        # Step 1: Fetch
        raw_df = fetch_all_stocks()
        if raw_df.empty:
            logger.error("Pipeline aborted — no data fetched.")
            return

        # Step 2: Save raw
        save_raw(raw_df)

        # Step 3: Clean
        clean_df = clean(raw_df)

        # Step 4: Validate
        valid_df, rejected_df = validate(clean_df)
        if not rejected_df.empty:
            rejected_df.to_csv(f"logs/rejected_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv", index=False)

        # Step 5: Transform
        processed_df = transform(valid_df)

        # Step 6: Save processed
        save_processed(processed_df)

        logger.info(f"Pipeline completed successfully at {datetime.utcnow()}")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)


if __name__ == "__main__":
    # Initialize DB on first run
    init_db()

    import sys
    if "--now" in sys.argv:
        # Run once immediately (for testing)
        run_pipeline()
    else:
        # Schedule daily run
        scheduler = BlockingScheduler(timezone="America/New_York")
        scheduler.add_job(run_pipeline, "cron", hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE)
        logger.info(f"Scheduler started — pipeline runs daily at {SCHEDULE_HOUR}:{SCHEDULE_MINUTE:02d} EST")
        logger.info("Run 'python main.py --now' to trigger immediately")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")
