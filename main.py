# main.py
import logging

from utils.extract import extract_data
from utils.transform import transform_data, validate_transformed_data, TransformationError
from utils.load import save_to_csv, save_to_postgresql, save_to_google_sheets, LoadError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    """
    Orchestrates the full ETL pipeline:
      1. Extract → extract_data()
      2. Transform → transform_data()
      3. Validate → validate_transformed_data()
      4. Load → CSV, PostgreSQL, Google Sheets
    """
    # --- Configuration ---
    # PostgreSQL
    PG_PASSWORD = "<your_password>"
    PG_CONN = f"postgresql://postgres:{PG_PASSWORD}@localhost:5432/fashion_db"
    PG_TABLE = "products"

    # Google Sheets
    SHEET_ID = "1jq8ltXsjPSibt2uVLrdGxgjqQ5dextWFQoXIFUYnbOw"
    SHEET_RANGE = "Sheet1!A1"
    CREDS_PATH = "sheet-api-key.json"

    # --- 1. Extract ---
    logger.info("Starting extract phase...")
    df_raw = extract_data()
    if df_raw.empty:
        logger.error("Extract returned no data; exiting.")
        return

    # --- 2. Transform ---
    logger.info("Starting transform phase...")
    try:
        df_trans = transform_data(df_raw)
    except TransformationError as e:
        logger.error(f"Transform failed: {e}")
        return

    if df_trans.empty:
        logger.error("No valid rows after transform; exiting.")
        return

    # --- 2,5. Validate ---
    logger.info("Validating transformed data...")
    try:
        metrics = validate_transformed_data(df_trans)
        logger.info(
            "Validation metrics: total_rows=%s, price_range=%s",
            metrics["total_rows"],
            metrics["price_range"],
        )
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return

    # --- 3. Load into targets ---
    # 3a. CSV
    logger.info("Saving to CSV: product.csv")
    try:
        save_to_csv(df_trans, "product.csv")
    except LoadError as e:
        logger.error(f"Failed to save CSV: {e}")

    # 3b. PostgreSQL
    logger.info("Saving to PostgreSQL table '%s'", PG_TABLE)
    try:
        save_to_postgresql(df_trans, PG_TABLE, PG_CONN)
    except LoadError as e:
        logger.error(f"Failed to save to PostgreSQL: {e}")

    # 3c. Google Sheets
    logger.info("Uploading to Google Sheets: %s [%s]", SHEET_ID, SHEET_RANGE)
    try:
        save_to_google_sheets(df_trans, SHEET_ID, SHEET_RANGE, CREDS_PATH)
    except LoadError as e:
        logger.error(f"Failed to upload to Google Sheets: {e}")

    logger.info("ETL pipeline completed.")

if __name__ == "__main__":
    main()
