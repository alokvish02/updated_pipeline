# from update_csv import update_csv
# from csv_insertion import active_data_insertion, csv_active_notification, History_data_insertion, csv_history_notification
# from delete_old_data import truncate_tile_views, truncate_ds_chart
# from view_for_Ds_charts import create_ds_chart_view
# from tile_views import create_tiles_views
#
#
# truncate_tile_views()
# truncate_ds_chart()
#
# # update_csv()
# # active_data_insertion()
# # csv_active_notification()
# # History_data_insertion()
# # csv_history_notification()
#
# create_ds_chart_view()
# create_tiles_views()


"""
Data Processing Pipeline
A comprehensive data processing pipeline that handles CSV updates, data insertion,
notifications, cleanup, and view creation.
"""

import logging
from typing import Optional
import sys

# Import all required modules
from update_csv import update_csv
from csv_insertion import (
    active_data_insertion,
    csv_active_notification,
    History_data_insertion,
    csv_history_notification
)
from delete_old_data import truncate_tile_views, truncate_ds_chart, run_custom_query
from view_for_Ds_charts import create_ds_chart_view
from tile_views import create_tiles_views


def setup_logging() -> None:
    """Setup logging configuration for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data_pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )


def cleanup_old_data() -> bool:
    """
    Clean up old data by truncating tile views and DS chart data.

    Returns:
        bool: True if cleanup successful, False otherwise
    """
    try:
        logging.info("Starting data cleanup process...")

        logging.info("Truncating tile views...")
        truncate_tile_views()

        logging.info("Truncating DS chart data...")
        truncate_ds_chart()

        logging.info("Truncating active_trade...")
        run_custom_query("DELETE FROM public.nse_utils_trade")

        logging.info("Truncating history_trade...")
        run_custom_query("DELETE FROM public.nse_utils_trade_history")

        logging.info("Data cleanup completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error during data cleanup: {str(e)}")
        return False


def update_csv_data() -> bool:
    """
    Update CSV data files.

    Returns:
        bool: True if update successful, False otherwise
    """
    try:
        logging.info("Starting CSV data update...")
        update_csv()
        logging.info("CSV data update completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error during CSV update: {str(e)}")
        return False

def insert_active_data() -> bool:
    """
    Insert active data and send notifications.

    Returns:
        bool: True if insertion successful, False otherwise
    """
    try:
        logging.info("Starting active data insertion...")

        logging.info("Inserting active data...")
        active_data_insertion()

        logging.info("Sending active data notification...")
        csv_active_notification()

        logging.info("Active data insertion completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error during active data insertion: {str(e)}")
        return False


def insert_history_data() -> bool:
    """
    Insert historical data and send notifications.

    Returns:
        bool: True if insertion successful, False otherwise
    """
    try:
        logging.info("Starting history data insertion...")

        logging.info("Inserting history data...")
        History_data_insertion()

        logging.info("Sending history data notification...")
        csv_history_notification()

        logging.info("History data insertion completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error during history data insertion: {str(e)}")
        return False


def insert_active_data_history() -> bool:
    try:
        logging.info("Starting active data insertion in history table...")

        run_custom_query("INSERT INTO nse_utils_trade_history SELECT * FROM nse_utils_trade;")

        logging.info("active data insertion in history completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error during active data insertion in history: {str(e)}")
        return False


def create_data_views() -> bool:
    """
    Create DS chart views and tile views.

    Returns:
        bool: True if view creation successful, False otherwise
    """
    try:
        logging.info("Starting data view creation...")

        logging.info("Creating DS chart views...")
        create_ds_chart_view()

        logging.info("Creating tile views...")
        create_tiles_views()

        logging.info("Data view creation completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error during view creation: {str(e)}")
        return False


def run_pipeline() -> bool:
    """
    Execute the complete data processing pipeline.

    Returns:
        bool: True if entire pipeline successful, False otherwise
    """
    pipeline_steps = [
        ("Data Cleanup", cleanup_old_data),
        ("CSV Update", update_csv_data),
        ("Active Data Insertion", insert_active_data),
        ("History Data Insertion", insert_history_data),
        ("Insert Active Data in history", insert_active_data_history),
        ("View Creation", create_data_views)
    ]

    logging.info("=" * 50)
    logging.info("Starting Data Processing Pipeline")
    logging.info("=" * 50)

    for step_name, step_function in pipeline_steps:
        logging.info(f"Executing step: {step_name}")

        if not step_function():
            logging.error(f"Pipeline failed at step: {step_name}")
            return False

        logging.info(f"Step completed: {step_name}")
        logging.info("-" * 30)

    logging.info("=" * 50)
    logging.info("Data Processing Pipeline completed successfully!")
    logging.info("=" * 50)
    return True


def main() -> None:
    """
    Main function to execute the data processing pipeline.
    """
    # Setup logging
    setup_logging()

    try:
        # Run the complete pipeline
        success = run_pipeline()

        if success:
            logging.info("All pipeline operations completed successfully")
            sys.exit(0)
        else:
            logging.error("Pipeline execution failed")
            sys.exit(1)

    except KeyboardInterrupt:
        logging.info("Pipeline execution interrupted by user")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected error in main execution: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
#     insert_active_data_history()