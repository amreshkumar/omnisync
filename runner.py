"""
runner.py is the entry point for the application.
It loads environment variables from a .env file and schedules the sync_s3_buckets()
"""
import os
import time
import schedule
from dotenv import load_dotenv
import aws_s3_connector as aws_connector


def load_environment_variables():
    """
    Loads environment variables from a .env file.

    This function uses the python-dotenv module to load environment variables
    from a .env file into the system environment variables for use in the
    application. If the .env file does not exist or an environment variable
    is already set, the function does not overwrite the existing value.

    Parameters:
    None

    Returns:
    None
    """
    load_dotenv()

if __name__ == '__main__':
    load_environment_variables()

    interval = int(os.getenv('JOB_RUN_INTERVAL_SECONDS', '30'))  # Default to '30'

    # Schedule the sync_s3_buckets() function to run every set interval
    schedule.every(interval).seconds.do(aws_connector.sync_s3_buckets,
                                        source_bucket="omnisync-source",
                                        target_bucket="omnisync-target")

    # Run the scheduler indefinitely
    while True:
        schedule.run_pending()
        # Start after 1 min and run every set interval.
        time.sleep(1)
