import logging
import sys

def setup_logging(log_file="power_consumption_app.log"):
    """
    Sets up logging configuration for the application.

    This function configures the logging system to output logs to both a file
    and the console. It includes timestamps, log levels, and messages in the log output.
    """
    try:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout) 
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        logging.info("Logging setup completed.")

    except Exception as e:
        print(f"Error setting up logging: {e}")