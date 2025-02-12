import pandas 
import re
import logging

from src.utils.logging_config import setup_logging
from pyspark.sql import SparkSession

setup_logging()

class DataHandler:
    """A class for loading and writing data."""

    def __init__(self, filepath=None,
                 spark_session: SparkSession = None):
        """
        Initialize the DataHandler with an optional file path and Spark session.

        Args:
            filepath (str): The path to the dataset file.
            spark_session (SparkSession): An existing Spark session.
        """
        self.filepath = filepath
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)

    def load_data(self):
        """
        Load data from a CSV file.
å
        Args:
            filepath (str): The path to the CSV file. 
            
        Returns:
            pandas.DataFrame: The loaded dataset.
        """
        if self.filepath is None and self.filepath is None:
            self.logger.error("File path must be provided either during initialization.")

        path_to_load = self.filepath if self.filepath else self.filepath
        
        try:
            data = pandas.read_csv(path_to_load)
            column_normalized_data = self.normalize_column_names(data)
            self.logger.info(f"Data successfully loaded from {path_to_load}.")
        except FileNotFoundError:
            self.logger.error(f"File not found at {path_to_load}. Please check the path.")
        except Exception as e:
            self.logger.error(f"An error occurred while loading the file: {e}")
        
        return column_normalized_data

    def write_data(self, pandas_dataset, 
                   catalog_name, 
                   schema_name, 
                   table_name):
        """
        Write a pandas DataFrame as a Delta table.

        Args:
            pandas_dataset (pandas.DataFrame): The pandas DataFrame to be written.
            catalog_name (str): The name of the catalog.
            schema_name (str): The name of the schema. 
            table_name (str): The name of the table.
        """
        if pandas_dataset is None or not isinstance(pandas_dataset, pandas.DataFrame):
            self.logger.error("A valid pandas DataFrame must be provided.")
        
        if not catalog_name or not schema_name or not table_name:
             self.logger.error("Catalog name, schema name, and table name must all be specified.")         

        try:
            spark_df = self.spark.createDataFrame(pandas_dataset)
            
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            spark_df.write.format("delta") \
                .mode('overwrite') \
                .saveAsTable(full_table_name)

            self.logger.info(f"Data successfully written to Delta table: {full_table_name}")
        except Exception as e:
            self.logger.error(f"An error occurred while writing to the Delta table: {e}")

    def normalize_column_names(self, pandas_dataset):
        """Normalize column names to lowercase with words separated by underscores."""
        if pandas_dataset is None or not isinstance(pandas_dataset, pandas.DataFrame):
            self.logger.error("A valid pandas DataFrame must be provided.")

        normalized_columns = [
            re.sub(r'\s+', '_', col.lower()).strip() for col in pandas_dataset.columns
        ]
        
        pandas_dataset.columns = normalized_columns
        self.logger.info("Column names normalized.")
        
        return pandas_dataset

