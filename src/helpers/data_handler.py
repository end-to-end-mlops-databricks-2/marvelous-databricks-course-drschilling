import pandas as pd
import re
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataHandler:
    """A class for loading and writing data."""

    def __init__(self, filepath=None, spark_session: SparkSession = None):
        """
        Initialize the DataHandler with an optional file path and Spark session.

        Args:
            filepath (str): The path to the dataset file.
            spark_session (SparkSession): An existing Spark session.
        """
        self.filepath = filepath
        self.data = None
        self.spark = spark_session

    def load_data(self, filepath=None):
        """
        Load data from a CSV file.

        Args:
            filepath (str): The path to the CSV file. 
            
        Returns:
            pandas.DataFrame: The loaded dataset.
        """
        if filepath is None and self.filepath is None:
            raise ValueError("File path must be provided either during initialization.")

        path_to_load = filepath if filepath else self.filepath
        try:
            self.data = pd.read_csv(path_to_load)
            logging.info(f"Data successfully loaded from {path_to_load}.")
            return self.data
        except FileNotFoundError:
            logging.error(f"File not found at {path_to_load}. Please check the path.")
            raise FileNotFoundError(f"File not found at {path_to_load}. Please check the path.")
        except Exception as e:
            logging.error(f"An error occurred while loading the file: {e}")
            raise Exception(f"An error occurred while loading the file: {e}")

    def write_data(self, pandas_df, catalog_name, schema_name, table_name):
        """
        Write a pandas DataFrame as a Delta table.

        Args:
            pandas_df (pandas.DataFrame): The pandas DataFrame to be written.
            catalog_name (str): The name of the catalog.
            schema_name (str): The name of the schema. 
            table_name (str): The name of the table.
        """
        if pandas_df is None or not isinstance(pandas_df, pd.DataFrame):
            raise ValueError("A valid pandas DataFrame must be provided.")

        if not catalog_name or not schema_name or not table_name:
            raise ValueError("Catalog name, schema name, and table name must all be specified.")

        try:
            spark_df = self.spark.createDataFrame(pandas_df)
            
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            spark_df.write.format("delta") \
                .mode('overwrite') \
                .saveAsTable(full_table_name)

            logging.info(f"Data successfully written to Delta table: {full_table_name}")
        except Exception as e:
            logging.error(f"An error occurred while writing to the Delta table: {e}")
            raise Exception(f"An error occurred while writing to the Delta table: {e}")

    @staticmethod
    def normalize_column_names(df):
        """Normalize column names to lowercase with words separated by underscores."""
        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("A valid pandas DataFrame must be provided.")

        normalized_columns = [
            re.sub(r'\s+', '_', col.lower()).strip() for col in df.columns
        ]
        
        df.columns = normalized_columns
        logging.info("Column names normalized.")
