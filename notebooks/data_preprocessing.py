import os
import yaml
import logging
from pyspark.sql import SparkSession

from src.helpers.data_handler import DataHandler
from src.utils.model_config import ModelConfig
from src.utils.logging_config import setup_logging
from src.helpers.data_preprocessor_helper import DataPreprocessor

setup_logging()
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

config = ModelConfig.from_yaml(config_path="../project_config.yml")

logger.info("Model configuration loaded")

data_handler_instance = DataHandler(filepath=config.data_source, 
                           spark_session=spark)

data_input = data_handler_instance.load_data()

data_preprocess_instance = DataPreprocessor(data_input)








