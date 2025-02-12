import logging
from pyspark.sql import SparkSession

from src.helpers.data_handler import DataHandler
from src.utils.model_config import ModelConfig
from src.utils.logging_config import setup_logging
from src.helpers.data_preprocessor_helper import DataPreprocessor

# Logging setup 
setup_logging()
logger = logging.getLogger(__name__)

# Creating a spark session
spark = SparkSession.builder.getOrCreate()

# Loading the model config from yaml file
config = ModelConfig.from_yaml(config_path="../project_config.yml")

logger.info("Model configuration loaded")

#Intancing the data handler class 
data_handler_instance = DataHandler(filepath=config.data_source, 
                           spark_session=spark)

# Loading data from the volumes folder
data_input = data_handler_instance.load_data()

# Intancing the preprocessing class
data_preprocess_instance = DataPreprocessor(data_input)

# Executing the data preprocess and split
try:
    
    X_train, X_test, y_train, y_test = data_preprocess_instance.preprocess_and_split()
    
except Exception as e:
    logger.exception(f"An error occurred: {e}")






