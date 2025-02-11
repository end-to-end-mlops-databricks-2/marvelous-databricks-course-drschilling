import pandas 
import logging
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from src.utils.logging_config import setup_logging

setup_logging()

class DataPreprocessor:
    
    def __init__(self, pandas_dataframe):
        """Initialize the preprocessor with the dataset file path."""
        self.data = pandas_dataframe
        self.logger = logging.getLogger(__name__)

    def indexing__data(self):
        """Formatting the datetime and setting it as a index."""
        self.data['datetime'] = pandas.to_datetime(self.data['datetime'], format='%d/%m/%Y %H:%M:%S')
        self.data.set_index('datetime', inplace=True)
        self.logger.info("Formatting datetime and estabilishing as an index")

    def date_engineering(self):
        """Create additional date representations from the existing data."""
        self.data['Hour'] = self.data.index.hour
        self.data['Day'] = self.data.index.dayofweek
        self.data['Month'] = self.data.index.month
        self.logger.info("New date features created from datetime")

    def scale_features_sets(self, data_split):
        """Scale features sets using Min-Max scaling."""
        scaler = MinMaxScaler()
        feature_columns = ['temperature', 'humidity', 'wind_speed', 
                           'general_diffuse_flows', 'diffuse_flows']
        
        data_split[feature_columns] = scaler.fit_transform(data_split[feature_columns])

    def split_data(self, test_size=0.2, random_state=42):
        """Split the dataset into training and testing sets."""
        X = self.data.drop(columns=['zone_1_power_consumption', 
                                     'zone_2_power_consumption', 
                                     'zone_3_power_consumption'])
        
        y = self.data[['zone_1_power_consumption', 
                       'zone_2_power_consumption', 
                       'zone_3_power_consumption']]
        
        return train_test_split(X, y, test_size=test_size, random_state=random_state)
