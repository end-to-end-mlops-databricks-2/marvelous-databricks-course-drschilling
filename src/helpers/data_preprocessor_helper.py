import pandas
import logging
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from src.utils.logging_config import setup_logging 

setup_logging() 

class DataPreprocessor:
    
    def __init__(self, pandas_dataframe):
        """Initialize the preprocessor with the dataset."""
        self.data = pandas_dataframe
        self.logger = logging.getLogger(__name__)

    def indexing_data(self):
        """Format the datetime column and set it as the index."""
        
        self.logger.info(self.data.columns)
        if isinstance(self.data['datetime'].iloc[0], str): 
            self.data['datetime'] = self.data['datetime'].str.strip()
            
        self.data['datetime'] = pandas.to_datetime(self.data['datetime'], infer_datetime_format=True, errors='coerce')

        self.data.set_index('datetime', inplace=True)

        self.logger.info("Datetime column formatted and set as index.")

    def date_engineering(self):
        """Create additional date features from the datetime index."""
        self.data['Hour'] = self.data.index.hour
        self.data['Day'] = self.data.index.dayofweek
        self.data['Month'] = self.data.index.month
        self.logger.info("New date features created from datetime index.")

    def scale_features_sets(self, X_train, X_test):
        """Scale feature sets using Min-Max scaling."""
        scaler = MinMaxScaler()
        feature_columns = ['temperature', 'humidity', 'wind_speed',
                           'general_diffuse_flows', 'diffuse_flows']

        X_train[feature_columns] = scaler.fit_transform(X_train[feature_columns])
        X_test[feature_columns] = scaler.transform(X_test[feature_columns])  
        self.logger.info("Features scaled using Min-Max scaling.")

        return X_train, X_test


    def split_data(self, test_size=0.2, random_state=42):
        """Split the dataset into training and testing sets."""
        X = self.data.drop(columns=['zone_1_power_consumption',
                                     'zone_2_power_consumption',
                                     'zone_3_power_consumption'])

        y = self.data[['zone_1_power_consumption',
                       'zone_2_power_consumption',
                       'zone_3_power_consumption']]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
        self.logger.info(f"Data split into training and testing sets with test_size={test_size} and random_state={random_state}.")
        return X_train, X_test, y_train, y_test

    def preprocess_and_split(self, test_size=0.2, random_state=42):
        """
        Orchestrates the data preprocessing steps: indexing, date engineering,
        splitting, and scaling.
        """
        self.indexing_data()
        self.date_engineering()
        X_train, X_test, y_train, y_test = self.split_data(test_size, random_state)
        X_train, X_test = self.scale_features_sets(X_train, X_test) 
        self.logger.info("Data preprocessing and splitting complete.")
        return X_train, X_test, y_train, y_test