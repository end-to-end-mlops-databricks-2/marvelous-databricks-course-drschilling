from typing import List

import yaml
from pydantic import BaseModel


class ModelConfig(BaseModel):
    data_source: str
    numeric_features: List[str]
    temporal_features: str
    targets: List[str]
    catalog_name: str
    schema_name: str

    @classmethod
    def from_yaml(cls, config_path: str):
        """Load configuration from a YAML file."""
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
        return cls(**config_dict)
