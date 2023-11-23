import json
import requests
import pandas as pd
import logging

from communities_selector import CommunitiesSelector
from files_operation import load_from_url, load_json
from json_operation import flatten_json_schema, flatten_data

class DatafileLoader():
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)

        self.scope = CommunitiesSelector(config["communities"])
        self.schema = self.load_schema(config)
        self.normalized_data = self.load_data(config)

    def load_schema(self, config):
        json_schema = load_from_url(config["search"]["marches_publics"]["schema"]["url"])
        schema_name = config["search"]["marches_publics"]["schema"]["name"]
        flattened_schema = flatten_json_schema(json_schema, schema_name)
        schema_df = pd.DataFrame(flattened_schema)
        return schema_df
    
    def load_data(self, config):
        data = load_json(config["search"]["marches_publics"]["unified_dataset"]["url"])
        df = flatten_data(data['marches'])
        self.logger.info(f"Le fichier au format JSON a été téléchargé avec succès à l'URL : {config['search']['marches_publics']['unified_dataset']['url']}")
        return df