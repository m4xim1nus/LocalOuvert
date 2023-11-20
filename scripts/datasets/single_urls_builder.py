import logging
from pathlib import Path
import pandas as pd

from communities_selector import CommunitiesSelector
from config import get_project_base_path

class SingleUrlsBuilder():
    def __init__(self,config):
        self.logger = logging.getLogger(__name__)
        self.scope = CommunitiesSelector(config["communities"])

    def get_datafiles(self,search_config):
        single_urls_source_file = Path(get_project_base_path()) / "data" / "datasets" / "subventions" / "inputs" / search_config["single_urls_file"]
        single_urls_files_in_scope = pd.read_csv(single_urls_source_file, sep=";")
        selected_data = self.scope.selected_data
        # Add 'nom' & 'type' columns to single_urls_files_in_scope from selected_data based on siren
        single_urls_files_in_scope = single_urls_files_in_scope.merge(selected_data[['siren', 'nom', 'type']], on='siren', how='left')
        # Add new 'source' column, filled with 'single_url' value
        single_urls_files_in_scope['source'] = 'single_url'

        return single_urls_files_in_scope
