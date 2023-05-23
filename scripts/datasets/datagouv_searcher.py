import sys
from pathlib import Path
import pandas as pd

communities_path = str(Path(__file__).resolve().parents[1] /'communities')
if communities_path not in sys.path:
    sys.path.insert(0, communities_path)

from communities_selector import CommunitiesSelector

utils_path = str(Path(__file__).resolve().parents[2] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)


from files_operation import load_from_path, load_from_url, save_csv, download_and_process_data

class DataGouvSearcher():
    def __init__(self,config):
        self.scope = CommunitiesSelector(config["communities"])
        self.scope.filter() # Not elegant, we need to change this (only share the organization_ids for instance ?)

        self.dataset_catalog_df = load_from_url(config["datagouv"]["datasets"]["url"], columns_to_keep=config["datagouv"]["datasets"]["columns"])
        self.dataset_catalog_df = self.filter_organizations_by_id(self.dataset_catalog_df,self.scope.get_datagouv_ids_list())

        self.datafile_catalog_df = load_from_url(config["datagouv"]["datafiles"]["url"])
        self.datafile_catalog_df.columns=list(map(lambda x: x.replace("dataset.organization_id","organization_id"), self.datafile_catalog_df.columns.to_list()))
        self.datafile_catalog_df = self.filter_organizations_by_id(self.datafile_catalog_df,self.scope.get_datagouv_ids_list())
    
    def filter_organizations_by_id(self, df, organization_ids, return_mask=False):
        mask = df['organization_id'].isin(organization_ids)
        return mask if return_mask else df[mask]
