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
        self.datagouv_ids_list = self.scope.get_datagouv_ids_list()

        self.dataset_catalog_df = load_from_url(config["datagouv"]["datasets"]["url"], columns_to_keep=config["datagouv"]["datasets"]["columns"])
        self.dataset_catalog_df = self.filter_organizations_by_id(self.dataset_catalog_df,self.datagouv_ids_list)

        self.datafile_catalog_df = load_from_url(config["datagouv"]["datafiles"]["url"])
        self.datafile_catalog_df.columns=list(map(lambda x: x.replace("dataset.organization_id","organization_id"), self.datafile_catalog_df.columns.to_list()))
        self.datafile_catalog_df = self.filter_organizations_by_id(self.datafile_catalog_df,self.datagouv_ids_list)
    
    def filter_organizations_by_id(self, df, organization_ids, return_mask=False):
        mask = df['organization_id'].isin(organization_ids)
        return mask if return_mask else df[mask]

    def filter_description(self, df, pattern, return_mask=False):
        mask = df['description'].str.contains(pattern, case=False, na=False)
        return mask if return_mask else df[mask]

    def filter_titles(self, df, pattern, return_mask=False):
        mask = df['title'].str.contains(pattern, case=False, regex=True, na=False)
        return mask if return_mask else df[mask]

    
    def get_datasets_by_title_and_desc(self,title_filter,description_filter):
        # First we get the masks on the data:
        mask_desc = self.filter_description(self.dataset_catalog_df, description_filter, return_mask=True)
        print(f"Nombre de datasets correspondant au filtre de description : {mask_desc.sum()}")

        mask_titles = self.filter_titles(self.dataset_catalog_df, title_filter, return_mask=True)
        print(f"Nombre de datasets correspondant au filtre de titre : {mask_titles.sum()}")

        filtered_catalog_df = self.dataset_catalog_df[(mask_titles | mask_desc)]

        # Now we merge with files 
        filtered_files = filtered_catalog_df[["id","title","description","organization","frequency"]].merge(self.datafile_catalog_df[["dataset.id","format","created_at","url"]],left_on="id",right_on="dataset.id",how="left")
        filtered_files.drop(columns=['dataset.id'], inplace=True)
        # Food for thought, do we need id from datafile ?

        return filtered_files
    
    def check_columns(self,dataframe, columns):
        lowercase_columns = [col.lower() for col in dataframe.columns]
        lowercase_check = [col.lower() in lowercase_columns for col in columns]
        return all(lowercase_check)


    def get_datasets_by_content(self,column_filter,content_filter,file_title_filter=None, formats=["csv"]):
        ### NOT WORKING ### 
        # For the moment we do it only on csv to be extended
        if file_title_filter:
            files_df = self.datafile_catalog_df[self.datafile_catalog_df.title.str.contains(file_title_filter,case=False, na=False)] 
        else:
            files_df = self.datafile_catalog_df

        for df_index, line_data in files_df.iterrows():
            if line_data.format in formats:
                content = load_from_url(line_data.url)
                if (isinstance(content,pd.DataFrame)) and self.check_columns(content,column_filter):
                    for column in content.columns:
                        if content[column].dtype == 'object' and content[column].str.contains('|'.join(content_filter)).any():
                            print("SUCCESS")
            else:
                print("OUT OF SCOPE FORMAT")
        


