import logging
from pathlib import Path
import pandas as pd

from scripts.datasets.datagouv_searcher import DataGouvSearcher
from scripts.datasets.single_urls_builder import SingleUrlsBuilder
from scripts.datasets.datafiles_loader import DatafilesLoader
from scripts.datasets.datafile_loader import DatafileLoader
from scripts.utils.psql_connector import PSQLConnector
from scripts.utils.config import get_project_base_path
from scripts.utils.files_operation import save_csv

class WorkflowManager:
    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.logger = logging.getLogger(__name__)

    def run_workflow(self):
        # Accéder à la clé "search"
        search_config = self.config['search']
        # Create blank dict to store dataframes that will be saved to the DB
        df_to_save = {}

        for topic, topic_config in search_config.items():
            if topic_config['source'] == 'multiple':
                datagouv_searcher = DataGouvSearcher(self.config["communities"], self.config["datagouv"])
                datagouv_topic_files_in_scope = datagouv_searcher.get_datafiles(topic_config)

                # Add communities to df_to_save
                df_to_save["communities"] = datagouv_searcher.scope.selected_data
        
                single_urls_builder = SingleUrlsBuilder(self.config["communities"])
                single_urls_topic_files_in_scope = single_urls_builder.get_datafiles(topic_config)
                topic_files_in_scope = pd.concat([datagouv_topic_files_in_scope, single_urls_topic_files_in_scope], ignore_index=True)

                topic_data_folder = Path(get_project_base_path()) / "data" / "datasets" / topic / "outputs"
                files_in_scope_filename = "files_in_scope.csv"
                save_csv(topic_files_in_scope, topic_data_folder, files_in_scope_filename, sep=";")


                # Build new object taking files_in_scope & self.config as inputs in init, to load the subventions_datafiles, normalize them and save them in a new folder.
                topic_datafiles = DatafilesLoader(topic_files_in_scope, topic, topic_config, self.config["datafile_loader"])
                # Save the normalized data in a csv file
                normalized_data_filename = "normalized_data.csv"
                save_csv(topic_datafiles.normalized_data, topic_data_folder, normalized_data_filename, sep=";")
                # Save the list of files that are not readable in a csv file
                datafiles_out_filename = "datafiles_out.csv"
                save_csv(topic_datafiles.datafiles_out, topic_data_folder, datafiles_out_filename, sep=";")
                # Save the list of files that have columns not in common with the schema in a csv file
                datacolumns_out_filename = "datacolumns_out.csv"

                save_csv(topic_datafiles.normalized_data, topic_data_folder, normalized_data_filename, sep=";")
                save_csv(topic_datafiles.datacolumns_out, topic_data_folder, datacolumns_out_filename, sep=";")
                save_csv(topic_datafiles.datafiles_out, topic_data_folder, datafiles_out_filename, sep=";")

                # Add topic_datafiles.normalized_data to df_to_save
                df_to_save[topic+"_normalized"] = topic_datafiles.normalized_data
            
            elif topic_config['source'] == 'single':        
                topic_data_folder = Path(get_project_base_path()) / "data" / "datasets" / topic / "outputs"
                topic_datafiles = DatafileLoader(self.config["communities"], topic_config)
                save_csv(topic_datafiles.normalized_data, topic_data_folder, normalized_data_filename, sep=";")
                save_csv(topic_datafiles.modifications_data, topic_data_folder, "modifications_data.csv", sep=";")

                # Add topic_datafiles.normalized_data to df_to_save
                df_to_save[topic+"_normalized"] = topic_datafiles.normalized_data
        
            
        ## Saving Data to the DB - /!\ Does not erase Data at the moment, need to agree on a rule /!\
        connector = PSQLConnector()
        connector.connect()
        for df_name, df in df_to_save.items():
            connector.save_df_to_sql(df, df_name)
