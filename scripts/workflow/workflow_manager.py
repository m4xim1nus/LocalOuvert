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
from scripts.utils.constants import FILES_IN_SCOPE_FILENAME, NORMALIZED_DATA_FILENAME, DATAFILES_OUT_FILENAME, DATACOLUMNS_OUT_FILENAME, MODIFICATIONS_DATA_FILENAME

class WorkflowManager:
    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.logger = logging.getLogger(__name__)

    def run_workflow(self):
        # Accéder à la clé "search"
        search_config = self.config['search']
        # Create blank dict to store dataframes that will be saved to the DB
        df_to_save_to_db = {}
        
        for topic, topic_config in search_config.items():

            if topic_config['source'] == 'multiple':
                datagouv_searcher = DataGouvSearcher(self.config["communities"], self.config["datagouv"])
                datagouv_topic_files_in_scope = datagouv_searcher.get_datafiles(topic_config)

                # Add communities to df_to_save
                df_to_save_to_db["communities"] = datagouv_searcher.scope.selected_data
        
                single_urls_builder = SingleUrlsBuilder(self.config["communities"])
                single_urls_topic_files_in_scope = single_urls_builder.get_datafiles(topic_config)
                topic_files_in_scope = pd.concat([datagouv_topic_files_in_scope, single_urls_topic_files_in_scope], ignore_index=True)

                # Build new object taking files_in_scope & self.config as inputs in init, to load the subventions_datafiles, normalize them and save them in a new folder.
                topic_datafiles = DatafilesLoader(topic_files_in_scope, topic, topic_config, self.config["datafile_loader"])
            
            elif topic_config['source'] == 'single':
                # Build new object taking files_in_scope & self.config as inputs in init, to load the subventions_datafiles, normalize them and save them in a new folder.
                topic_datafiles = DatafileLoader(self.config["communities"], topic_config)
                
            # Save the topics outputs to csv
            self.save_output_to_csv(topic, topic_datafiles.normalized_data, topic_files_in_scope, topic_datafiles.datacolumns_out, topic_datafiles.datafiles_out, topic_datafiles.modifications_data)
            
            # Add topic_datafiles.normalized_data to df_to_save
            df_to_save_to_db[topic+"_normalized"] = topic_datafiles.normalized_data
            
        ## Saving Data to the DB - /!\ Does not erase Data at the moment, need to agree on a rule /!\
        connector = PSQLConnector()
        connector.connect()
        for df_name, df in df_to_save_to_db.items():
            connector.save_df_to_sql(df, df_name)

    def save_output_to_csv(self, topic, normalized_data, topic_files_in_scope=None, datacolumns_out=None, datafiles_out=None, modifications_data=None):
        output_folder = Path(get_project_base_path()) / "data" / "datasets" / topic / "outputs"

        # Loop through the dataframes (if not None) to save them to the output folder
        if normalized_data is not None:
            save_csv(normalized_data, output_folder, NORMALIZED_DATA_FILENAME, sep=";")
        if topic_files_in_scope is not None:
            save_csv(topic_files_in_scope, output_folder, FILES_IN_SCOPE_FILENAME, sep=";")
        if datacolumns_out is not None:
            save_csv(datacolumns_out, output_folder, DATACOLUMNS_OUT_FILENAME, sep=";")
        if datafiles_out is not None:
            save_csv(datafiles_out, output_folder, DATAFILES_OUT_FILENAME, sep=";")
        if modifications_data is not None:
            save_csv(modifications_data, output_folder, MODIFICATIONS_DATA_FILENAME, sep=";")