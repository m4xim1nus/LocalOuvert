from pathlib import Path
import pandas as pd

from scripts.utils.argument_parser import ArgumentParser
from scripts.utils.config_manager import ConfigManager
from scripts.utils.logger_manager import LoggerManager

from scripts.datasets.datagouv_searcher import DataGouvSearcher
from scripts.datasets.single_urls_builder import SingleUrlsBuilder
from scripts.datasets.datafiles_loader import DatafilesLoader
from scripts.datasets.datafile_loader import DatafileLoader
from scripts.utils.psql_connector import PSQLConnector
from scripts.utils.config import get_project_base_path
from scripts.utils.files_operation import save_csv

if __name__ == "__main__":
    # Parse arguments, load config and configure logger
    args = ArgumentParser.parse_args("Gestionnaire du projet LocalOuvert")
    config = ConfigManager.load_config(args.filename)        
    LoggerManager.configure_logger(config)



    datagouv = DataGouvSearcher(config)
    single_urls = SingleUrlsBuilder(config)
    
    datagouv_subventions_files_in_scope = datagouv.get_datafiles(config["search"]["subventions"])

    single_urls_subventions_files_in_scope = single_urls.get_datafiles(config["search"]["subventions"])
    subventions_files_in_scope = pd.concat([datagouv_subventions_files_in_scope, single_urls_subventions_files_in_scope], ignore_index=True)

    subventions_data_folder = Path(get_project_base_path()) / "data" / "datasets" / "subventions" / "outputs"
    files_in_scope_filename = "files_in_scope.csv"
    save_csv(subventions_files_in_scope, subventions_data_folder, files_in_scope_filename, sep=";")


    # Build new object taking files_in_scope & config as inputs in init, to load the subventions_datafiles, normalize them and save them in a new folder.
    subventions_datafiles = DatafilesLoader(subventions_files_in_scope,config)
    # Save the normalized data in a csv file
    normalized_data_filename = "normalized_data.csv"

    save_csv(subventions_datafiles.normalized_data, subventions_data_folder, normalized_data_filename, sep=";")
    # Save the list of files that are not readable in a csv file
    datafiles_out_filename = "datafiles_out.csv"
    save_csv(subventions_datafiles.datafiles_out, subventions_data_folder, datafiles_out_filename, sep=";")
    # Save the list of files that have columns not in common with the schema in a csv file
    datacolumns_out_filename = "datacolumns_out.csv"

    save_csv(subventions_datafiles.normalized_data, subventions_data_folder, normalized_data_filename, sep=";")
    save_csv(subventions_datafiles.datacolumns_out, subventions_data_folder, datacolumns_out_filename, sep=";")
    save_csv(subventions_datafiles.datafiles_out, subventions_data_folder, datafiles_out_filename, sep=";")
    
    marches_publics_data_folder = Path(get_project_base_path()) / "data" / "datasets" / "marches_publics" / "outputs"
    marches_publics = DatafileLoader(config)
    save_csv(marches_publics.normalized_data, marches_publics_data_folder, normalized_data_filename, sep=";")
    save_csv(marches_publics.modifications_data, marches_publics_data_folder, "modifications_data.csv", sep=";")
    
        
    ## Saving Data to the DB - /!\ Does not erase Data at the moment, need to agree on a rule /!\
    connector = PSQLConnector()
    connector.connect()
    connector.save_df_to_sql(datagouv.scope.selected_data,"communities")
    connector.save_df_to_sql(subventions_datafiles.normalized_data,"subventions_normalized")
    connector.save_df_to_sql(marches_publics.normalized_data,"marches_public_normalized")
    


