import logging
import yaml
import argparse
import sys
from pathlib import Path
import pandas as pd

# Pré-nettoyage des ajouts à sys_path : à sortir dans config & utils (?)
def add_to_sys_path(path: str):
    if path not in sys.path:
        sys.path.insert(0, path)

base_path = Path(__file__).resolve().parents[0] / 'scripts'
add_to_sys_path(str(base_path / 'datasets'))
add_to_sys_path(str(base_path / 'utils'))
add_to_sys_path(str(base_path / 'communities'))
add_to_sys_path(str(base_path / 'communities' / 'loaders'))

from datagouv_searcher import DataGouvSearcher
from datafiles_loader import DatafilesLoader
from single_urls_builder import SingleUrlsBuilder
from config import get_project_base_path
from files_operation import save_csv
from logger import configure_logger

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument('filename')   
    args = parser.parse_args()
    with open(args.filename) as f:
        # use safe_load instead load
        config = yaml.safe_load(f)

    configure_logger(config)    

    datagouv = DataGouvSearcher(config)
    datagouv_files_in_scope = datagouv.get_datafiles(config["search"]["subventions"])

    single_urls = SingleUrlsBuilder(config)
    single_urls_files_in_scope = single_urls.get_datafiles(config["search"]["subventions"])
    files_in_scope = pd.concat([datagouv_files_in_scope, single_urls_files_in_scope], ignore_index=True)

    data_folder = Path(get_project_base_path()) / "data" / "datasets" / "subventions" / "outputs"
    files_in_scope_filename = "files_in_scope.csv"
    save_csv(files_in_scope, data_folder, files_in_scope_filename, sep=";", index=True)

    # Build new object taking files_in_scope & config as inputs in init, to load the datafiles, normalize them and save them in a new folder.
    datafiles = DatafilesLoader(files_in_scope,config)
    # Save the normalized data in a csv file
    normalized_data_filename = "normalized_data.csv"
    save_csv(datafiles.normalized_data, data_folder, normalized_data_filename, sep=";", index=True)
    # Save the list of files that are not readable in a csv file
    datafiles_out_filename = "datafiles_out.csv"
    save_csv(datafiles.datafiles_out, data_folder, datafiles_out_filename, sep=";", index=True)
    # Save the list of files that have columns not in common with the schema in a csv file
    datacolumns_out_filename = "datacolumns_out.csv"
    save_csv(datafiles.datacolumns_out, data_folder, datacolumns_out_filename, sep=";", index=True)