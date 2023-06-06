import yaml
import argparse
import sys
from pathlib import Path
import pandas as pd

utils_path = str(Path(__file__).resolve().parents[0] / 'scripts' /'datasets')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from datagouv_searcher import DataGouvSearcher
from datafiles_loader import DatafilesLoader
from config import get_project_base_path
from files_operation import save_csv

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument('filename')   
    args = parser.parse_args()
    with open(args.filename) as f:
        # use safe_load instead load
        config = yaml.safe_load(f)
    datagouv = DataGouvSearcher(config)

    files_list = datagouv.get_datafiles(config["search"]["subventions"])
    data_folder = Path(get_project_base_path()) / "data" / "datasets"
    fileslist_filename = "files_list.csv"
    save_csv(files_list, data_folder, fileslist_filename, sep=";")

    # # Build new object taking files_list & config as inputs in init, to load the datafiles, normalize them and save them in a new folder. Class is called DatafilesLoader
    # datafiles = DatafilesLoader(files_list,config)
    # # Save the normalized data in a csv file
    # normalized_data_filename = "normalized_data.csv"
    # save_csv(datafiles.normalized_data, data_folder, normalized_data_filename, sep=";")
    # # Save the list of files that are not readable in a csv file
    # datafiles_out_filename = "datafiles_out.csv"
    # save_csv(datafiles.datafiles_out, data_folder, datafiles_out_filename, sep=";")
    # # Save the list of files that have columns not in common with the schema in a csv file
    # datacolumns_out_filename = "datacolumns_out.csv"
    # save_csv(datafiles.datacolumns_out, data_folder, datacolumns_out_filename, sep=";")


