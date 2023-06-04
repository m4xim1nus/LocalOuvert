import yaml
import argparse
import sys
from pathlib import Path
import pandas as pd

utils_path = str(Path(__file__).resolve().parents[0] / 'scripts' /'datasets')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from datagouv_searcher import DataGouvSearcher

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument('filename')   
    args = parser.parse_args()
    with open(args.filename) as f:
        # use safe_load instead load
        config = yaml.safe_load(f)
    datagouv = DataGouvSearcher(config)
    topdown_out = datagouv.get_datasets_by_title_and_desc(config["search"]["subventions"]["title_filter"],config["search"]["subventions"]["description_filter"])
    
    #print topdown_out basic info - Données avec config.yaml
    # print(f"Nombre de datasets correspondant au filtre de titre ou de description : {topdown_out.id.nunique()}")
    # print(f"Nombre de fichiers : {topdown_out.shape[0]}")
    # print(f"Nombre de fichiers uniques : {topdown_out.url.nunique()}")
    # print(f"Nombre de fichiers par format : {topdown_out.groupby('format').size().to_dict()}")
    # print(f"Nombre de fichiers par fréquence : {topdown_out.groupby('frequency').size().to_dict()}")

    #Bottom-up search: not working
    # datagouv.get_datasets_by_content(config["search"]["subventions"]["column_filter"],config["search"]["subventions"]["content_filter"],file_title_filter="asso")
