import yaml
import argparse
import sys
from pathlib import Path
import pandas as pd

utils_path = str(Path(__file__).resolve().parents[0] / 'scripts' /'datasets')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from datagouv_searcher import DataGouvSearcher

# utils_path = str(Path(__file__).resolve().parents[0] / 'scripts' /'utils')
# if utils_path not in sys.path:
#     sys.path.insert(0, utils_path)
# from files_operation import load_from_path, load_from_url, save_csv, download_and_process_data
# from config import get_project_base_path

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument('filename')   
    args = parser.parse_args()
    with open(args.filename) as f:
        # use safe_load instead load
        config = yaml.safe_load(f)
    datagouv = DataGouvSearcher(config)
    topdown_out = datagouv.get_datasets_by_title_and_desc(config["search"]["subventions"]["title_filter"],config["search"]["subventions"]["description_filter"])
    
#     #print topdown_out basic info
#     print(f"Nombre de datasets correspondant au filtre de titre ou de description : {topdown_out.id.nunique()}")
#     print(f"Nombre de fichiers : {topdown_out.shape[0]}")
#     print(f"Nombre de fichiers uniques : {topdown_out.url.nunique()}")
#     print(f"Nombre de fichiers par format : {topdown_out.groupby('format').size().to_dict()}")
#     print(f"Nombre de fichiers par fréquence : {topdown_out.groupby('frequency').size().to_dict()}")
#     INFO:datagouv_searcher:Nombre de datasets correspondant au filtre de description : 201
#     INFO:datagouv_searcher:Nombre de datasets correspondant au filtre de titre : 154
#     Nombre de datasets correspondant au filtre de titre ou de description : 232
#     Nombre de fichiers : 504
#     Nombre de fichiers uniques : 468
#     Nombre de fichiers par format : {'arcgis geoservices rest api': 2, 'csv': 206, 'do?id=jorftext000036040528': 3, 'excel': 1, 'geojson': 7, 'html': 1, 'json': 126, 'odata': 1, 'ods': 2, 'page web': 9, 'pdf': 32, 'shp': 11, 'web page': 13, 'xlb': 2, 'xls': 46, 'xlsx': 31, 'zip': 1}
#     Nombre de fichiers par fréquence : {'annual': 93, 'irregular': 4, 'monthly': 7, 'punctual': 8, 'quarterly': 21, 'semiannual': 2, 'unknown': 296, 'weekly': 2}


    datagouv.get_datasets_by_content(config["search"]["subventions"]["column_filter"],config["search"]["subventions"]["content_filter"],file_title_filter="asso")
