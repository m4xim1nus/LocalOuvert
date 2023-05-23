import yaml
import argparse
import sys
from pathlib import Path

import pandas as pd
utils_path = str(Path(__file__).resolve().parents[0] / 'scripts' /'datasets')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from datagouv_searcher import DataGouvSearcher

utils_path = str(Path(__file__).resolve().parents[0] / 'scripts' /'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_path, load_from_url, save_csv, download_and_process_data
from config import get_project_base_path

if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Gestionnaire du projet LocalOuvert")
    parser.add_argument('filename')   
    args = parser.parse_args()
    with open(args.filename) as f:
        # use safe_load instead load
        config = yaml.safe_load(f)
    datagouv = DataGouvSearcher(config)
