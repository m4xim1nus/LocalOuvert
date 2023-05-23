import sys
from pathlib import Path
import pandas as pd

utils_path = str(Path(__file__).resolve().parents[2] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_path, load_from_url, save_csv, download_and_process_data
from config import get_project_base_path

class OdfLoader():
    def __init__(self,config):
        data_folder = Path(get_project_base_path()) / config["processed_data"]["path"]
        data_file = data_folder / config["processed_data"]["filename"]
        if data_file.exists():
            self.data = pd.read_csv(data_file)
        else:
            odf_data = load_from_url(config["url"], dtype=config["dtype"])
            self.data = odf_data
            self.save(Path(config["processed_data"]["path"]),config["processed_data"]["filename"])

    def get(self):
        return self.data
    
    def save(self,path,filename):
        save_csv(self.data,path,filename)
