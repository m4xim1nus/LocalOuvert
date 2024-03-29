from pathlib import Path
import pandas as pd

from files_operation import save_csv
from scripts.loaders.base_loader import BaseLoader
from config import get_project_base_path

class OdfLoader():
    def __init__(self,config):
        data_folder = Path(get_project_base_path()) / config["processed_data"]["path"]
        data_file = data_folder / config["processed_data"]["filename"]
        if data_file.exists():
            self.data = pd.read_csv(data_file, sep=";")
        else:
            odf_data_loader = BaseLoader.loader_factory(config["url"], dtype=config["dtype"])
            odf_data = odf_data_loader.load()
            self.data = odf_data
            self.save(Path(config["processed_data"]["path"]),config["processed_data"]["filename"])

    def get(self):
        return self.data
    
    def save(self,path,filename):
        save_csv(self.data,path,filename, sep=";", index=True)
