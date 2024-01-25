from pathlib import Path
import pandas as pd

from scripts.utils.config import get_project_base_path

class SireneLoader():
    def __init__(self,config):
        base_path = get_project_base_path()
        data_folder = Path(base_path / config["path"])
        self.data = pd.read_csv( data_folder / config["filename"], usecols=config["columns"])
    
    def get(self):
        return self.data
