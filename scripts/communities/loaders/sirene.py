import sys
from pathlib import Path
import pandas as pd

utils_path = str(Path(__file__).resolve().parents[2] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from config import get_project_base_path

class SireneLoader():
    def __init__(self,config):
        base_path = get_project_base_path()
        data_folder = Path(base_path / config["path"])
        self.data = pd.read_csv( data_folder / config["filename"], usecols=config["columns"])
    
    def get(self):
        return self.data
