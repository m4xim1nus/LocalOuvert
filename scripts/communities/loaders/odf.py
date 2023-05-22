import sys
from pathlib import Path

utils_path = str(Path(__file__).resolve().parents[2] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_path, load_from_url, save_csv, download_and_process_data
from config import get_project_base_path

class OdfLoader():
    def __init__(self,config):
        print(config)
        odf_data = load_from_url(config["url"], dtype=config["dtype"])
        self.data = odf_data

    def get(self):
        return self.data
    
    def save(self):
        save_csv(self.data,config["processed_data_folder"],f"ofgl_data_{datetime.now().strftime('%Y')}.csv")
