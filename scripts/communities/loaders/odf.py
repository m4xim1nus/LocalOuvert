from scripts.utils.data_schema import OdfSchema
from scripts.utils.data_columns import OdfDF
from scripts.utils.loader import BaseLoader
from scripts.utils.file_utils import save_csv
from typing import Dict, Any
from pathlib import Path
import pandas as pd
from utils import get_project_base_path

class OdfLoader:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.load_data()
    
    def load_data(self) -> None:
        data_folder = Path(get_project_base_path()) / self.config["processed_data"]["path"]
        data_file = data_folder / self.config["processed_data"]["filename"]
        
        if data_file.exists():
            self.data = pd.read_csv(data_file, sep=";")
        else:
            odf_data_loader = BaseLoader.loader_factory(self.config["url"], dtype=self.config["dtype"])
            self.data = odf_data_loader.load()
        
        # Making sure we trim everything in the SIREN column so it can be an Int
        self.data[OdfDF.siren] = self.data[OdfDF.siren].str.replace('[^0-9]', '', regex=True)
        
        validated_odf_data = OdfSchema.validate(self.data)
        self.data = validated_odf_data
        
        if not data_file.exists():
            self.save_data()
    
    def get(self) -> pd.DataFrame:
        return self.data
    
    def save_data(self) -> None:
        data_folder = Path(get_project_base_path()) / self.config["processed_data"]["path"]
        save_csv(self.data, data_folder, self.config["processed_data"]["filename"], sep=";", index=True)
