from pathlib import Path
import pandas as pd
from typing import Optional, Dict, Any

from files_operation import save_csv
from scripts.loaders.base_loader import BaseLoader
from config import get_project_base_path

from scripts.utils.data_schema import OdfSchema
from scripts.utils.data_columns import OdfDF

class OdfLoader:
    def __init__(self, config: Dict[str, Any]) -> None:
        data_folder: Path = Path(get_project_base_path()) / config["processed_data"]["path"]
        data_file: Path = data_folder / config["processed_data"]["filename"]
        
        if data_file.exists():
            odf_data: pd.DataFrame = pd.read_csv(data_file, sep=";")
        else:
            odf_data_loader: BaseLoader = BaseLoader.loader_factory(config["url"], dtype=config["dtype"])
            odf_data: pd.DataFrame = odf_data_loader.load()
        
        # Making sure we trim everything in the SIREN column
        odf_data[OdfDF.siren] = odf_data[OdfDF.siren].str.replace('[^0-9]', '', regex=True)
        
        validated_odf_data: pd.DataFrame = OdfSchema.validate(odf_data)
        self.data = validated_odf_data
        
        if not data_file.exists():
            # Save the processed data to a CSV file
            self.save(Path(config["processed_data"]["path"]), config["processed_data"]["filename"])
    
    def get(self) -> pd.DataFrame:
        return self.data
    
    def save(self, path: Path, filename: str, sep: str = ";", index: bool = True) -> None:
        save_csv(self.data, path, filename, sep=sep, index=index)