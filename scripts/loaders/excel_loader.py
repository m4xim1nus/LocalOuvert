import pandas as pd
from io import BytesIO
import logging

from .base_loader import BaseLoader
from scripts.utils.dataframe_operation import detect_skiprows, detect_skipcolumns

class ExcelLoader(BaseLoader):
    def __init__(self, file_url, dtype=None, columns_to_keep=None, **kwargs):
        super().__init__(file_url, **kwargs)
        self.dtype = dtype
        self.columns_to_keep = columns_to_keep

    def process_data(self, data):
        df = pd.read_excel(BytesIO(data), header=None, dtype=self.dtype)

        skiprows = detect_skiprows(df)
        skipcols = detect_skipcolumns(df)

        df = df.iloc[skiprows:, skipcols:]
        df.columns = df.iloc[0]
        df = df.drop(df.index[0]).reset_index(drop=True)

        if self.columns_to_keep is not None:
            df = df.loc[:, self.columns_to_keep]

        return df