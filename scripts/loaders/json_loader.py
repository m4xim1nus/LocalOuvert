import pandas as pd
import json

from base_loader import BaseLoader

class JSONLoader(BaseLoader):
    def __init__(self, file_url, normalize=False, **kwargs):
        super().__init__(file_url, **kwargs)
        self.normalize = normalize

    def process_data(self, data):
        json_data = json.loads(data.decode('utf-8'))
        if self.normalize:
            return pd.json_normalize(json_data)
        else:
            return pd.DataFrame(json_data)
