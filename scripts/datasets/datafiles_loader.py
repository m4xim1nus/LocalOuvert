import logging
from pathlib import Path
import sys
import pandas as pd


utils_path = str(Path(__file__).resolve().parents[2] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_url

# Configure the logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DatafilesLoader():
    def __init__(self,files_in_scope,config):
        self.files_in_scope = files_in_scope
        self.schema = self.load_schema(config)
        readable_files, self.datafiles_out = self.keep_readable_datafiles()
        self.corpus = self.load_datafiles(readable_files)
        # self.normalized_data, self.datacolumns_out = self.normalize_data()

    def load_schema(self,config):
        json_schema = load_from_url(config["search"]["subventions"]["schema"]["url"])
        schema_df = pd.DataFrame(json_schema["fields"])
        return schema_df

    def keep_readable_datafiles(self):
        preferred_formats = ["csv", "xls", "json", "zip"] # Could be put outside

        readable_files = self.files_in_scope[self.files_in_scope["format"].isin(preferred_formats)]
        datafiles_out = self.files_in_scope[~self.files_in_scope["format"].isin(preferred_formats)]
        return readable_files, datafiles_out

    def load_datafiles(self, readable_files):
        len_out = len(self.datafiles_out)
        data = []
        for index, row in readable_files.iterrows():
            url = row["url"]
            siren = row["SIREN"]
            organization = row["organization"]
            title = row["title"]
            created_at = row["created_at"]

            df = load_from_url(url)
            if df is not None:
                if isinstance(df, list):
                    logger.warning("load_from_url returned a list for url %s, expected a DataFrame", url)
                    continue
                for col in ["SIREN", "organization", "title", "created_at", "url"]:
                    if col not in row:
                        logger.warning("Column %s not found in readable_files", col)
                        continue
                    df[col] = row[col]
                data.append(df)
            else:
                # Add to self.datafiles_out
                self.datafiles_out = self.datafiles_out.append(row)
                logger.warning("Unable to load file %s", url)

        # Perform checks on overall function via prints on data and self.datafiles_out:
        logger.info("Number of dataframes loaded: %s", len(data))
        logger.info("Number of files not loaded: %s", len(self.datafiles_out)-len_out)
        return data
    
    def normalize_data(self):
        # Based on schema, normalize the dataframes of self.corpus
        # Merge the normalized dataframes in one dataframe
        # If a dataframe has 0 column in common with the schema, add the file to self.datafiles_out
        # If a dataframe has 1 or more columns not in common with the schema, add the file to self.datacolumns_out (a dataframe with the name of the file and the name of the columns not in common with the schema, the type of the columns, and the number of non null values in the column)
        # Drop potential duplicates (potentially with different values in the columns not in schema, eg filename, url, etc)
        # Return 3 dataframes : normalized_data, datafiles_out, datacolumns_out
        pass
    
