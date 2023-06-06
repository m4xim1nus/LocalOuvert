import logging
from pathlib import Path
import sys


utils_path = str(Path(__file__).resolve().parents[2] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_url

# Configure the logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DatafilesLoader():
    def __init__(self,files_list,config):
        self.files_list = files_list
        self.schema = self.load_schema()
        readable_files, self.datafiles_out = self.keep_readable_datafiles()
        self.corpus = self.load_datafiles(readable_files)
        self.normalized_data, self.datacolumns_out = self.normalize_data()

    def load_schema(self,config):
        # Take url from config to load json that contains the schema
        # url in config["search"]["subventions"]["schema"]["url"]
        # Return the schema (name of columns, type of columns) as a dict
        pass

    def keep_readable_datafiles(self):
        # Based on files_list, keep only the files that are readable (v1 : csv // v2 :  xls, xlsx, json ?)
        # Return the list of readable files and the list of files that are not readable
        pass

    def load_datafiles(self, readable_files):
        # Load the datafiles in a list of dataframes
        # Return the list of dataframes
        data = []
        for index, row in self.files_list.iterrows():
            try:
                data.append(load_from_url(row["url"], dtype=self.config["dtype"]))
            except Exception as e:
                logger.error("Error loading datafile %s", row["url"], exc_info=True)
        return data
    
    def normalize_data(self):
        # Based on schema, normalize the dataframes of self.corpus
        # Merge the normalized dataframes in one dataframe
        # If a dataframe has 0 column in common with the schema, add the file to self.datafiles_out
        # If a dataframe has 1 or more columns not in common with the schema, add the file to self.datacolumns_out (a dataframe with the name of the file and the name of the columns not in common with the schema, the type of the columns, and the number of non null values in the column)
        # Drop potential duplicates (potentially with different values in the columns not in schema, eg filename, url, etc)
        # Return 3 dataframes : normalized_data, datafiles_out, datacolumns_out
        pass
    
