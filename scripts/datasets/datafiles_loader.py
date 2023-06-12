import collections
import logging
import pandas as pd

from files_operation import load_from_url


class DatafilesLoader():
    def __init__(self,files_in_scope,config):
        self.logger = logging.getLogger(__name__)
        self.files_in_scope = files_in_scope
        self.schema = self.load_schema(config)
        readable_files, self.datafiles_out = self.keep_readable_datafiles()
        self.corpus = self.load_datafiles(readable_files, config)
        self.normalized_data, self.datacolumns_out = self.normalize_data(config)

    def load_schema(self,config):
        json_schema = load_from_url(config["search"]["subventions"]["schema"]["url"])
        schema_df = pd.DataFrame(json_schema["fields"])
        return schema_df

    def keep_readable_datafiles(self):
        preferred_formats = ["csv", "xls", "json", "zip"] # Could be put outside

        readable_files = self.files_in_scope[self.files_in_scope["format"].isin(preferred_formats)]
        datafiles_out = self.files_in_scope[~self.files_in_scope["format"].isin(preferred_formats)]
        return readable_files, datafiles_out

    def load_datafiles(self, readable_files, config):
        len_out = len(self.datafiles_out)
        data = []
        file_info_columns = config["datafile_loader"]["file_info_columns"] 

        for index, row in readable_files.iterrows():
            url = row["url"]
            df = load_from_url(url)
            if df is not None:
                if isinstance(df, list):
                    df = pd.DataFrame(df)
                
                if isinstance(df, dict):
                    for key, value in df.items():
                        if isinstance(value, collections.abc.Collection):
                            self.logger.info(f"Length of array for key {key}: {len(value)}")
                        else:
                            self.logger.info(f"Value for key {key} is not a collection, it's a(n) {type(value).__name__}")
                    try:
                        df = pd.DataFrame(df)
                    except ValueError as ve:
                        self.logger.error(f"Error while converting dict to DataFrame: {ve}")
                        self.logger.error(f"Failed dict: {df}")
                        self.logger.error(f"Failed url: {url}")
                        self.datafiles_out = self.datafiles_out.append(row)
                        continue
                    
                if df.empty:
                    self.logger.warning(f"Data from {url} is empty.")
                    self.datafiles_out = self.datafiles_out.append(row)
                    self.logger.warning("Unable to load file %s", url)
                    continue
                
                for col in file_info_columns:
                    if col not in row:
                        self.logger.warning("Column %s not found in readable_files", col)
                        continue
                    df[col] = row[col]
                data.append(df)
            else:
                # Add to self.datafiles_out
                self.datafiles_out = self.datafiles_out.append(row)
                self.logger.warning("Unable to load file %s", url)

        self.logger.info("Number of dataframes loaded: %s", len(data))
        self.logger.info("Number of elements in data that are not dataframes: %s", sum([not isinstance(df, pd.DataFrame) for df in data]))
        self.logger.info("Number of files not loaded: %s", len(self.datafiles_out)-len_out)
        return data
    
    def normalize_data(self, config):
        len_out = len(self.datafiles_out)
        file_info_columns = config["datafile_loader"]["file_info_columns"]
        normalized_data = pd.DataFrame(columns=self.schema["name"])
        for col in file_info_columns:
            normalized_data[col] = ""
        schema_lower = [col.lower() for col in self.schema["name"].values]

        # Create a mapping dictionary between lower case schema names and original schema names
        schema_mapping = dict(zip(schema_lower, self.schema["name"].values))
        
        datacolumns_out = pd.DataFrame(columns=["filename", "column_name", "column_type", "nb_non_null_values"])

        for df in self.corpus:
            df.columns = df.columns.astype(str)
            columns_lower = [col.lower() for col in df.columns]
            # Check if the dataframe has at least 1 column in common with the schema
            if len(set(columns_lower).intersection(set(schema_lower))) > 0:
                common_columns = []
                for col, col_lower in zip(df.columns, columns_lower):
                    if col_lower in schema_lower or col in file_info_columns:
                        common_columns.append(col)
                    else:
                        datacolumns_out = datacolumns_out.append({"filename":df["url"].iloc[0], "column_name":col, "column_type":df[col].dtype, "nb_non_null_values":df[col].count()}, ignore_index=True)
                
                df_filtered = df[common_columns]

                # Rename the columns in df_filtered using the schema_mapping
                df_filtered.columns = [schema_mapping[col.lower()] if col.lower() in schema_mapping else col for col in df_filtered.columns]
                # Append df_filtered to normalized_data
                normalized_data = normalized_data.append(df_filtered, ignore_index=True)

                self.logger.info("Normalized dataframe %s", df["url"].iloc[0])
                self.logger.info("Number of datapoints in normalized_data: %s", len(normalized_data))
                self.logger.info("Number of columns in schema: %s", len(common_columns) - len(file_info_columns))
                self.logger.info("Number of columns not in schema: %s", len(df.columns)-len(common_columns) + len(file_info_columns))
            else:
                self.datafiles_out = self.datafiles_out.append(df.iloc[0])
                self.logger.warning("No column in common with schema for file %s", df["url"].iloc[0])
        
        # Drop potential duplicates (same values for schema & siren columns)
        subset_columns = list(self.schema["name"].values)
        subset_columns.append("siren")
        normalized_data = normalized_data.drop_duplicates(subset=subset_columns, keep="first")

        self.logger.info("Number of datapoints in normalized_data: %s", len(normalized_data))
        self.logger.info("Number of columns in normalized_data: %s", len(normalized_data.columns))
        self.logger.info("Number of files not normalized: %s", len(self.datafiles_out)-len_out)
        self.logger.info("Number of columns in datacolumns_out: %s", len(datacolumns_out))
        self.logger.info("Number of NaN values in normalized_data, per column: %s", normalized_data.isna().sum())

        return normalized_data, datacolumns_out