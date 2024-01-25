import collections
import logging
import pandas as pd
from pathlib import Path

from scripts.utils.config import get_project_base_path

from scripts.loaders.csv_loader import CSVLoader
from scripts.loaders.excel_loader import ExcelLoader
from scripts.loaders.json_loader import JSONLoader
from scripts.utils.dataframe_operation import merge_duplicate_columns, safe_rename, cast_data


class DatafilesLoader():
    def __init__(self,files_in_scope, topic, topic_config, datafile_loader_config):
        self.logger = logging.getLogger(__name__)

        self.loader_classes = {
            'csv': CSVLoader,
            'xls': ExcelLoader,
            'xlsx': ExcelLoader,
            'excel': ExcelLoader,
            'json': JSONLoader,
        }

        self.files_in_scope = files_in_scope
        self.schema = self.load_schema(topic_config["schema"])
        self.datafiles_out = pd.DataFrame()
        readable_files, self.datafiles_out = self.keep_readable_datafiles()
        self.corpus = self.load_datafiles(readable_files, datafile_loader_config)
        self.normalized_data, self.datacolumns_out = self.normalize_data(topic, topic_config, datafile_loader_config)

    def load_schema(self, schema_topic_config):
        json_schema_loader = JSONLoader(schema_topic_config["url"], key="fields") # Impr : "subentions" & "fields" should be variables
        schema_df = json_schema_loader.load()
        self.logger.info("Schema loaded.")
        return schema_df

    def keep_readable_datafiles(self):
        preferred_formats = ["csv", "xls", "xlsx", "json", "zip"] # Impr: keep outside of the class

        readable_files = self.files_in_scope[self.files_in_scope["format"].isin(preferred_formats)]
        datafiles_out = self.files_in_scope[~self.files_in_scope["format"].isin(preferred_formats)]
        self.logger.info(f"{len(readable_files)} readable files selected.")
        return readable_files, datafiles_out

    def load_file_data(self, file_info, datafile_loader_config):
        loader_class = self.loader_classes.get(file_info["format"].lower())
        if loader_class:
            loader = loader_class(file_info["url"])
            try:
                df = loader.load()
                if not df.empty:
                    for col in datafile_loader_config["file_info_columns"]:
                        if col in file_info:
                            df[col] = file_info[col]
                    self.logger.info(f"Data from {file_info['url']} loaded.")
                    return df
            except Exception as e:
                self.logger.error(f"Failed to load data from {file_info['url']} - {e}")
        else:
            self.logger.warning(f"Loader not found for format {file_info['format']}")
        
        file_info_df = pd.DataFrame(file_info).transpose()
        self.datafiles_out = pd.concat([self.datafiles_out, file_info_df], ignore_index=True)
        return None

    def load_datafiles(self, readable_files, datafile_loader_config):
        len_out = len(self.datafiles_out)
        data = []

        for i, file_info in readable_files.iterrows():
            df = self.load_file_data(file_info, datafile_loader_config)
            if df is not None:
                data.append(df)

        self.logger.info("Number of dataframes loaded: %s", len(data))
        self.logger.info("Number of elements in data that are not dataframes: %s", sum([not isinstance(df, pd.DataFrame) for df in data]))
        self.logger.info("Number of files not loaded: %s", len(self.datafiles_out) - len_out)
        
        return data
    
    def normalize_data(self, topic, topic_config, datafile_loader_config):
        len_out = len(self.datafiles_out)
        file_info_columns = datafile_loader_config["file_info_columns"]
        normalized_data = pd.DataFrame(columns=self.schema["name"])
        for col in file_info_columns:
            normalized_data[col] = ""
        schema_lower = [col.lower() for col in self.schema["name"].values]

        # Create a mapping dictionary between lower case schema names and original schema names
        schema_mapping = dict(zip(schema_lower, self.schema["name"].values))

        schema_dict_file = Path(get_project_base_path())  / "data" / "datasets" / topic / "inputs" / topic_config["schema_dict_file"]
        schema_dict = pd.read_csv(schema_dict_file, sep=";").set_index('original_name')['official_name'].to_dict()
        
        datacolumns_out = pd.DataFrame(columns=["filename", "column_name", "column_type", "nb_non_null_values"])

        for df in self.corpus:
            # Merge les colonnes avec le même nom
            df = merge_duplicate_columns(df)
            safe_rename(df, schema_dict)

            df.columns = df.columns.astype(str)
            columns_lower = [col.lower() for col in df.columns]
            # Check if the dataframe has at least 1 column in common with the schema
            if len(set(columns_lower).intersection(set(schema_lower))) > 0:
                common_columns = []
                for col, col_lower in zip(df.columns, columns_lower):
                    if col_lower in schema_lower or col in file_info_columns:
                        common_columns.append(col)
                    else:
                        out_col_df = pd.DataFrame({"filename":df["url"].iloc[0], "column_name":col, "column_type":df[col].dtype, "nb_non_null_values":df[col].count()}, index=[0])
                        datacolumns_out = pd.concat([datacolumns_out, out_col_df], ignore_index=True)
                
                df_filtered = df[common_columns]

                # Rename the columns in df_filtered using the schema_mapping
                df_filtered.columns = [schema_mapping[col.lower()] if col.lower() in schema_mapping else col for col in df_filtered.columns]
                # Append df_filtered to normalized_data
                normalized_data = pd.concat([normalized_data, df_filtered], ignore_index=True)

                self.logger.info("Normalized dataframe %s", df["url"].iloc[0])
                self.logger.info("Number of datapoints in normalized_data: %s", len(normalized_data))
                self.logger.info("Number of columns in schema: %s", len(common_columns) - len(file_info_columns))
                self.logger.info("Number of columns not in schema: %s", len(df.columns)-len(common_columns) + len(file_info_columns))
            else:
                out_df = pd.DataFrame(df.iloc[0]).transpose()
                self.datafiles_out = pd.concat([self.datafiles_out, out_df], ignore_index=True)
                self.logger.warning("No column in common with schema for file %s", df["url"].iloc[0])
        
        # Cast data to schema types
        schema_selected = self.schema.loc[:, ['name', 'type']]        
        normalized_data = cast_data(normalized_data, schema_selected, 'name')

        self.logger.info("Data types per column after casting in normalized data: %s", normalized_data.dtypes)
        self.logger.info("Percentage of NaN values after casting, per column: %s", (normalized_data.isna().sum() / len(normalized_data)) * 100)

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