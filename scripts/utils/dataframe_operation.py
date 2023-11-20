def convert_columns_to_lowercase(df):
    df.columns = [col.lower() for col in df.columns]
    return df

def create_columns(df, desired_columns):
    for col in desired_columns:
        if col not in df.columns:
            df[col] = ''
    return df
    
def merge_duplicate_columns(df):
    df.columns = df.columns.astype(str)
    duplicate_columns = df.columns[df.columns.duplicated(keep=False)]
    for col in duplicate_columns.unique():
        cols_to_merge = df.filter(like=col, axis=1)
        df[col] = cols_to_merge.apply(lambda x: ' / '.join(x.dropna().astype(str)), axis=1)
        df.drop(cols_to_merge.columns[1:], axis=1, inplace=True)
    return df

def safe_rename(df, schema_dict):
    schema_dict_copy = schema_dict.copy()    
    for original_name, official_name in schema_dict.items():
        if official_name in df.columns and original_name != official_name:
            del schema_dict_copy[original_name]
    df.rename(columns=schema_dict_copy, inplace=True)