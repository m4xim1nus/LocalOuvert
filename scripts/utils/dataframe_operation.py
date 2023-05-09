def convert_columns_to_lowercase(df):
    df.columns = [col.lower() for col in df.columns]
    return df

def create_columns(df, desired_columns):
    for col in desired_columns:
        if col not in df.columns:
            df[col] = ''
    return df