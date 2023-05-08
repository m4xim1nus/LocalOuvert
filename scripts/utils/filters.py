import pandas as pd

def filter_organizations(df, pattern, return_mask=False):
    mask = df['organization'].str.contains(pattern, regex=True)
    return mask if return_mask else df[mask]

def filter_description(df, pattern, return_mask=False):
    mask = df['description'].str.contains(pattern, case=False)
    return mask if return_mask else df[mask]

def filter_titles(df, pattern, return_mask=False):
    mask = df['title'].str.contains(pattern, case=False, regex=True)
    return mask if return_mask else df[mask]
