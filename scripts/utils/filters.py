import pandas as pd

def filter_organizations_by_id(df, organization_ids, return_mask=False):
    mask = df['organization_id'].isin(organization_ids)
    return mask if return_mask else df[mask]

def filter_description(df, pattern, return_mask=False):
    mask = df['description'].str.contains(pattern, case=False, na=False)
    return mask if return_mask else df[mask]

def filter_titles(df, pattern, return_mask=False):
    mask = df['title'].str.contains(pattern, case=False, regex=True, na=False)
    return mask if return_mask else df[mask]
