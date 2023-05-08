from pathlib import Path
import sys

# Ajoutez le dossier /scripts/utils au `sys.path`
utils_path = str(Path(__file__).resolve().parents[1] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_url, save_csv
from config import get_project_base_path
from filters import filter_organizations, filter_description, filter_titles

# 1er filtre manuel, à améliorer
def filter_datasets(df):
    organizations_pattern = r'^(Région|Collectivité de Corse)'
    description_pattern = 'conventions de subvention'
    title_patterns = r'(conventions de subvention|subventions.*associations|Subventions du Conseil Régional|Interventions de la Région des Pays de la Loire|SCDL - Subventions)'

    mask_org = filter_organizations(df, organizations_pattern, return_mask=True)
    mask_desc = filter_description(df, description_pattern, return_mask=True)
    mask_titles = filter_titles(df, title_patterns, return_mask=True)

    filtered_df = df[mask_org & (mask_titles | mask_desc)]

    return filtered_df[['id', 'title', 'url', 'description', 'organization', 'organization_id', 'frequency']]

# Catalogue de l'ensemble des datasets de datagouv
dataset_catalog_url = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
dataset_catalog_df = load_from_url(dataset_catalog_url)

#Applications des filtres
filtered_datasets_df = filter_datasets(dataset_catalog_df)

#Sauvegarde de la sélection
base_path = get_project_base_path()
datasets_file_folder = Path(base_path / "data/datasets/")
save_csv(filtered_datasets_df, datasets_file_folder, "filtered_datasets.csv")