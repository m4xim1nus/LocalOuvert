from pathlib import Path
import sys

# Ajoutez le dossier /scripts/utils au `sys.path`
utils_path = str(Path(__file__).resolve().parents[1] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_url, save_csv
from config import get_project_base_path
from filters import filter_organizations_by_id, filter_description, filter_titles


# 1er filtre manuel, à améliorer
def filter_datasets(df):
    organization_ids = [
        #Régions à la mano
        '534fffa8a3a7292c64a780c8',
        '561a7f00c751df4e6dcdbb48',
        '5835cdb988ee383e0bc65bb3',
        '565ebf4788ee3869aae72046',
        '5df935d8634f414e69469492',
        '55df259f88ee386e57a46ec2',
        '534fffa7a3a7292c64a780c5',
        '559a4970c751df46a9a453ba',
        '534fff6aa3a7292c64a77d95',
        '5a153f5688ee3829e2fbf39f',
        '60784425bed18cfaefe5e653',
        '534fffa8a3a7292c64a780d2',
        '537d58eea3a72973a2dc026c',
    ]
    #organizations_pattern = r'^(?:.*Région|.*Collectivité de Corse)'
    description_pattern = 'conventions de subvention'
    title_patterns = r'(?:conventions de subvention|subventions.*associations|Subventions du Conseil Régional|Interventions de la Région des Pays de la Loire|SCDL - Subventions)'

    mask_org = filter_organizations_by_id(df, organization_ids, return_mask=True)
    print(f"Nombre de datasets correspondant au filtre d'organisation : {mask_org.sum()}")

    mask_desc = filter_description(df, description_pattern, return_mask=True)
    print(f"Nombre de datasets correspondant au filtre de description : {mask_desc.sum()}")

    mask_titles = filter_titles(df, title_patterns, return_mask=True)
    print(f"Nombre de datasets correspondant au filtre de titre : {mask_titles.sum()}")

    filtered_df = df[mask_org & (mask_titles | mask_desc)]

    return filtered_df

# Catalogue de l'ensemble des datasets de datagouv
columns_to_keep = ['id', 'title', 'url', 'description', 'organization', 'organization_id', 'frequency']
dataset_catalog_url = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
dataset_catalog_df = load_from_url(dataset_catalog_url, columns_to_keep=columns_to_keep)
print(dataset_catalog_df.head())
print(dataset_catalog_df['organization'].head(10))

#Applications des filtres

filtered_datasets_df = filter_datasets(dataset_catalog_df)
print(f"Nombre de datasets filtrés : {len(filtered_datasets_df)}")

#Sauvegarde de la sélection
base_path = get_project_base_path()
datasets_file_folder = Path(base_path / "data/datasets/")
save_csv(filtered_datasets_df, datasets_file_folder, "filtered_datasets.csv")