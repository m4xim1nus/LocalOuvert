from pathlib import Path
import sys

# Ajoutez le dossier /scripts/utils au `sys.path`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'utils'))
from utils import load_from_url, save_csv, get_project_base_path

# 1er filtre manuel, à améliorer
def filter_datasets(df):
    # Filtrer des organisations régionales manuel : à connecter à la db communities
    mask_org = df['organization'].str.startswith('Région') | df['organization'].str.startswith('Collectivité de Corse')
    
    # Filtre des lignes manuel pour les subventions aux associations
    mask_desc = df['description'].str.contains('conventions de subvention', case=False)
    mask_title_1 = df['title'].str.contains('conventions de subvention', case=False)
    mask_title_2 = df['title'].str.contains('subventions', case=False) & df['title'].str.contains('associations', case=False)
    mask_title_3 = df['title'].str.contains('Subventions du Conseil Régional', case=False)
    mask_title_4 = df['title'].str.contains('Interventions de la Région des Pays de la Loire', case=False)
    mask_title_5 = df['title'].str.contains('SCDL - Subventions', case=False)

    filtered_df = df[mask_org & (mask_title_1 | mask_title_2 | mask_title_3 | mask_title_4 | mask_title_5 | mask_desc)][['id', 'title', 'url', 'description', 'organization', 'organization_id', 'frequency']]
    return filtered_df


# Catalogue de l'ensemble des datasets de datagouv
dataset_catalog_url = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
dataset_catalog_df = load_from_url(dataset_catalog_url)

#Applications des filtres
filtered_datasets_df = filter_datasets(dataset_catalog_df)

#Sauvegarde de la sélection
base_path = get_project_base_path()
datasets_file_folder = Path(base_path / "data/datasets/")
save_csv(filtered_datasets_df, datasets_file_folder, "filtered_datasets.csv")