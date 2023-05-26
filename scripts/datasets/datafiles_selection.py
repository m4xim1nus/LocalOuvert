from pathlib import Path
import pandas as pd
import sys

# Ajoutez le dossier /scripts/utils au `sys.path`
utils_path = str(Path(__file__).resolve().parents[1] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_url, save_csv
from config import get_project_base_path
from dataframe_operation import convert_columns_to_lowercase, create_columns

# 1er filtre manuel, à améliorer
def process_datafiles(datafiles_df, desired_columns, url_column):
    main_columns = set(desired_columns + ['organization_id', 'organization'])
    main_df = pd.DataFrame(columns=main_columns)

    num_files = len(datafiles_df)
    for index, (i, row) in enumerate(datafiles_df.iterrows()):
        csv_link = row[url_column]
        organization_id = row['organization_id']
        organization = row['organization']

        csv_df = load_from_url(csv_link, columns_to_keep=desired_columns)

        if csv_df is not None and isinstance(csv_df, pd.DataFrame):
            csv_df = convert_columns_to_lowercase(csv_df)
            csv_df['organization_id'] = organization_id
            csv_df['organization'] = organization
            csv_df = create_columns(csv_df, desired_columns)

            main_df = pd.concat([main_df, csv_df], ignore_index=True)

        print(f"Progress: {index + 1}/{num_files}")  # Ajout d'un message pour suivre l'avancée

    return main_df.drop_duplicates()

# Import des datasets filtrés
base_path = get_project_base_path()
datasets_file_folder = Path(base_path / "data/datasets/")
filtered_datasets_filename = "filtered_datasets.csv"
filtered_datasets_df = pd.read_csv(datasets_file_folder / filtered_datasets_filename)

# Catalogue de l'ensemble des datafiles de datagouv
datafile_catalog_url = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'
datafile_catalog_df = load_from_url(datafile_catalog_url)

# Jointure entre table des datasets filtrees sur les bons themes et la table des fichiers
filtered_datafiles_df = filtered_datasets_df.merge(datafile_catalog_df, left_on='id', right_on='dataset.id')

# Filtre des lignes qui ne sont pas des csv, Suppression des doublons
filtered_datafiles_df = filtered_datafiles_df[filtered_datafiles_df['format']=='csv']
filtered_datafiles_df = filtered_datafiles_df.drop_duplicates(subset=['url_y'])

# Sélection des colonnes
columns_to_drop = [
    'url_x', 'description_x',
    'dataset.id', 'dataset.title', 'dataset.slug', 'dataset.url',
    'dataset.organization', 'dataset.organization_id', 'dataset.license','dataset.private', 'dataset.archived',
    'description_y',
    'filetype', 'mime', 'filesize', 'checksum.type', 'checksum.value','downloads',
    'harvest.created_at', 'harvest.modified_at'
]
filtered_datafiles_df = filtered_datafiles_df.drop(columns=columns_to_drop)

# Renommage les colonnes
columns_rename = {
    'id_x': 'dataset_id',
    'title_x' : 'dataset_title',
    'id_y': 'datafile_id', 
    'title_y': 'datafile_title', 
    'url_y': 'datafile_url',
    'format': 'file_format'
}
filtered_datafiles_df = filtered_datafiles_df.rename(columns=columns_rename)

# Sauvegarde des fichiers filtrés
save_csv(filtered_datafiles_df, datasets_file_folder, "filtered_datafiles.csv")

# Choix des colonnes à filtrer
desired_columns = [
    'nomattribuant', 'idattribuant', 'dateconvention', 'referencedecision',
    'nombeneficiaire', 'idbeneficiaire', 'rnabeneficiaire', 'objet', 'montant',
    'nature', 'conditionsversement', 'datesperiodeversement', 'idrae',
    'notificationue', 'pourcentagesubvention', 'dispositifaide'
]

# 
filtered_detailed_data_df = process_datafiles(filtered_datafiles_df, desired_columns, 'datafile_url')

# Sauvegarde des fichiers filtrés
save_csv(filtered_detailed_data_df, datasets_file_folder, "filtered_detailed_data.csv")