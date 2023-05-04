from pathlib import Path
import pandas as pd
import sys

# Ajoutez le dossier /scripts/utils au `sys.path`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'utils'))
from utils import load_from_url, save_csv, get_project_base_path

# 1er filtre manuel, à améliorer
def process_datafiles(datafiles_df, desired_columns):
    main_df = pd.DataFrame(columns=desired_columns + ['organization_id', 'organization'])

    num_files = len(datafiles_df)
    for index, (i, row) in enumerate(datafiles_df.iterrows()):
        csv_link = row['url_y']
        organization_id = row['organization_id']
        organization = row['organization']

        csv_df = load_from_url(csv_link)

        if csv_df is not None and isinstance(csv_df, pd.DataFrame):
            # Ajouter les colonnes 'organization_id' et 'organization' à csv_df
            csv_df['organization_id'] = organization_id
            csv_df['organization'] = organization

            csv_df = csv_df[[col for col in csv_df.columns if col.lower() in [x.lower() for x in desired_columns] + ['organization_id', 'organization']]]
            main_df = pd.concat([main_df, csv_df], ignore_index=True)

        print(f"Progress: {index + 1}/{num_files}")  # Ajout d'un message pour suivre l'avancée

    return main_df.drop_duplicates()


# Catalogue de l'ensemble des datafiles de datagouv
datafile_catalog_url = 'https://www.data.gouv.fr/fr/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d'
datafile_catalog_df = load_from_url(datafile_catalog_url)

# Import des datasets filtrés
base_path = get_project_base_path()
datasets_file_folder = Path(base_path / "data/datasets/")
filtered_datasets_filename = "filtered_datasets.csv"
filtered_datasets_df = pd.read_csv(datasets_file_folder / filtered_datasets_filename)

# Jointure entre table des datasets filtrees sur les bons themes et la table des fichiers
#print("filtered_datasets_df :")
#print(filtered_datasets_df.head())
#print("datafile_catalog_df :")
#print(datafile_catalog_df.head())
filtered_datafiles_df = filtered_datasets_df.merge(datafile_catalog_df, left_on='id', right_on='dataset.id')

# Filtre des lignes qui ne sont pas des csv : à étudier
filtered_datafiles_df = filtered_datafiles_df[filtered_datafiles_df['format']=='csv']

# Sauvegarde des fichiers filtrés
save_csv(filtered_datafiles_df, datasets_file_folder, "filtered_datafiles.csv")

# Choix des colonnes à filtrer (je me suis apercu que tout est etait lowercase dans beaucoup de regions)
desired_columns = [
    'nomattribuant', 'idattribuant', 'dateconvention', 'referencedecision',
    'nombeneficiaire', 'idbeneficiaire', 'rnabeneficiaire', 'objet', 'montant',
    'nature', 'conditionsversement', 'datesperiodeversement', 'idrae',
    'notificationue', 'pourcentagesubvention', 'dispositifaide'
]

# Suppression des doublons
filtered_datafiles_df = filtered_datafiles_df.drop_duplicates(subset=['url_y'])
filtered_detailed_data_df = process_datafiles(filtered_datafiles_df, desired_columns)
#print(filtered_detailed_data_df.shape)
#print(filtered_detailed_data_df.head())

# Sauvegarde des fichiers filtrés
save_csv(filtered_detailed_data_df, datasets_file_folder, "filtered_detailed_data.csv")