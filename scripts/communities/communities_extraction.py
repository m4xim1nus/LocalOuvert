from datetime import datetime
import pandas as pd
import sys
from pathlib import Path

# Ajoutez le dossier /scripts/utils au `sys.path`
utils_path = str(Path(__file__).resolve().parents[1] / 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from files_operation import load_from_path, load_from_url, save_csv, download_and_process_data
from config import get_project_base_path

# Processus de traitement spécifiques des données
def process_data(df, key, epci_communes_mapping=None):
    if key == 'regions':
        df = df[['Code Insee 2021 Région', 'Nom 2021 Région', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
        df.columns = ['COG', 'nom', 'type', 'SIREN', 'population']
        df = df.astype({'SIREN': str, 'COG': str})
        df = df.sort_values('COG')
        
    elif key == 'departements':
        df = df[['Code Insee 2021 Région', 'Code Insee 2021 Département', 'Nom 2021 Département', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
        df.columns = ['code_region', 'COG', 'nom', 'type', 'SIREN', 'population']
        df['type'] = 'DEP'
        df = df.astype({'SIREN': str, 'COG': str, 'code_region': str})
        df['COG_3digits'] = df['COG'].str.zfill(3)
        df = df[['nom', 'SIREN', 'type', 'COG', 'COG_3digits', 'code_region', 'population']]
        df = df.sort_values('COG')
        
    elif key == 'communes':
        df = df[['Code Insee 2021 Région', 'Code Insee 2021 Département', 'Code Insee 2021 Commune', 'Nom 2021 Commune', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
        df.columns = ['code_region', 'code_departement', 'COG', 'nom', 'type', 'SIREN', 'population']
        df['type'] = 'COM'
        df = df.astype({'SIREN': str, 'COG': str, 'code_departement': str})
        df['code_departement_3digits'] = df['code_departement'].str.zfill(3)
        df = df[['nom', 'SIREN', 'COG', 'type', 'code_departement', 'code_departement_3digits', 'code_region', 'population']]
        df = df.sort_values('COG')
        df = df.merge(epci_communes_mapping[['siren', 'siren_membre']], left_on='SIREN', right_on='siren_membre', how='left')
        df = df.drop(columns=['siren_membre'])
        df.rename(columns={'siren': 'EPCI'}, inplace=True)
        
    elif key == 'interco':
        df = df[['Code Insee 2021 Région', 'Code Insee 2021 Département', 'Nature juridique 2021 abrégée', 'Code Siren 2021 EPCI', 'Nom 2021 EPCI', 'Population totale']]
        df.columns = ['code_region', 'code_departement', 'type', 'SIREN', 'nom', 'population']
        df['type'] = df['type'].replace({'MET69': 'MET', 'MET75': 'MET', 'M': 'MET'})
        df = df.astype({'SIREN': str, 'code_departement': str})
        df['code_departement_3digits'] = df['code_departement'].str.zfill(3)
        df = df[['nom', 'SIREN', 'type', 'code_departement', 'code_departement_3digits', 'code_region', 'population']]
        df = df.sort_values('SIREN')

# URL des données
url_regions = "https://data.ofgl.fr/explore/dataset/ofgl-base-regions-consolidee/download/?format=csv&disjunctive.reg_name=true&disjunctive.agregat=true&refine.agregat=D%C3%A9penses+totales&refine.exer=2020&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
url_departements = "https://data.ofgl.fr/explore/dataset/ofgl-base-departements-consolidee/download/?format=csv&disjunctive.reg_name=true&disjunctive.dep_tranche_population=true&disjunctive.dep_name=true&disjunctive.agregat=true&refine.exer=2020&refine.agregat=D%C3%A9penses+totales&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
url_communes = "https://data.ofgl.fr/explore/dataset/ofgl-base-communes-consolidee/download/?format=csv&disjunctive.reg_name=true&disjunctive.dep_name=true&disjunctive.epci_name=true&disjunctive.tranche_population=true&disjunctive.tranche_revenu_imposable_par_habitant=true&disjunctive.com_name=true&disjunctive.agregat=true&refine.exer=2020&refine.agregat=D%C3%A9penses+totales&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
url_interco = "https://data.ofgl.fr/explore/dataset/ofgl-base-gfp-consolidee/download/?format=csv&disjunctive.dep_name=true&disjunctive.gfp_tranche_population=true&disjunctive.nat_juridique=true&disjunctive.mode_financement=true&disjunctive.gfp_tranche_revenu_imposable_par_habitant=true&disjunctive.epci_name=true&disjunctive.agregat=true&refine.exer=2020&refine.agregat=D%C3%A9penses+totales&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"

# Chemin des données téléchargées 
# EPCI<>Communes : fichier excel téléchargé depuis https://www.collectivites-locales.gouv.fr/institutions/liste-et-composition-des-epci-fiscalite-propre
base_path = get_project_base_path()
epci_communes_path = base_path / "data/communities/scrapped_data/gouv_colloc" / "epcicom2023.xlsx"
epci_communes_mapping = load_from_path(epci_communes_path, dtype={"siren": str, "siren_membre": str})

# Téléchargement des données
OFGL_dtype = {"Code Insee 2021 Région": str, "Code Insee 2021 Département": str, "Code Insee 2021 Commune": str}
url_data = {'regions': url_regions, 'departements': url_departements, 'communes': url_communes, 'interco': url_interco}
data_frames = {}
infos_coll = pd.DataFrame()

processed_data_folder = Path(base_path / "data/communities//processed_data/")

for key, url in url_data.items():
    # Téléchargement des données
    df = load_from_url(url, dtype=OFGL_dtype)

    # Traitement spécifique pour chaque base en utilisant la fonction process_data
    if key == 'communes':
        df = process_data(df, key, epci_communes_mapping)
    else:
        df = process_data(df, key)

    # Sauvegarde du fichier CSV
    save_csv(df, processed_data_folder, f"identifiants_{key}_{datetime.now().strftime('%Y')}.csv")

    # Ajout du DataFrame traité dans le dictionnaire "data_frames"
    data_frames[key] = df

    # Concaténation des DataFrames traités dans infos_coll
    infos_coll = pd.concat([infos_coll, df], axis=0, ignore_index=True)

# Remplir les valeurs manquantes par une chaîne vide
infos_coll.fillna('', inplace=True)

# Téléchargez les données OpenDataFrance
odf_dtype = {'siren': str}
url_odf = "https://static.data.gouv.fr/resources/donnees-de-lobservatoire-open-data-des-territoires-edition-2022/20230202-112356/indicateurs-odater-organisations-2022-12-31-.csv"
odf_data = load_from_url(url_odf, dtype=odf_dtype)

# Effectuez une jointure entre "infos_coll" et "odf_data" sur la colonne "SIREN"
infos_coll = infos_coll.merge(odf_data[['siren', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']], left_on='SIREN', right_on='siren', how='left')

# Supprimez la colonne 'siren' dupliquée et réorganisez les colonnes
infos_coll.drop(columns=['siren'], inplace=True)
infos_coll = infos_coll[['nom', 'SIREN', 'type', 'COG', 'COG_3digits', 'code_departement', 'code_departement_3digits', 'code_region', 'population', 'EPCI', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']]

# Enregistrement du fichier CSV
save_csv(infos_coll, processed_data_folder, "infos_collectivites.csv")