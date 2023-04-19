from datetime import datetime
import pandas as pd
import requests
import xlrd
from io import StringIO
from pathlib import Path

# Téléchargement des fichiers CSV
def download_csv(url):
    response = requests.get(url)
    content = response.content.decode('utf-8')
    return pd.read_csv(StringIO(content), sep=";", dtype={"Code Insee 2021 Région": str, "Code Insee 2021 Département": str, "Code Insee 2021 Commune": str})

def download_odf_data(url):
    response = requests.get(url)
    content = response.content.decode('utf-8')
    return pd.read_csv(StringIO(content), dtype={'siren': str})

def save_csv(df, file_name):
    data_folder = Path("../../data/communities/processed_data/")
    df.to_csv(data_folder / file_name, index=False)

# Téléchargement des fichiers Excel 
def read_epci_mapping(file_path):
    return pd.read_excel(file_path, dtype={"siren": str, "siren_membre": str})

# URL des données
url_regions = "https://data.ofgl.fr/explore/dataset/ofgl-base-regions-consolidee/download/?format=csv&disjunctive.reg_name=true&disjunctive.agregat=true&refine.agregat=D%C3%A9penses+totales&refine.exer=2020&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
url_departements = "https://data.ofgl.fr/explore/dataset/ofgl-base-departements-consolidee/download/?format=csv&disjunctive.reg_name=true&disjunctive.dep_tranche_population=true&disjunctive.dep_name=true&disjunctive.agregat=true&refine.exer=2020&refine.agregat=D%C3%A9penses+totales&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
url_communes = "https://data.ofgl.fr/explore/dataset/ofgl-base-communes-consolidee/download/?format=csv&disjunctive.reg_name=true&disjunctive.dep_name=true&disjunctive.epci_name=true&disjunctive.tranche_population=true&disjunctive.tranche_revenu_imposable_par_habitant=true&disjunctive.com_name=true&disjunctive.agregat=true&refine.exer=2020&refine.agregat=D%C3%A9penses+totales&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
url_interco = "https://data.ofgl.fr/explore/dataset/ofgl-base-gfp-consolidee/download/?format=csv&disjunctive.dep_name=true&disjunctive.gfp_tranche_population=true&disjunctive.nat_juridique=true&disjunctive.mode_financement=true&disjunctive.gfp_tranche_revenu_imposable_par_habitant=true&disjunctive.epci_name=true&disjunctive.agregat=true&refine.exer=2020&refine.agregat=D%C3%A9penses+totales&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"

# Chemin des données téléchargées 
# EPCI<>Communes : fichier excel téléchargé depuis https://www.collectivites-locales.gouv.fr/institutions/liste-et-composition-des-epci-fiscalite-propre
epci_communes_path = "../../data/communities/scrapped_data/gouv_colloc/epcicom2023.xlsx"

# Téléchargement des données
OFGL_regions = download_csv(url_regions)
OFGL_departements = download_csv(url_departements)
OFGL_communes = download_csv(url_communes)
OFGL_interco = download_csv(url_interco)
epci_communes_mapping = read_epci_mapping(epci_communes_path)

# Traitement des régions
OFGL_regions = OFGL_regions[['Code Insee 2021 Région', 'Nom 2021 Région', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
OFGL_regions.columns = ['COG', 'nom', 'type', 'SIREN', 'population']
OFGL_regions = OFGL_regions.astype({'SIREN': str, 'COG': str})
OFGL_regions = OFGL_regions.sort_values('COG')

# Traitement des départements
OFGL_departements = OFGL_departements[['Code Insee 2021 Région', 'Code Insee 2021 Département', 'Nom 2021 Département', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
OFGL_departements.columns = ['code_region', 'COG', 'nom', 'type', 'SIREN', 'population']
OFGL_departements['type'] = 'DEP'
OFGL_departements = OFGL_departements.astype({'SIREN': str, 'COG': str, 'code_region': str})
OFGL_departements['COG_3digits'] = OFGL_departements['COG'].str.zfill(3)
OFGL_departements = OFGL_departements[['nom', 'SIREN', 'type', 'COG', 'COG_3digits', 'code_region', 'population']]
OFGL_departements = OFGL_departements.sort_values('COG')

# Traitement des communes
OFGL_communes = OFGL_communes[['Code Insee 2021 Région', 'Code Insee 2021 Département', 'Code Insee 2021 Commune', 'Nom 2021 Commune', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
OFGL_communes.columns = ['code_region', 'code_departement', 'COG', 'nom', 'type', 'SIREN', 'population']
OFGL_communes['type'] = 'COM'
OFGL_communes = OFGL_communes.astype({'SIREN': str, 'COG': str, 'code_departement': str})
OFGL_communes['code_departement_3digits'] = OFGL_communes['code_departement'].str.zfill(3)
OFGL_communes = OFGL_communes[['nom', 'SIREN', 'COG', 'type', 'code_departement', 'code_departement_3digits', 'code_region', 'population']]
OFGL_communes = OFGL_communes.sort_values('COG')

# Fusion du mapping communes <> EPCI
OFGL_communes = OFGL_communes.merge(epci_communes_mapping[['siren', 'siren_membre']], left_on='SIREN', right_on='siren_membre', how='left')
OFGL_communes = OFGL_communes.drop(columns=['siren_membre'])
OFGL_communes.rename(columns={'siren': 'EPCI'}, inplace=True)

# Traitement des intercommunalités
OFGL_interco = OFGL_interco[['Code Insee 2021 Région', 'Code Insee 2021 Département', 'Nature juridique 2021 abrégée', 'Code Siren 2021 EPCI', 'Nom 2021 EPCI', 'Population totale']]
OFGL_interco.columns = ['code_region', 'code_departement', 'type', 'SIREN', 'nom', 'population']
OFGL_interco['type'] = OFGL_interco['type'].replace({'MET69': 'MET', 'MET75': 'MET', 'M': 'MET'})
OFGL_interco = OFGL_interco.astype({'SIREN': str})
OFGL_interco['code_departement_3digits'] = OFGL_interco['code_departement'].str.zfill(3)
OFGL_interco = OFGL_interco[['nom', 'SIREN', 'type', 'code_departement', 'code_departement_3digits', 'code_region', 'population']]
OFGL_interco = OFGL_interco.sort_values('population')

# Export des fichiers CSV
save_csv(OFGL_regions, f"identifiants_regions_{datetime.now().strftime('%Y')}.csv")
save_csv(OFGL_departements, f"identifiants_departements_{datetime.now().strftime('%Y')}.csv")
save_csv(OFGL_communes, f"identifiants_communes_{datetime.now().strftime('%Y')}.csv")
save_csv(OFGL_interco, f"identifiants_epci_{datetime.now().strftime('%Y')}.csv")

# Fusion des DataFrames
infos_coll = pd.concat([OFGL_regions, OFGL_departements, OFGL_communes, OFGL_interco], axis=0, ignore_index=True)

# Remplir les valeurs manquantes par une chaîne vide
infos_coll.fillna('', inplace=True)

# Téléchargez les données OpenDataFrance
url_odf = "https://static.data.gouv.fr/resources/donnees-de-lobservatoire-open-data-des-territoires-edition-2022/20230202-112356/indicateurs-odater-organisations-2022-12-31-.csv"
odf_data = download_odf_data(url_odf)

# Effectuez une jointure entre "infos_coll" et "odf_data" sur la colonne "SIREN"
infos_coll = infos_coll.merge(odf_data[['siren', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']], left_on='SIREN', right_on='siren', how='left')

# Supprimez la colonne 'siren' dupliquée et réorganisez les colonnes
infos_coll.drop(columns=['siren'], inplace=True)
infos_coll = infos_coll[['nom', 'SIREN', 'type', 'COG', 'COG_3digits', 'code_departement', 'code_departement_3digits', 'code_region', 'population', 'EPCI', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']]


# Enregistrement du fichier CSV
save_csv(infos_coll, "infos_collectivites.csv")