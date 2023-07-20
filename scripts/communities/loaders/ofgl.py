from pathlib import Path
import pandas as pd

from files_operation import load_from_path, load_from_url, save_csv
from config import get_project_base_path

class OfglLoader():
    def __init__(self,config):
        data_folder = Path(get_project_base_path()) / config["processed_data"]["path"]
        data_file = data_folder / config["processed_data"]["filename"]
        if data_file.exists():
            self.data = pd.read_csv(data_file)
        else:
            base_path = get_project_base_path()
            epci_communes_path = base_path / config["epci"]["file"]
            epci_communes_mapping = load_from_path(epci_communes_path, dtype=config["epci"]["dtype"])
            infos_coll = pd.DataFrame()
            for key, url in config["url"].items():
                # Téléchargement des données
                df = load_from_url(url, dtype=config["dtype"])
                # Traitement spécifique pour chaque base en utilisant la fonction process_data
                if key == 'communes':
                    df = self.process_data(df, key, epci_communes_mapping)
                else:
                    df = self.process_data(df, key)

                # Concaténation des DataFrames traités dans infos_coll
                infos_coll = pd.concat([infos_coll, df], axis=0, ignore_index=True)

            # Remplir les valeurs manquantes par une chaîne vide
            infos_coll.fillna('', inplace=True)
            self.data = infos_coll
            self.save(Path(config["processed_data"]["path"]),config["processed_data"]["filename"])
    
    def get(self):
        return self.data
    
    def save(self,path,filename):
        save_csv(self.data,path,filename)

    def process_data(self, df, key, epci_communes_mapping=None):
        if key == 'regions':
            df = df[['Code Insee 2022 Région', 'Nom 2022 Région', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
            df.columns = ['COG', 'nom', 'type', 'SIREN', 'population']
            df = df.astype({'SIREN': str, 'COG': str})
            df = df.sort_values('COG')
            
        elif key == 'departements':
            df = df[['Code Insee 2022 Région', 'Code Insee 2022 Département', 'Nom 2022 Département', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
            df.columns = ['code_region', 'COG', 'nom', 'type', 'SIREN', 'population']
            df['type'] = 'DEP'
            df = df.astype({'SIREN': str, 'COG': str, 'code_region': str})
            df['COG_3digits'] = df['COG'].str.zfill(3)
            df = df[['nom', 'SIREN', 'type', 'COG', 'COG_3digits', 'code_region', 'population']]
            df = df.sort_values('COG')
            
        elif key == 'communes':
            df = df[['Code Insee 2022 Région', 'Code Insee 2022 Département', 'Code Insee 2022 Commune', 'Nom 2022 Commune', 'Catégorie', 'Code Siren Collectivité', 'Population totale']]
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
            df = df[['Code Insee 2022 Région', 'Code Insee 2022 Département', 'Nature juridique 2022 abrégée', 'Code Siren 2022 EPCI', 'Nom 2022 EPCI', 'Population totale']]
            df.columns = ['code_region', 'code_departement', 'type', 'SIREN', 'nom', 'population']
            df['type'] = df['type'].replace({'MET69': 'MET', 'MET75': 'MET', 'M': 'MET'})
            df = df.astype({'SIREN': str, 'code_departement': str})
            df['code_departement_3digits'] = df['code_departement'].str.zfill(3)
            df = df[['nom', 'SIREN', 'type', 'code_departement', 'code_departement_3digits', 'code_region', 'population']]
            df = df.sort_values('SIREN')
        
        return df
