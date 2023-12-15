import logging
import requests
from pathlib import Path
import pandas as pd
from io import StringIO
import os
import json 

from config import get_project_base_path
from scripts.loaders.base_loader import BaseLoader


class GeoLocator:
    def __init__(self, geo_config):
        self.logger = logging.getLogger(__name__)
        # Charger les données une seule fois lors de l'initialisation de l'instance
        data_folder = Path(get_project_base_path()) / "data" / "communities" / "scrapped_data" / "geoloc"
        reg_dep_geoloc_filename = "dep_reg_centers.csv"

        reg_dep_geoloc_df = pd.read_csv(data_folder / reg_dep_geoloc_filename, sep=';')
        if reg_dep_geoloc_df.empty:
            self.reg_dep_geoloc_df = None
        else:
            reg_dep_geoloc_df['cog'] = reg_dep_geoloc_df['cog'].astype(str)
            self.reg_dep_geoloc_df = reg_dep_geoloc_df

        epci_coord_loader = BaseLoader.loader_factory(geo_config["epci_coord_url"])
        self.epci_coord_df = epci_coord_loader.load()

        communes_coord_loader = BaseLoader.loader_factory(geo_config["communes_id_url"])
        self.communes_df = communes_coord_loader.load()

    
    def get_commune_coordinates(self, city_name, city_code):
        # Implémenter la logique pour récupérer les coordonnées via l'API de https://adresse.data.gouv.fr/api-doc/adresse, via le CODE INSEE (COG)
        formatted_city_name = city_name.replace(" ", "+")
        url = f"https://api-adresse.data.gouv.fr/search/?q={formatted_city_name}code={city_code}&type=municipality"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data['features']:
                coordinates = data['features'][0]['geometry']['coordinates']
                self.logger.info(f"Les coordonnées de {city_name} sont {coordinates[0]}, {coordinates[1]}")
                return coordinates[0], coordinates[1]  # longitude, latitude
        self.logger.warning(f"Les coordonnées de {city_name} ne sont pas trouvées")
        return None, None

    def get_region_department_coordinates(self, cog, type):
        # Implémenter la logique - à partir d'un csv scrappé à la main sur l'IGN - stable
        if self.reg_dep_geoloc_df is not None:
            reg_dep_geoloc = self.reg_dep_geoloc_df.copy()
            # Convert cog to string
            cog = str(cog)
            
            # get latitude and longitude based on cog & type
            reg_dep_geoloc = reg_dep_geoloc.loc[(reg_dep_geoloc['cog'] == cog) & (reg_dep_geoloc['type'] == type)]
            if not reg_dep_geoloc.empty:
                self.logger.info(f"Les coordonnées de {reg_dep_geoloc['nom'].values[0]} sont {reg_dep_geoloc['latitude'].values[0]}, {reg_dep_geoloc['longitude'].values[0]}")
                return reg_dep_geoloc['longitude'].values[0], reg_dep_geoloc['latitude'].values[0]
            else:
                self.logger.warning(f"Les coordonnées de {cog} de type {type} ne sont pas trouvées")
                return None, None
        else:
            self.logger.warning("Le fichier CSV des coordonnées des régions et départements n'est pas trouvé")
            return None, None

    def get_epci_coordinates(self, siren):
        # Implémenter la logique - à partir d'un triple matching :
        # 1. siren epci -> siren de la commune du siège via https://www.data.gouv.fr/fr/datasets/base-nationale-sur-les-intercommunalites/ (coordonnees-epci-fp-janv2023-last.xlsx)
        commune_siege_siren = self.epci_coord_df[self.epci_coord_df['N° SIREN'] == siren]['Commune siège'].str.extract('(\d+)').iloc[0, 0]

        # 2. siren commune-> nom, COG commune via https://www.data.gouv.fr/en/datasets/identifiants-des-collectivites-territoriales-et-leurs-etablissements/ (identifiants-communes-2022.csv)
        # Cast commune_siege_siren and communes_df 'SIREN' to string
        communes_df = self.communes_df.copy()
        
        commune_siege_siren = str(commune_siege_siren)
        communes_df['SIREN'] = communes_df['SIREN'].astype(str)

        # extract nom, COG commune safely
        commune_info_df = communes_df[communes_df['SIREN'] == commune_siege_siren][['nom', 'COG']]
        if commune_info_df.empty:
            self.logger.warning(f"Les coordonnées pour l'EPCI {siren} de la commune siège {commune_siege_siren} ne sont pas trouvées")
            return None, None
        commune_info = communes_df[communes_df['SIREN'] == commune_siege_siren][['nom', 'COG']].iloc[0]

        # 3. nom, COG -> coordonnées via les coordonnées des communes - utiliser la fonction get_commune_coordinates
        if not commune_info.empty:
            coordinates = self.get_commune_coordinates(commune_info['nom'], commune_info['COG'])
            if coordinates:
                self.logger.info(f"Les coordonnées de l'EPCI {siren} sont celle de la commune siège {commune_siege_siren} : {coordinates[0]}, {coordinates[1]}")
                return coordinates[0], coordinates[1]
            else:
                self.logger.warning(f"Les coordonnées de l'EPCI {siren} de la commune siège {commune_siege_siren} ne sont pas trouvées")
                return None, None
    
    def add_geocoordinates(self, data_frame):

        # Boucle sur le DataFrame et enrichissement avec les coordonnées
        # Logique à revoir : toutes regions et départements, puis toutes les communes, puis les EPCI (car copies des coordonnées des communes) 
        for index, row in data_frame.iterrows():
            if row['type'] in ['COM']:
                coordinates = self.get_commune_coordinates(row['nom'], row['COG'])
            elif row['type'] in ['REG', 'DEP', 'CTU']:
                coordinates = self.get_region_department_coordinates(row['COG'], row['type'])
            else:
                if row['siren']:
                    coordinates = self.get_epci_coordinates(row['siren'])
                else:
                    coordinates = None, None
                    self.logger.warning(f"Le SIREN de l'EPCI {row['nom']} n'est pas trouvé")
            
            if coordinates:
                data_frame.at[index, 'longitude'] = coordinates[0]
                data_frame.at[index, 'latitude'] = coordinates[1]
            else:
                # Gérer le cas où aucune coordonnée n'est trouvée
                data_frame.at[index, 'longitude'] = None
                data_frame.at[index, 'latitude'] = None

        return data_frame