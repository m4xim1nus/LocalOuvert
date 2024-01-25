import logging
from pathlib import Path
import pandas as pd
import numpy as np
import re

from scripts.communities.loaders.odf import OdfLoader
from scripts.communities.loaders.ofgl import OfglLoader
from scripts.communities.loaders.sirene import SireneLoader

from scripts.utils.files_operation import save_csv
from scripts.utils.config import get_project_base_path
from scripts.utils.geolocator import GeoLocator
from scripts.utils.psql_connector import PSQLConnector

class CommunitiesSelector():
    _instance = None
    _init_done = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CommunitiesSelector, cls).__new__(cls)
        return cls._instance

    
    def __init__(self,config):
        if self._init_done:
            return
        self.logger = logging.getLogger(__name__)
        ofgl = OfglLoader(config["ofgl"])
        odf = OdfLoader(config["odf"])
        sirene = SireneLoader(config["sirene"])
        ofgl_data = ofgl.get()
        odf_data = odf.get()
        # Worth Exploring Here ! If you cast to Int, it breaks
        ofgl_data["siren"] = pd.to_numeric(ofgl_data["siren"], errors="coerce")
        ofgl_data["siren"] = ofgl_data["siren"].fillna(0).astype(int) 
        odf_data["siren"] = pd.to_numeric(odf_data["siren"], errors="coerce")
        odf_data["siren"] = odf_data["siren"].fillna(0).astype(int)
        all_data = ofgl_data.merge(odf_data[['siren', 'url_ptf', 'url_datagouv', 'id_datagouv', 'merge', 'ptf']], on='siren', how='left')
        # TODO Manage columns outside of classes (configs ?)
        all_data = all_data[['nom', 'siren', 'type', 'cog', 'cog_3digits', 'code_departement', 'code_departement_3digits', 'code_region', 'population', 'epci', 'url_ptf', 'url_datagouv', 'id_datagouv', 'merge', 'ptf']]
        all_data['siren'] = pd.to_numeric(all_data['siren'], errors='coerce')
        all_data["siren"] = all_data["siren"].fillna(0).astype(int)

        all_data = all_data.merge(sirene.get(), on='siren', how='left')
        
        # Conversion de la colonne 'trancheEffectifsUniteLegale' et 'population' en type numérique
        all_data['trancheEffectifsUniteLegale'] = pd.to_numeric(all_data['trancheEffectifsUniteLegale'].astype(str), errors='coerce')
        all_data['population'] = pd.to_numeric(all_data['population'].astype(str), errors='coerce')

        # Ajout de la variable EffectifsSup50, filtre légale d'application de l'open data par défaut (50 ETP agents)
        all_data['EffectifsSup50'] = np.where(all_data['trancheEffectifsUniteLegale'] > 15, True, False)

        self.all_data = all_data

        #Copy all data to selected data before filtering (utile ?)
        #Filter based on law
        selected_data = all_data.copy()
        selected_data = selected_data.loc[
                        (self.all_data['type'] != 'COM') |
                        ((self.all_data['type'] == 'COM') &
                        (self.all_data['population'] >= 3500) &
                        (self.all_data['EffectifsSup50'] == True))
                        ]
        
        # Ajout des coordonnées géographiques
        geolocator = GeoLocator(config["geolocator"])
        selected_data = geolocator.add_geocoordinates(selected_data)
        selected_data.columns = [re.sub(r"[.-]", "_", col.lower()) for col in selected_data.columns] # to adjust column for SQL format and ensure consistency
        self.selected_data = selected_data

        # save all_data & selected_data to csv
        data_folder = Path(get_project_base_path()) / "data" / "communities" / "processed_data"
        all_data_filename = "all_communities_data.csv"
        selected_data_filename = "selected_communities_data.csv"
        save_csv(all_data, data_folder, all_data_filename, sep=";")
        save_csv(selected_data, data_folder, selected_data_filename, sep=";")

        #Saving to DB (WARNING : does not erase at the moment)

        connector = PSQLConnector()
        connector.connect()
        connector.save_df_to_sql(selected_data,"communities")
        self._init_done = True

     
    def get_datagouv_ids(self):
        new_instance = self.selected_data.copy()
        datagouv_ids = new_instance[new_instance["id_datagouv"].notnull()][["siren", "id_datagouv"]]        
        return datagouv_ids # return a dataframe with siren and id_datagouv columns
    
    def get_selected_ids(self):
        new_instance = self.selected_data.copy()
        selected_data_ids = new_instance[new_instance["siren"].notnull()][["siren", "nom", "type"]]
        selected_data_ids.drop_duplicates(subset=['siren'], keep='first', inplace=True)   # keep only the first duplicated value TODO to be improved      
        return selected_data_ids # return a dataframe with siren and & basic info