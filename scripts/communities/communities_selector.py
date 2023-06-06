import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Ajoutez le dossier /scripts/utils au `sys.path`
utils_path = str(Path(__file__).resolve().parents[1] / 'communities'/'loaders')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

from odf import OdfLoader
from ofgl import OfglLoader
from sirene import SireneLoader

class CommunitiesSelector():
    def __init__(self,config):
        ofgl = OfglLoader(config["ofgl"])
        odf = OdfLoader(config["odf"])
        sirene = SireneLoader(config["sirene"])
        ofgl_data = ofgl.get()
        odf_data = odf.get()
        # Worth Exploring Here ! If you cast to Int, it breaks
        ofgl_data["SIREN"] = ofgl_data["SIREN"].astype(str)
        odf_data["siren"] = odf_data["siren"].astype(str) 
        all_data = ofgl_data.merge(odf_data[['siren', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']], left_on='SIREN', right_on='siren', how='left')

        # Supprimer la colonne 'siren' dupliquée et réorganiser les colonnes
        all_data.drop(columns=['siren'], inplace=True)
        
        # TODO Manage columns outside of classes (configs ?)
        all_data = all_data[['nom', 'SIREN', 'type', 'COG', 'COG_3digits', 'code_departement', 'code_departement_3digits', 'code_region', 'population', 'EPCI', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']]
        all_data["SIREN"] = all_data["SIREN"].astype(int)

        all_data = all_data.merge(sirene.get(), left_on='SIREN', right_on='siren', how='left')
        all_data.drop(columns=['siren'], inplace=True)
        
        # Conversion de la colonne 'trancheEffectifsUniteLegale' en type numérique
        all_data['trancheEffectifsUniteLegale'] = pd.to_numeric(all_data['trancheEffectifsUniteLegale'].astype(str), errors='coerce')

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
        self.selected_data = selected_data
     
    def get_datagouv_ids(self):
        new_instance = self.selected_data.copy()
        datagouv_ids = new_instance[new_instance["id-datagouv"].notnull()][["SIREN", "id-datagouv"]]        
        return datagouv_ids # return a dataframe with SIREN and id-datagouv columns

    def save_csv(self,config):
        print("hello")
        #save_csv(self["data_folder"], "processed_data/selected_communities.csv"