import logging
import pandas as pd
import numpy as np

from odf import OdfLoader
from ofgl import OfglLoader
from sirene import SireneLoader

class CommunitiesSelector():
    def __init__(self,config):
        self.logger = logging.getLogger(__name__)
        ofgl = OfglLoader(config["ofgl"])
        odf = OdfLoader(config["odf"])
        sirene = SireneLoader(config["sirene"])
        ofgl_data = ofgl.get()
        odf_data = odf.get()
        # Worth Exploring Here ! If you cast to Int, it breaks
        ofgl_data["siren"] = ofgl_data["SIREN"].astype(str)
        ofgl_data.drop(columns=['SIREN'], inplace=True)
        odf_data["siren"] = odf_data["siren"].astype(str) 
        all_data = ofgl_data.merge(odf_data[['siren', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']], on='siren', how='left')
        
        # TODO Manage columns outside of classes (configs ?)
        all_data = all_data[['nom', 'siren', 'type', 'COG', 'COG_3digits', 'code_departement', 'code_departement_3digits', 'code_region', 'population', 'EPCI', 'url-ptf', 'url-datagouv', 'id-datagouv', 'merge', 'ptf']]
        all_data["siren"] = all_data["siren"].astype(int)

        all_data = all_data.merge(sirene.get(), on='siren', how='left')
        
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
        datagouv_ids = new_instance[new_instance["id-datagouv"].notnull()][["siren", "id-datagouv"]]        
        return datagouv_ids # return a dataframe with siren and id-datagouv columns