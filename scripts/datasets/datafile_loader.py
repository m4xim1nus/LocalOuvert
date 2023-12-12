import json
import re
import requests
import pandas as pd
import logging
import unidecode

from communities_selector import CommunitiesSelector
from files_operation import load_from_url, load_json
from json_operation import flatten_json_schema, flatten_data
from dataframe_operation import cast_data

class DatafileLoader():
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)

        self.scope = CommunitiesSelector(config["communities"])
        self.schema = self.load_schema(config)
        self.loaded_data = self.load_data(config)
        self.cleaned_data = self.clean_data(config)

        self.communities_scope = CommunitiesSelector(config["communities"])
        self.communities_ids = self.communities_scope.get_selected_ids()
        self.selected_data = self.select_data(config)
        self.primary_data = self.remove_secondary_columns(config)
        self.normalized_data = self.normalize_data(config)

    def load_schema(self, config):
        json_schema = load_from_url(config["search"]["marches_publics"]["schema"]["url"])
        schema_name = config["search"]["marches_publics"]["schema"]["name"]
        flattened_schema = flatten_json_schema(json_schema, schema_name)
        schema_df = pd.DataFrame(flattened_schema)
        # In "type" column, replace NaN values by "string" (default value)
        schema_df['type'].fillna('string', inplace=True)
        return schema_df
    
    def load_data(self, config):
        data = load_json(config["search"]["marches_publics"]["unified_dataset"]["url"])
        df = flatten_data(data['marches'])
        self.logger.info(f"Le fichier au format JSON a été téléchargé avec succès à l'URL : {config['search']['marches_publics']['unified_dataset']['url']}")
        return df
    
    def clean_data(self, config):
        # Clean columns : remove columns from loaded_data whose names are not in schema
        original_to_cleaned_names = {
            col: self.clean_column_name_for_comparison(col) for col in self.loaded_data.columns
        }
        # Récupérer les noms de propriétés du schéma
        schema_columns = set(self.schema['property'])
        # Trouver les colonnes à conserver en fonction du schéma
        columns_to_keep = set()
        for original_name, cleaned_name in original_to_cleaned_names.items():
            if cleaned_name in schema_columns:
                columns_to_keep.add(original_name)
        # Conserver uniquement les colonnes qui correspondent au schéma
        cleaned_data = self.loaded_data.filter(columns_to_keep)

        self.logger.info(f"Nettoyage des colonnes terminé, {len(columns_to_keep)} colonnes conservées.")

        # To do better: specifif marchés publics rows cleaning (mixed with concessions data in source file)
        procedure_values = self.get_schema_values('procedure', 'enum')
        nature_values = self.get_schema_values('nature', 'enum')
        type_pattern = self.get_schema_value('_type', 'pattern')
        
        cleaned_data = cleaned_data[
            cleaned_data['procedure'].apply(self.matches_values, args=(procedure_values,)) |
            cleaned_data['nature'].apply(self.matches_values, args=(nature_values,)) |
            cleaned_data['_type'].str.match(type_pattern)
        ]

        return cleaned_data

    def clean_column_name_for_comparison(self, column_name):
        # Supprimer les indices numériques et les points supplémentaires du nom de colonne
        return re.sub(r'\.\d+\.', '.', column_name)
    
    def get_schema_values(self, property_name, column_name):
        # Récupérer les valeurs ou le pattern du schéma pour une propriété donnée
        values = self.schema.loc[self.schema['property'] == property_name, column_name].iloc[0]
        return [self.clean_value(value) for value in values] if isinstance(values, list) else self.clean_value(values)

    def get_schema_value(self, property_name, column_name):
        # Récupérer une valeur unique du schéma pour une propriété donnée
        return self.schema.loc[self.schema['property'] == property_name, column_name].iloc[0]

    def clean_value(self, value):
        # Nettoyer une valeur en retirant les accents, en mettant en minuscule et en supprimant certains caractères
        value = unidecode.unidecode(value).lower()
        return re.sub(r"[,']", "", value)

    def matches_values(self, value, values):
        # Vérifier si la valeur nettoyée correspond à l'une des valeurs nettoyées du schéma
        if pd.isna(value):
            return False
        cleaned_value = self.clean_value(value)
        return cleaned_value in values
    
    def select_data(self, config):
        cleaned_data = self.cleaned_data.copy()
        communities_data = self.communities_ids.copy()

        cleaned_data['siren'] = cleaned_data['acheteur.id'].str[:9].astype(str)
        communities_data['siren'] = communities_data['siren'].astype(str)

        selected_data = pd.merge(cleaned_data, communities_data, on='siren', how='left', validate="many_to_one")
        selected_data = selected_data.dropna(subset=['type'])

        return selected_data
    
        
    def remove_secondary_columns(self, config):
        # Supprimer les colonnes qui commencent par 'modifications.' ou 'titulaires.*.id' ou 'titulaires.*.typeIdentifiant'
        primary_data = self.selected_data.loc[:, ~self.selected_data.columns.str.contains(r'(modifications\.|titulaires\.\d+\.id|titulaires\.\d+\.typeIdentifiant)')]

        # Sélectionner les colonnes qui commencent par 'titulaires.*.denominationSociale'
        titulaires_cols = primary_data.filter(regex=r'^titulaires\.\d+\.denominationSociale')

        # Concaténer les valeurs de ces colonnes en une seule colonne, en ignorant les valeurs NaN
        primary_data['titulaires'] = titulaires_cols.apply(lambda row: ', '.join(row.dropna().astype(str).replace(r"[\[\]']", "")), axis=1)

        # Supprimer les colonnes 'titulaires.*.denominationSociale' originales
        primary_data = primary_data.drop(columns=titulaires_cols.columns)

        return primary_data
    
    def normalize_data(self, config):

        # Drop cleaned_data duplicates
        normalized_data = self.primary_data.applymap(lambda x: ','.join(map(str, x)) if isinstance(x, list) else x)
        normalized_data = normalized_data.drop_duplicates()

        # Cast data to schema types
        schema_selected = self.schema.loc[:, ['property', 'type']]        
        normalized_data = cast_data(normalized_data, schema_selected, "property", clean_column_name_for_comparison=self.clean_column_name_for_comparison)
        return normalized_data
    
