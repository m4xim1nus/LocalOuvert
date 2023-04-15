import csv
import pandas as pd
from community import Community  # Assurez-vous que la classe Community est importable

# Lire les fichiers CSV et créer des DataFrames
communes_df = pd.read_csv('../../data/communities/insee_cog_2023/v_commune_2023.csv', encoding='utf-8')
cantons_df = pd.read_csv('../../data/communities/insee_cog_2023/v_canton_2023.csv', encoding='utf-8')
comer_df = pd.read_csv('../../data/communities/insee_cog_2023/v_comer_2023.csv', encoding='utf-8')
commune_comer_df = pd.read_csv('../../data/communities/insee_cog_2023/v_commune_comer_2023.csv', encoding='utf-8')
ctcd_df = pd.read_csv('../../data/communities/insee_cog_2023/v_ctcd_2023.csv', encoding='utf-8')
departements_df = pd.read_csv('../../data/communities/insee_cog_2023/v_departement_2023.csv', encoding='utf-8')
regions_df = pd.read_csv('../../data/communities/insee_cog_2023/v_region_2023.csv', encoding='utf-8')

# Créer une liste pour stocker les objets Community
communities = []

# Fonction pour ajouter des objets Community à la liste
def create_communities(df, community_type):
    for index, row in df.iterrows():
        community = Community(
            code=row['COM'] if 'COM' in row else row['CAN'] if 'CAN' in row else row['DEP'] if 'DEP' in row else row['REG'] if 'REG' in row else row['COMER'],
            name=row['NCCENR'],
            community_type=community_type,
            region_code=row['REG'] if 'REG' in row else None,
            department_code=row['DEP'] if 'DEP' in row else None
        )
        communities.append(community)

# Parcourir les DataFrames et créer des objets Community pour chaque type de collectivité
create_communities(communes_df, 'Commune')
create_communities(cantons_df, 'Canton')
create_communities(comer_df, 'Collectivité d\'outre-mer')
create_communities(commune_comer_df, 'Commune des Collectivités d\'outre-mer')
create_communities(ctcd_df, 'Collectivité territoriale ayant les compétences départementales')
create_communities(departements_df, 'Département')
create_communities(regions_df, 'Région')

# Créer une liste de dictionnaires à partir des objets Community
communities_dicts = [community.to_dict() for community in communities]

# Créer un DataFrame à partir de la liste des dictionnaires et écrire le fichier CSV
communities_df = pd.DataFrame(communities_dicts)
communities_df.to_csv('../../data/communities/communities.csv', index=False, encoding='utf-8')
