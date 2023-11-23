import logging
import pandas as pd
import psycopg2
from io import StringIO
import os
from dotenv import load_dotenv

load_dotenv()  # Charge les variables d'environnement à partir du fichier .env

class PSQLConnector:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.dbname = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")

    def connect(self):
        try:
            self.connection = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host,port=self.port)
            self.cursor = self.connection.cursor()
            logger.info("Connexion à la base de données réussie")
        except Exception as e:
            self.logger.info(f"Erreur lors de la connexion à la base de données: {e}")

    def save_communities_to_sql(self,df):
        # Votre DataFrame Pandas
        # dataframe = pd.read_csv('votre_fichier.csv')  # Si vous chargez depuis un fichier CSV
        # Assurez-vous que les colonnes du DataFrame correspondent à la table

        # Just in Case
        df["trancheEffectifsUniteLegale"] = df["trancheEffectifsUniteLegale"].astype('Int64')
        
        try:
            truncate_query = "TRUNCATE TABLE public.communities"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            self.logger.info("Table vidée avec succès.")
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.info("Erreur lors du vidage de la table :", error)
            self.connection.rollback()
        # Sauvegarder le DataFrame dans un buffer en mémoire sous forme CSV
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        # Préparer la requête COPY
        copy_query = """
            COPY public.communities(nom, siren, type, "COG", "COG_3digits", code_departement, 
            code_departement_3digits, code_region, population, "EPCI", url_ptf, url_datagouv, 
            id_datagouv, merge, ptf, "trancheEffectifsUniteLegale", "EffectifsSup50")
            FROM STDIN WITH CSV DELIMITER '\t' NULL ''
        """

        # Copier les données du buffer dans la table
        try:
            self.cursor.copy_expert(copy_query, output)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.info("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()
            return 1

        self.logger.info("Copy successful")

    
    def save_normalized_data_to_sql(self,df,chunk_size=1000):
        # Votre DataFrame Pandas
        # dataframe = pd.read_csv('votre_fichier.csv')  # Si vous chargez depuis un fichier CSV
        # Assurez-vous que les colonnes du DataFrame correspondent à la table

        # Reformating (to discuss if we keep it here)
        
        df['record_id'] = df.index
        df["export_date"] = pd.Timestamp.now().strftime('%Y-%m-%d')
        col_order = ['record_id', 'export_date'] + [col for col in df.columns if col not in ['record_id', 'export_date']]
        df = df[col_order]
        df = df.drop(columns=["nom","type","source"])

        try:
            truncate_query = "TRUNCATE TABLE public.normalized_data"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            self.logger.info("Table vidée avec succès.")
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.info("Erreur lors du vidage de la table :", error)
            self.connection.rollback()
        # Sauvegarder le DataFrame dans un buffer en mémoire sous forme CSV
        
        for k in range(0,df.index.size,chunk_size):
            output = StringIO()
            df[k:k+chunk_size].to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            # Préparer la requête COPY


            # Copier les données du buffer dans la table
            try:
                self.cursor.copy_expert("COPY public.normalized_data FROM STDIN WITH CSV DELIMITER '\t' NULL ''", output)
                self.connection.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                self.logger.info("Error: %s" % error)
                self.connection.rollback()
                return 1

            print("Copy successful")

    def insert_data_in_chunks(self, data, table_name, chunk_size=1000):
    """
    Insère les données en petits lots dans la table spécifiée.

    :param data: DataFrame contenant les données à insérer.
    :param table_name: Nom de la table où insérer les données.
    :param chunk_size: Taille de chaque lot.
    """

    # Assurez-vous que la connexion est établie
    if not self.connection:
        self.connect()

    for start in range(0, len(data), chunk_size):
        end = min(start + chunk_size, len(data))
        chunk = data.iloc[start:end]

        try:
            # Création d'un buffer pour l'insertion en masse
            buffer = StringIO()
            chunk.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Utilisation de la commande COPY pour l'insertion
            with self.connection.cursor() as cursor:
                cursor.copy_from(buffer, table_name, sep=",", columns=chunk.columns)
                self.connection.commit()

            self.logger.info(f"Lot de données inséré : lignes {start} à {end}")
        except Exception as e:
            logger.error(f"Erreur lors de l'insertion du lot : {e}")
            self.connection.rollback()

    # Fermeture de la connexion après l'insertion
    self.close()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        self.logger.info("Connexion à la base de données fermée")

# Exemple d'utilisation
# connector = PSQLConnector("dbname", "user", "password", "host", "port")
# connector.connect()

# connector.save_communities_to_sql(sel_com_df)

# connector.save_normalized_data_to_sql(normalized_df)
