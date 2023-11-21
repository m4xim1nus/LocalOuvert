import logging
import pandas as pd
import psycopg2
from io import StringIO

class PSQLConnector:
    def __init__(self, dbname, user, password, host,port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        logger = logging.getLogger(__name__)
        try:
            self.connection = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host,port=self.port)
            self.cursor = self.connection.cursor()
            logger.info("Connexion à la base de données réussie")
        except Exception as e:
            logger.info(f"Erreur lors de la connexion à la base de données: {e}")

    def save_communities_to_sql(self,df):
        logger = logging.getLogger(__name__)
        # Votre DataFrame Pandas
        # dataframe = pd.read_csv('votre_fichier.csv')  # Si vous chargez depuis un fichier CSV
        # Assurez-vous que les colonnes du DataFrame correspondent à la table

        # Just in Case
        df["trancheEffectifsUniteLegale"] = df["trancheEffectifsUniteLegale"].astype('Int64')
        
        try:
            truncate_query = "TRUNCATE TABLE public.communities"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            logger.info("Table vidée avec succès.")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info("Erreur lors du vidage de la table :", error)
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
            logger.info("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()
            return 1

        logger.info("Copy successful")

    
    def save_normalized_data_to_sql(self,df,chunk_size=1000):
        # Votre DataFrame Pandas
        # dataframe = pd.read_csv('votre_fichier.csv')  # Si vous chargez depuis un fichier CSV
        # Assurez-vous que les colonnes du DataFrame correspondent à la table
        logger = logging.getLogger(__name__)

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
            logger.info("Table vidée avec succès.")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info("Erreur lors du vidage de la table :", error)
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
                logger.info("Error: %s" % error)
                self.connection.rollback()
                return 1

            print("Copy successful")

    def close_connection(self):
        logger = logging.getLogger(__name__)
        self.cursor.close()
        self.connection.close()
        logger.info("Connexion à la base de données fermée")

# Exemple d'utilisation
# connector = PSQLConnector('postgres', 'postgres.gaaokuyqioaobxxyapbt', '7kFFZ8xNa9T8Bqm', 'aws-0-eu-central-1.pooler.supabase.com',6543)
# connector.connect()

# connector.save_communities_to_sql(sel_com_df)

# connector.save_normalized_data_to_sql(normalized_df)
