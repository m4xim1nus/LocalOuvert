import logging
import os
import csv
import gzip
import time
import requests
import pandas as pd
from io import StringIO
from requests.exceptions import Timeout

# Configure the logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# Fonction pour détecter le délimiteur d'un fichier CSV
def detect_delimiter(text, num_lines=5, delimiters=None):
    if delimiters is None:
        delimiters = [',', ';', '\t', '|']

    counts = {delimiter: 0 for delimiter in delimiters}
    line_counts = {delimiter: 0 for delimiter in delimiters}

    for line_number, line in enumerate(StringIO(text)):
        if line_number >= num_lines:
            break
        for delimiter in delimiters:
            if delimiter in line:
                counts[delimiter] += line.count(delimiter)
                line_counts[delimiter] += 1

    # Compute average occurrences per line for each delimiter
    averages = {delimiter: counts[delimiter] / line_counts[delimiter] for delimiter in delimiters if line_counts[delimiter] > 0}

    return max(averages, key=averages.get)

# Fonction pour télécharger un fichier CSV
def load_from_url(url, dtype=None, columns_to_keep=None, num_retries=3, delay_between_retries=5):
    for attempt in range(num_retries):
        try:
            response = requests.get(url)

            content_type = response.headers.get('content-type')
            if 'json' in content_type:
                return response.json()
            else:
                content = response.content
                if 'gzip' in content_type:
                    content = gzip.decompress(content)
                
                #Détection de l'encoding
                encoding = 'utf-8' #chardet.detect(content)['encoding']

                #Décodage du contenu
                decoded_content = content.decode(encoding)

                delimiter = detect_delimiter(decoded_content)
                if columns_to_keep is not None:
                    df = pd.read_csv(StringIO(decoded_content), delimiter=delimiter, dtype=dtype, usecols=lambda c: c in columns_to_keep, error_bad_lines=False, quoting=csv.QUOTE_MINIMAL)
                else:
                    df = pd.read_csv(StringIO(decoded_content), delimiter=delimiter, dtype=dtype, error_bad_lines=False, quoting=csv.QUOTE_MINIMAL)
                return df
        except Timeout:
            if attempt < num_retries - 1:
                logger.warning(f"Le téléchargement a échoué en raison d'un timeout. Tentative de réessai après {delay_between_retries} secondes...")
                time.sleep(delay_between_retries)
            else:
                logger.error(f"Le téléchargement a échoué après {num_retries} tentatives en raison d'un timeout.")
                break
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement du fichier CSV à l'URL : {url}")
            logger.error(f"Erreur : {e}")
            break
    return None

# Fonction pour télécharger et traiter plusieurs fichiers CSV
def download_and_process_data(urls_list, dtype=None):
    data_frames = []
    for url in urls_list:
        df = load_from_url(url, dtype=dtype)
        if df is not None:
            data_frames.append(df)
    return data_frames

# Téléchargement des fichiers Excel 
def load_from_path(file_path, dtype=None):
    return pd.read_excel(file_path, dtype=dtype)

def save_csv(df, file_folder, file_name, sep=","):
    # Vérifie si le répertoire existe, le crée si nécessaire
    if not os.path.exists(file_folder):
        os.makedirs(file_folder)

    df.to_csv(file_folder / file_name, index=False, sep=sep)
    logger.info(f"Le fichier {file_name} a été enregistré dans le répertoire {file_folder}")