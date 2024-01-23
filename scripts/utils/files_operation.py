import logging
import os
import re

# TODO: encapsuler dans une classe
def save_csv(df, file_folder, file_name, sep=",", index=False):
    logger = logging.getLogger(__name__)
    # Vérifie si le répertoire existe, le crée si nécessaire
    if not os.path.exists(file_folder):
        os.makedirs(file_folder)

    df.columns = [re.sub(r"[.-]", "_", col.lower()) for col in df.columns] # to adjust column for SQL format and ensure consistency
    df.to_csv(file_folder / file_name, index=True, sep=sep)

    logger.info(f"Le fichier {file_name} a été enregistré dans le répertoire {file_folder}")