import os
import csv
import requests
from datetime import datetime, timedelta

def download_data(url, csv_filename):
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data['records'][0]['fields'].keys())
        for record in data['records']:
            writer.writerow(record['fields'].values())

def is_csv_up_to_date(csv_filename, days=7):
    if not os.path.exists(csv_filename):
        return False

    file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(csv_filename))
    return file_age <= timedelta(days=days)

# Premier jeu de données
url1 = "https://data.iledefrance.fr/api/records/1.0/search/?dataset=subventions-versees-en-2013-aux-associations-ou-aux-fondations&q=&facet=gestion&rows=10000"
csv_filename1 = "data/regions/idf/subventions-2013.csv"

if not is_csv_up_to_date(csv_filename1):
    download_data(url1, csv_filename1)
    print(f"{csv_filename1} mis à jour.")

# Deuxième jeu de données (à remplacer par l'URL de l'autre base de données)
url2 = "https://data.iledefrance.fr/api/records/1.0/search/?dataset=subventions-versees-aux-associations-par-la-region-ile-de-france&q=&facet=gestion&rows=10000"
csv_filename2 = "data/regions/idf/subventions-2016-17.csv"

if not is_csv_up_to_date(csv_filename2):
    download_data(url2, csv_filename2)
    print(f"{csv_filename2} mis à jour.")
