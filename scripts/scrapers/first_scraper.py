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

# Lire les URLs et les noms de fichiers CSV à partir de api_urls.csv
script_dir = os.path.dirname(os.path.abspath(__file__))
api_urls_file_path = os.path.join(script_dir, 'api_urls.csv')
with open(api_urls_file_path, mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        url = row['url']
        csv_filename = row['csv_filename']

    if not is_csv_up_to_date(csv_filename):
        download_data(url, csv_filename)
        print(f"{csv_filename} mis à jour.")