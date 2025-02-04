# Projet "République Numérique" - Analyse de la transparence des collectivités locales

Ce projet vise à analyser la transparence des collectivités locales concernées par la loi "République Numérique" en cartographiant de manière ponctuelle la publication de certaines données jugées critiques en matière d'intérêt économique ou de probité politique (publication ou non, qualité des publications).

CE POC EST MAINTENANT ACHEVÉ : PLACE Á ÉCLAIREUR PUBLIC ET DATA FOR GOOD !

## Objectif

QUE VIVE ET PROSPÈRE ÉCLAIREUR PUBLIC !!!

## Plan d'attaque & Avancement

Contacter Data for Good

## Structure du projet

- `data/`: dossier pour stocker les données du projet, organisées en sous-dossiers
    - `communities/`: informations sur les collectivités
    - `datasets/`: données récupérées et filtrées
    - `processed_data/`: données traitées et prêtes pour l'analyse
- `scripts/`: dossier pour les scripts Python du projet, organisés en sous-dossiers
    - `workflow/` : script gérant le workflow général
    - `communities/`: scripts pour la gestion des collectivités
    - `datasets/`: scripts pour le scrapping et le filtrage des données
    - `data_processing/`: scripts pour le traitement des données
    - `analysis/`: scripts pour l'analyse des données (vide à date)
    - `loaders/`: scripts de téléchargement de fichiers 
    - `utils/`: scripts utilitaires et helpers
- `main.py`: script principal pour exécuter les scripts du projet
- `config.yaml`: fichier de configuration pour faire tourner `main.py`.
- `requirements.txt`: fichier contenant les dépendances Python
 - `.gitignore`: fichier contenant les références ignorées par git
- `README.md`: ce fichier


## Comment utiliser à date

1. Clonez ce dépôt : 
```
git clone https://github.com/m4xim1nus/LocalOuvert.git
cd LocalOuvert
```

2. (Recommandé) Créez un environnement virtuel pour éviter les conflits de dépendances :
```
python -m venv venv
# Activation de l’environnement virtuel
source venv/bin/activate  # Sur macOS/Linux
venv\Scripts\activate     # Sur Windows
```

3. Installez les dépendances à l'aide de 
```
pip install -r requirements.txt
```

4. Pour exécuter les scripts pour télécharger et traiter les données, executez 
```
python main.py config.yaml 
```


## License

### Code

The code in this repository is licensed under the MIT License:

MIT License

Copyright (c) 2023 Max Lévy

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### Data and Analyses

Unless otherwise stated, the data and analyses in this repository are licensed under the Creative Commons Attribution 4.0 International (CC BY 4.0) License. For more information, please visit [Creative Commons License](https://creativecommons.org/licenses/by/4.0/).