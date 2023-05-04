# Projet "République Numérique" - Analyse de la transparence des collectivités locales

Ce projet vise à analyser la transparence des collectivités locales concernées par la loi "République Numérique" en cartographiant de manière ponctuelle la publication de certaines données jugées critiques en matière d'intérêt économique ou de probité politique (publication ou non, qualité des publications).

## Objectifs

1. Identifier les collectivités locales concernées par la loi “République Numérique” et rassembler des informations complémentaires sur ces collectivités.
2. Effectuer un scrapping différencié des données en fonction des sources disponibles (APIs, open data, sites web des collectivités) pour recueillir des informations essentielles.
3. Préparer et analyser les données récupérées pour évaluer la transparence des collectivités locales et contribuer au projet "République Numérique" d'Anticor.

## Plan d'attaque

1. Structuration des BDD collectivités
    * Recherche des collectivités locales concernées par la loi (plus de 3500 habitants, plus de 50 agents employés) à partir des données exposées par l'INSEE, ou d'autres organismes officielles
    * Compléments d'info "open data" sur ces collectivités si existant (via les données exposées par OpenDataFrance)
2. Structuration des infos recherchées
    * Identification et validation des 6 informations essentielles à récupérer par collectivité :
        - Subventions aux associations
        - Subventions aux entreprises
        - Passation de marchés publics
        - Plan local d’urbanisme
        - Indemnités des élus
        - Assiduité des élus aux séances et commissions de la collectivité
    * REcherche et/ou définition des standards des variables attendues, et des critères de filtre associés
    * Établissement des critères de qualité de la donnée (e.g. format des données, lisibilité, pré-filtrage, fréquence de mise à jour)
3. Scrapping différencié
    * Catégorisation des collectivités selon la facilité du scrapping
        - Données accessibles via API sur data.gouv.fr
        - Données accessibles via un portail open data propre à la collectivité
        - (si besoin) Données uniquement disponibles sur le site web de la collectivité
    * Scrapping spécifique selon la catégorie
    * Suivi et stockage de l'ensemble de ces scrapping
4. Préparation pour l'analyse des données récupérées (normalisation, extraction, pré-analyse)
5. Analyse

## Avancement

* Fait
    - Base de données complète des collectivités (hors arrondissements)
    - Scripts d'extraction et de traitement de jeux de données sur data gouv
    - Premier test concluant de bout en bout pour les régions & leurs subventions aux associations

* En cours
    - (max) Application du script du test régional à l'ensemble de la base 
    - (cycy) Exploration / structuration du scripts pour d'autres infos (subventions entreprises, marchés publics)

* A faire
    - Ajout des arrondissements

## Structure du projet

- `data/`: dossier pour stocker les données du projet, organisées en sous-dossiers
    - `communities/`: informations sur les collectivités
    - `datasets/`: données récupérées et filtrées
    - `processed_data/`: données traitées et prêtes pour l'analyse
- `docs/`: dossier pour la documentation du projet, organisée en sous-dossiers
    - `general/`: documentation non technique
    - `scrapers/`: documentation sur les scrapers
    - `communities/`: documentation sur la gestion des collectivités
    - `data_processing/`: documentation sur le traitement des données
    - `analysis/`: documentation sur l'analyse des données
- `scripts/`: dossier pour les scripts Python du projet, organisés en sous-dossiers
    - `communities/`: scripts pour la gestion des collectivités
    - `datasets/`: scripts pour le scrapping et le filtrage des données
    - `data_processing/`: scripts pour le traitement des données
    - `analysis/`: scripts pour l'analyse des données
    - `utils/`: scripts utilitaires et helpers
- `tests/`: dossier pour les tests unitaires du projet, organisés en sous-dossiers
    - `scrapers/`: tests pour les scrapers
    - `communities/`: tests pour la gestion des collectivités
    - `data_processing/`: tests pour le traitement des données
    - `analysis/`: tests pour l'analyse des données
- `manage.py`: script principal pour exécuter les différentes commandes du projet
- `requirements.txt`: fichier contenant les dépendances Python
- `README.md`: ce fichier


## Comment utiliser à date

1. Clonez ce dépôt. 
 - Si vous souhaitez cloner sans les données, utilisez la **branche without-data** `:git clone -b without-data https://github.com/m4xim1nus/LocalOuvert.git`
 - Pour cloner avec les données, utilisez la **branche principale** : `git clone https://github.com/m4xim1nus/LocalOuvert.git`

2. Installez les dépendances à l'aide de `pip install -r requirements.txt`.

3. Si vous avez cloné la branche sans les données, vous pouvez exécuter les scripts pour télécharger et traiter les données en utilisant `manage.py`. Le script `run_all` exécute les 4 scripts suivants :
- "run_communities_extraction"
- "run_communities_selection"
- "run_datasets_selection"
- "run_datafiles_selection"

Pour exécuter tous les scripts en une seule commande, utilisez : ` python manage.py run_all `

4. Les collectivités filtrées sont identifiées dans [ce fichier](./data/communities/processed_data/selected_communities.csv), et ces données peuvent être explorées dans [ce notebook](./scripts/communities/selection_explorer.ipynb).

5. Les données récoltées sont identifiées dans [ce fichier](./data/datasets/filtered_detailed_data.csv), et ces données peuvent être explorées dans [ce notebook](./scripts/datasets/datasets_explorer.ipynb).