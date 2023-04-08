# Projet "République Numérique" - Analyse de la transparence des collectivités locales

Ce projet vise à analyser la transparence des collectivités locales concernées par la loi "République Numérique" en cartographiant de manière ponctuelle la publication de certaines données jugées critiques en matière d'intérêt économique ou de probité politique (publication ou non, qualité des publications).

## Objectifs

1. Identifier les collectivités locales concernées par la loi “République Numérique” et rassembler des informations complémentaires sur ces collectivités.
2. Effectuer un scrapping différencié des données en fonction des sources disponibles (APIs, open data, sites web des collectivités) pour recueillir des informations essentielles.
3. Préparer et analyser les données récupérées pour évaluer la transparence des collectivités locales et contribuer au projet "République Numérique" d'Anticor.

## Plan d'attaque

1. Structuration des BDD localités
    * Recherche des collectivités locales concernées par la loi (plus de 3500 habitants, plus de 50 agents employés) à partir des données exposées par l'INSEE sur data.gouv.fr
    * Compléments d'info "open data" sur ces collectivités si existant (via les données exposées par OpenDataFrance)
2. Structuration des infos recherchées
    * Identification et validation des 6 informations essentielles à récupérer par localité :
        - Subventions aux associations
        - Subventions aux entreprises
        - Passation de marchés publics
        - Plan local d’urbanisme
        - Indemnités des élus
        - Assiduité des élus aux séances et commissions de la collectivité
    * Définition des fuzzy matches pour identifier ces informations et des standards des variables attendues
    * Établissement des critères de qualité de la donnée (e.g. format des données, lisibilité, pré-filtrage, fréquence de mise à jour)
3. Scrapping différencié
    * Catégorisation des localités selon la facilité du scrapping
        - Données accessibles via API sur data.gouv.fr
        - Données accessibles via un portail open data propre à la collectivité
        - Données uniquement disponibles sur le site web de la collectivité
    * Scrapping spécifique selon la catégorie
    * Suivi et stockage de l'ensemble de ces scrapping
4. Préparation pour l'analyse des données récupérées (normalisation, extraction, pré-analyse)
5. Analyse

## Structure du projet à date

- `data/`: dossier pour stocker les données récupérées
- `docs/`: dossier pour la documentation du projet
- `scripts/`: dossier pour les scripts Python utilisés pour le scraping
- `requirements.txt`: fichier contenant les dépendances Python
- `api_urls.csv`: fichier contenant les URLs des APIs et les noms des fichiers CSV associés
- `README.md`: ce fichier

## Comment utiliser à date

1. Installez les dépendances à l'aide de `pip install -r requirements.txt`.
2. Exécutez runtest dans le script `manage.py` pour récupérer les données des APIs et les sauvegarder dans des fichiers CSV.