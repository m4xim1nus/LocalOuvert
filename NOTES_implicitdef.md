# Start virtual environnement

    python3 -m venv loc_env
    source loc_env/bin/activate

Then run pip, python, etc.

When you're done working on your project, you can deactivate the virtual environment by running:

    deactivate

# faire tourner le projet

    pip install -r requirements.txt
    python main.py config.yaml

If not in virtual environnement, use pip3 and python3 instead.

# importer dans SQLite

    sqlite3

    .open ./data/datasets/mydb.db

    CREATE TABLE normalized_data (
        nomAttribuant TEXT,
        idAttribuant TEXT,
        dateConvention DATE,
        referenceDecision TEXT,
        nomBeneficiaire TEXT,
        idBeneficiaire TEXT,
        rnaBeneficiaire TEXT,
        objet TEXT,
        montant REAL,
        nature TEXT,
        conditionsVersement TEXT,
        datesPeriodeVersement DATE,
        idRAE TEXT,
        notificationUE TEXT,
        pourcentageSubvention REAL,
        dispositifAide TEXT,
        siren TEXT,
        organization TEXT,
        title TEXT,
        created_at DATETIME,
        url TEXT
    );


    .mode csv
    .import ./data/datasets/normalized_data.csv normalized_data

# Start datasette on this SQLite db

    datasette serve ./data/datasets/mydb.db
