import pandera as pa
from pandera import Column, DataFrameSchema, Check, Index
# Odf Schema
OdfSchema = DataFrameSchema(
    {
        "siren": Column(int),
        "nom": Column(str),
        "type": Column(str), # TODO: We could highlight here all the values it could take...
        "regcode": Column(str),
        "url_ptf": Column(str, nullable=True),
        "nb_ptf": Column("Int64", nullable=True), # Int64 to manage Null values
        "url_datagouv": Column(str, nullable=True),
        "nb_datagouv": Column("Int64", nullable=True),
        "id_datagouv": Column(str, nullable=True),
        "id_ods": Column(str, nullable=True),
        "pop_insee": Column("Int64", nullable=True),
        "lat": Column(float,nullable=True),
        "long": Column(float,nullable=True),
        "merge": Column(str,nullable=True),
        "depcode": Column(str,nullable=True),
        "ptf": Column(str,nullable=True),
        "thématiques": Column(str,nullable=True),
    },
    coerce=True,
    
)