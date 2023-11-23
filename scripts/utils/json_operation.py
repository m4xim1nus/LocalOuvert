import pandas as pd
import logging

def flatten_schema_ref(prop, details, root_definitions):
    ref_path = details['$ref'].split('/')
    ref_detail = root_definitions
    for part in ref_path[4:]:
        ref_detail = ref_detail[part]

    # Si la référence pointe vers une structure avec des 'properties', traiter comme un objet
    if 'properties' in ref_detail:
        return flatten_schema_object(prop, ref_detail, root_definitions)

    return flatten_schema_property(prop, ref_detail, root_definitions)

def flatten_schema_array(prop, details, root_definitions):
    if 'items' in details:
        if '$ref' in details['items']:
            return flatten_schema_ref(prop, details['items'], root_definitions)
        elif 'properties' in details['items']:
            # Cas anormal : Si les items sont des objets, les traiter comme tels
            return flatten_schema_object(prop, details, root_definitions)
    return [{'property': prop}]

def flatten_schema_object(prop, details, root_definitions):
    flattened_schema = []
    for sub_prop, sub_details in details['properties'].items():
        flattened_schema.extend(flatten_schema_property(f"{prop}.{sub_prop}", sub_details, root_definitions))
    return flattened_schema

def flatten_schema_property(prop, details, root_definitions):
    if '$ref' in details:
        return flatten_schema_ref(prop, details, root_definitions)
    elif details.get('type') == 'array':
        return flatten_schema_array(prop, details, root_definitions)
    elif details.get('type') == 'object' and 'properties' in details:
        return flatten_schema_object(prop, details, root_definitions)
    else:
        return [{'property': prop, **details}]
    
def flatten_json_schema(schema, schema_name):
    definitions = schema['definitions'][schema_name]['definitions']
    properties = schema['definitions'][schema_name]['properties']
    flattened_schema = []

    for prop, details in properties.items():
        flattened_schema.extend(flatten_schema_property(prop, details, definitions))
    
    return flattened_schema


def flatten_object(obj, parent_key=''):
    logger = logging.getLogger(__name__)
    """
    Aplatit un objet JSON, préfixant les clés avec parent_key.
    """
    items = {}
    if obj is None:
        return items  # Retourner un dictionnaire vide si l'objet est None
    for key, value in obj.items():
        try:
            new_key = f"{parent_key}.{key}" if parent_key else key
            if isinstance(value, dict):
                items.update(flatten_object(value, new_key))
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                items.update(flatten_array_of_objects(value, new_key))  # Récurser si la valeur est une liste d'objets
            else:
                items[new_key] = value
        except Exception as e:
            logger.error(f"Erreur lors de l'aplatissage de l'objet JSON : {e}")
            raise e
    return items

def flatten_array_of_objects(array, parent_key):
    """
    Aplatit un array d'objets JSON, numérotant et préfixant les clés.
    """
    items = {}
    for i, obj in enumerate(array[:15], start=1):
        obj_items = flatten_object(obj, f"{parent_key}.{i}")
        for key, value in obj_items.items():
            items.setdefault(key, []).append(value)
    return items

def flatten_row(row):
    """
    Traite une ligne de données JSON et aplatit en fonction du type de données.
    """
    flattened_row = {}
    for key, value in row.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            flattened_row.update(flatten_array_of_objects(value, key))
        elif isinstance(value, dict):
            flattened_row.update(flatten_object(value, key))
        else:  # Direct value or empty list
            flattened_row[key] = value

    return flattened_row

def flatten_data(data):
    """
    Traite toutes les lignes de données JSON en utilisant process_row.
    """
    rows = []

    for row in data:
        if row is not None:
            flattened_row = flatten_row(row)
            if flattened_row:
                flattened_row = {key.replace('titulaires.titulaire', 'titulaires'): value for key, value in flattened_row.items()}
                rows.append(flattened_row)

    flattened_data = pd.DataFrame(rows)
    return flattened_data