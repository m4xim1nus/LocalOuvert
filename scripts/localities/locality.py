class Locality:
    def __init__(self, insee_code, name, population, employees):
        self.insee_code = insee_code
        self.name = name
        self.population = population
        self.employees = employees
        # autres, du genre URL de l'OpenData gouv, l'URL de l'Opendata en propre ?

    # autres méthodes pour charger et sauvegarder les informations sur les localités

    
def load_localities_from_csv(csv_path):
    # Chargez les données du fichier CSV et créez des objets Locality
    # Retournez une liste d'objets Locality

def filter_localities(localities):
    # Filtrez les localités en fonction des critères de la loi
    # Retournez une liste de localités filtrées
