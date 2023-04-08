class Community:
    def __init__(self, code, name, community_type, region_code, department_code, population=None, territorial_agents=None):
        self.code = code
        self.name = name
        self.community_type = community_type
        self.region_code = region_code
        self.department_code = department_code
        self.population = population
        self.territorial_agents = territorial_agents
        self.additional_data = {}

    def to_dict(self):
        return {
            'code': self.code,
            'name': self.name,
            'community_type': self.community_type,
            'region_code': self.region_code,
            'department_code': self.department_code,
            'population': self.population,
            'territorial_agents': self.territorial_agents
        }

    def load_additional_data(self, data_source):
        """
        Charge les données supplémentaires depuis une source de données spécifique (par exemple, un fichier CSV)
        """
        pass  # implémenter la logique pour charger les données depuis data_source

    def __repr__(self):
        return f"Community(code={self.code}, name={self.name}, community_type={self.community_type}, region_code={self.region_code}, department_code={self.department_code}, population={self.population}, territorial_agents={self.territorial_agents})"
