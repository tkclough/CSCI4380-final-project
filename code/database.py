import psycopg2

class DBMSFinalProject:
    def __init__(self, connection_string):
        self.conn = psycopg2.connect(connection_string)

    def precipition_vs_num_incidents(self):
        pass

    def property_damage_vs_precipitation(self):
        pass

    def precipitation_by_city(self, city_name):
        pass

    def total_precipitation_by_state(self, state):
        pass

    def total_incidents_by_city(self, city_name):
        pass
