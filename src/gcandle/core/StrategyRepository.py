from gcandle.objects.basic_objects import DB_CLIENT

class StrategyRepository:
    def __init__(self):
        self._db_connection = DB_CLIENT
        self._db_connection.set_dbName()
        self.client = self._db_connection.get_client()

