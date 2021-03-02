from gcandle.utils.DatabaseClient import DatabaseClient
from gcandle.utils.SecurityDataRepository import SecurityDataRepository


DB_CLIENT = DatabaseClient()
SEC_DATA_REPO = SecurityDataRepository()
SEC_DATA_REPO.set_client(DB_CLIENT)