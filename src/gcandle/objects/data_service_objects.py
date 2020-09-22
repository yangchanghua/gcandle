from gcandle.data.csv.CsvSecurityFetcher import CsvSecurityFetcher
from gcandle.data.tdx.TdxSecurityFetcher import TdxSecurityFetcher
from gcandle.data.SecurityDataService import SecurityDataService

SECURITY_DATA_READ_SERVICE = SecurityDataService()

TDX_SECURITY_DATA_UPDATE_SERVICE = SecurityDataService(TdxSecurityFetcher())

CSV_SECURITY_DATA_SERVICE = SecurityDataService(CsvSecurityFetcher())

if __name__ == '__main__':
    print('Defines objects that to be used for security data service')
