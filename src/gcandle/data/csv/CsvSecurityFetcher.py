import pandas as pd
from gcandle.data.SecurityFetcher import SecurityFetcher
from gcandle.data.se_types import Exchange, SeFreq, SeType
from gcandle.utils.date_time_utils import Date


CSV_FILE_DIR = '/home/ych/stock/'

MAP_EX_TO_CSV_PREFIX = {
    Exchange.SZSE: 'SZ',
    Exchange.SHSE: 'SH'
}

class CsvSecurityFetcher(SecurityFetcher):
    def __init__(self):
        super().__init__()

    def fetch_security_list(self):
        print('Not implemented')
        pass

    def file_name(self, code, freq):
        exchange = Exchange.from_type_and_code(SeType.Stock, code)
        return CSV_FILE_DIR + 'stock_' + freq.value + '/' + MAP_EX_TO_CSV_PREFIX[exchange] + code + '.CSV'

    def fetch_bars(self, typ: SeType, code, start, end, freq):
        try:
            cols = ['date', 'time', 'open', 'high', 'low', 'close', 'vol', 'amount']
            data = pd.read_csv(self.file_name(code, freq), names=cols)
            if len(data) < 1:
                return None

            if freq.is_minutes():
                data['datetime'] = data['date'] + ' ' + data['time']
                data = data.assign(code=str(code)) \
                    .set_index('datetime', drop=True, inplace=False)
            return data
        except Exception as e:
            print('exception caught: {}'.format(e))

