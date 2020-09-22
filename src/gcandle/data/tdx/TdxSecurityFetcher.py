import pandas as pd
import gcandle.utils.date_time_utils as date_time_utils
from pytdx.hq import TdxHq_API
from gcandle.data.SecurityFetcher import SecurityFetcher
from gcandle.data.se_types import Exchange, SeFreq, SeType

#map to a list with [frequence number, count factor of days]
map_freq_to_tdx_number = {
    SeFreq.ONE_MIN: [8, 240],
    SeFreq.FIVE_MIN: [0, 48],
    SeFreq.FIFTEEN_MIN: [1, 16],
    SeFreq.THIRTY_MIN: [2, 8],
    SeFreq.DAY: [9, 1],
    SeFreq.WEEK: [5, 0.2],
    SeFreq.MONTH: [6, 0.02],
    SeFreq.YEAR: [11, 0.005]
}

map_exchange_to_tdx_number = {
    Exchange.SZSE: 0,
    Exchange.SHSE: 1
}

MAX_DAYS = 4000
BATCH_SIZE = 800
STOCK_DATA_SERVER = {
    "ip": "221.194.181.176",
    "port": 7709
}

class TdxSecurityFetcher(SecurityFetcher):
    def __init__(self):
        super().__init__()
        self.server = STOCK_DATA_SERVER

    def __do_fetch_security_list(self, api: TdxHq_API, exchange: Exchange):
        arr = []
        start = 0
        ex_nbr = map_exchange_to_tdx_number[exchange]
        count = api.get_security_count(ex_nbr)
        while start < count:
            tmp = api.get_security_list(ex_nbr, start)
            if tmp is None:
                break
            sz = len(tmp)
            if sz < 1:
                break
            start += sz
            arr.extend(tmp)

        print('total fetched rows: {}'.format(start))
        df = api.to_df(arr).assign(ex=exchange.name)
        df = df.assign(
            stype=df.code.apply(exchange.code_to_type_str)
        ).set_index(['code', 'ex'], drop=True)
        return df[df.stype.notnull()]

    def fetch_security_list(self):
        api = TdxHq_API()
        with api.connect(self.server['ip'], self.server['port'], time_out=3):
            data = pd.concat([
                self.__do_fetch_security_list(api, ex) for ex in Exchange], axis=0
            )
        return data

    def fetch_bars(self, typ: SeType, code, start, end, freq):
        try:
            tdxApi = TdxHq_API()
            ex_nbr = map_exchange_to_tdx_number[Exchange.from_type_and_code(typ, code)]
            if typ == SeType.Stock or typ == SeType.ETF:
                tdx_fetch_func = tdxApi.get_security_bars
            elif typ == SeType.Index:
                tdx_fetch_func = tdxApi.get_index_bars
            else:
                raise Exception('Unsupported se type')

            #timeout is in seconds.
            with tdxApi.connect(self.server['ip'], self.server['port'], time_out=3):
                frequence = map_freq_to_tdx_number[freq][0]
                days_between = date_time_utils.Date.from_str(end).delta_to(start).days + 1
                if days_between > MAX_DAYS:
                    days_between = MAX_DAYS
                max_count = int(map_freq_to_tdx_number[freq][1] * days_between)
                tmp_list = []
                n_start = 0
                while n_start < max_count:
                    fetch_size = min(max_count - n_start, BATCH_SIZE)
                    tmp_res = tdx_fetch_func(
                        frequence,
                        ex_nbr,
                        code, n_start, fetch_size
                    )
                    if tmp_res is None or len(tmp_res) < 1:
                        break
                    n_start += len(tmp_res)
                    tmp_list.extend(tmp_res)
                if n_start == 0:
                    print('No rows fetched for {}'.format(code))
                # print('total fetched rows for {}: {}'.format(code, n_start))
                data = tdxApi.to_df(tmp_list)

                if len(data) < 1:
                    return None
                data = data[data['open'] != 0]
                cols_to_drop = ['year', 'month', 'day']

                if freq.is_minutes():
                    data = data.assign(datetime=data['datetime'].apply(lambda x: str(x[0:19])),
                                       date=data['datetime'].apply(lambda x: str(x[0:10])),
                                       code=str(code)) \
                        .set_index('datetime', drop=True, inplace=False) \
                        .drop(cols_to_drop, axis=1)
                else:
                    cols_to_drop.extend(['datetime', 'hour', 'minute'])
                    data = data.assign(datetime=data['datetime'],
                                       date=data['datetime'].apply(lambda x: str(x[0:10])),
                                       code=str(code)) \
                        .set_index('date', drop=True, inplace=False) \
                        .drop(cols_to_drop, axis=1)
                return data
        except Exception as e:
            print('exception caught: {}'.format(e))

    def fetch_xdxr(self, code):
        ip, port = self.server['ip'], self.server['port']
        api = TdxHq_API()
        ex_nbr = map_exchange_to_tdx_number[Exchange.from_type_and_code(SeType.Stock, code)]
        with api.connect(ip, port, time_out=3):
            data = api.to_df(api.get_xdxr_info(ex_nbr, code))
            if len(data) >= 1:
                data = data \
                    .assign(date=pd.to_datetime(data[['year', 'month', 'day']])) \
                    .drop(['year', 'month', 'day'], axis=1) \
                    .assign(code=str(code))
                return data.assign(date=data['date'].apply(lambda x: str(x)[0:10]))\
                    .set_index('date', drop=True, inplace=False)
            else:
                return None
