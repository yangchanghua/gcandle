from concurrent.futures import ThreadPoolExecutor
import gcandle.utils.date_time_utils as date_time_utils
from gcandle.objects.basic_objects import DB_CLIENT
import gcandle.utils.multi_process_pool as executor
from gcandle.data.SecurityFetcher import SecurityFetcher
from gcandle.data.se_types import SeFreq, SeType
from gcandle.utils.CodeListTaskExecutor import CodeListTaskExecutor

MIN_START_TIME = '2006-01-01'
REPO_XDXR = 'security_xdxr'
REPO_SECURITY_LIST = 'security_list'
REPO_QFQ_SUFFIX = '_qfq'
DAYS_TO_UPDATE = 16


class SecurityDataService:
    def __init__(self, fetcher: SecurityFetcher = None):
        self.fetcher = fetcher
        self.failed_codes = []

    def set_fetcher(self, fetcher: SecurityFetcher):
        self.fetcher = fetcher

    def read_security_codes(self, typ: SeType = SeType.Stock):
        return DB_CLIENT.read_codes_from_db(REPO_SECURITY_LIST, filter={'stype': typ.value})

    def read_fq_info(self, code, start, end):
        data = DB_CLIENT.read_data_by_codes_and_date(REPO_XDXR, [code], start, end=end, extra_filter={'category': 1})
        return data

    def read_qfq_security_data_for_single(self, code, start, end, typ: SeType = SeType.Stock, freq: SeFreq = SeFreq.DAY):
        codes = [code]
        data = DB_CLIENT.read_data_by_codes_and_date(typ.repo_name(freq) + REPO_QFQ_SUFFIX, codes, start, end)
        if data is None:
            return None
        return data[data.vol > 1]

    def read_security_data_for_single(self, code, start, end, typ: SeType = SeType.Stock, freq: SeFreq = SeFreq.DAY):
        codes = [code]
        data = DB_CLIENT.read_data_by_codes_and_date(typ.repo_name(freq), codes, start, end)
        if data is None:
            return None
        if 'vol' in data.columns:
            return data[data.vol > 1]
        else:
            return data

    def read_security_data_for_list(self, codes, start, end, typ: SeType = SeType.Stock, freq: SeFreq = SeFreq.DAY):
        return DB_CLIENT.read_data_by_codes_and_date(typ.repo_name(freq), codes, start, end)

    def do_fetch_and_save_bars(self, code, typ: SeType, start, end, freq: SeFreq):
        repo_name = typ.repo_name(freq)
        end_date = date_time_utils.Date().as_str()
        max_date_in_db = DB_CLIENT.read_max_date_for(code, repo_name)
        if max_date_in_db is None:
            start_date = MIN_START_TIME
        else:
            start_date = date_time_utils.Date.from_str(max_date_in_db).as_str()
        try:
            if start_date != end_date:
                df = self.fetcher.fetch_bars(typ, code, start, end, freq)
                if len(df) < 1:
                    self.failed_codes.append(code)
                else:
                    df = self.remove_columns_for_min_data(df, freq)
                    DB_CLIENT.update_data_append_newer(df, repo_name)
        except Exception as e:
            print('Failed to fetch bars for {} {}, reason={}'.format(code, freq, e))
            self.failed_codes.append(str(code))

    def do_fetch_and_replace_by_dates(self, code, typ: SeType, start, end, freq: SeFreq):
        repo_name = typ.repo_name(freq)
        df = self.fetcher.fetch_bars(typ, code, start, end, freq)
        if df is None or len(df) < 1:
            self.failed_codes.append(code)
        else:
            df = self.remove_columns_for_min_data(df, freq)
            DB_CLIENT.replace_data_with_its_date_range(df, repo_name)

    def get_failed_codes(self):
        return self.failed_codes

    def update_bars_by_dates(self,  start, end, typ: SeType=SeType.Stock, freq: SeFreq = SeFreq.DAY, codes=None):
        DB_CLIENT.create_index(typ.repo_name(freq), freq.keys_to_index())
        if codes is None:
            codes = self.read_security_codes(typ)
        executor.execute_tasks(codes, self.do_fetch_and_replace_by_dates, typ, start, end, freq)
        print("These codes failed to update: [" + ','.join(self.failed_codes) + "]")

    def update_bars(self, typ: SeType=SeType.Stock, freq: SeFreq = SeFreq.DAY, codes=None, days_to_update=DAYS_TO_UPDATE):
        DB_CLIENT.create_index(typ.repo_name(freq), freq.keys_to_index())
        if codes is None:
            codes = self.read_security_codes(typ)
        end = date_time_utils.Date()
        start = end.get_before(days_to_update)
        executor.execute_tasks(codes, self.do_fetch_and_save_bars, typ, start.as_str(), end.as_str(), freq)
        print("These codes failed to update: [" + ','.join(self.failed_codes) + "]")

    def refetch_and_save_bars(self, typ: SeType, freq: SeFreq = SeFreq.DAY, codes=None):
        if codes is None:
            if self.__confirm_drop():
                self.drop_bar_repo(typ, freq)
                print('删除完成。')
                codes = self.read_security_codes(typ)
            else:
                print("操作已取消。")
                return
        else:
            DB_CLIENT.remove_all_from(typ.repo_name(freq), query={'code': {'$in': codes}})
        DB_CLIENT.create_index(typ.repo_name(freq), freq.keys_to_index())
        end = date_time_utils.Date()
        start = date_time_utils.Date.from_str(MIN_START_TIME)
        executor.execute_tasks(codes, self.do_fetch_and_save_bars, typ, start.as_str(), end.as_str(), freq)

    def drop_bar_repo(self, typ: SeType, freq: SeFreq):
        DB_CLIENT.drop(typ.repo_name(freq))

    def __confirm_drop(self):
        confirm = input("确认删除所有的数据？请确认[yes/no]:")
        return confirm == 'yes'

    def do_update_xdxr(self, code):
        df = self.fetcher.fetch_xdxr(code)
        if len(df) > 0:
            DB_CLIENT.update_data_append_newer(df, REPO_XDXR)

    def refetch_and_save_xdxr(self, codes: list):
        executor = CodeListTaskExecutor(ThreadPoolExecutor)
        executor.execute(codes, self.do_update_xdxr)

    def update_code_list(self):
        data = self.fetcher.fetch_security_list()
        DB_CLIENT.replace_data_by_codes(data, REPO_SECURITY_LIST)

    def remove_columns_for_min_data(self, df, freq):
        if freq == SeFreq.ONE_MIN or freq == SeFreq.FIVE_MIN:
            df = df.drop(columns=['vol', 'amount', 'open', 'hour', 'minute'], errors='ignore')
        return df


STOCK_DAY_DATA_SERVICE = SecurityDataService()


if __name__ == '__main__':
    from gcandle.data.tdx.TdxSecurityFetcher import TdxSecurityFetcher
    service = SecurityDataService(TdxSecurityFetcher())
    # service.update_code_list()
    # service.update_xdxr(codes)
    # service.refetch_and_save_bars(typ=SeType.Stock, freq=SeFreq.FIVE_MIN)
    service.refetch_and_save_bars(typ=SeType.Stock, freq=SeFreq.THIRTY_MIN)
    # service.do_fetch_and_save_bars('000001', SeType.Stock, '2020-08-01', '2020-09-30', SeFreq.FIVE_MIN)

