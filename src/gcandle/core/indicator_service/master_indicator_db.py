import gcandle.utils.dummy_appender as dummy_appender
import gcandle.utils.date_time_utils as date_time_utils
import gcandle.utils.multi_process_pool as executor
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE
from gcandle.objects.basic_objects import DB_CLIENT


class MasterIndicatorDB:
    MAX_CALENDAR_DAYS_TO_KEEP = 15
    DEFAULT_DUMMY_ROWS = 10
    BACKTEST_POSTFIX = '_backtest'
    TRADE_POSTFIX = '_dummy_for_trade'

    def __init__(self, name, indicator_func=None, dummy_rows=DEFAULT_DUMMY_ROWS):
        self.name = name
        self.indicator_func = indicator_func
        self.dummy_rows = dummy_rows

    def set_dummy_rows(self, n):
        self.dummy_rows = n

    def set_indicator_func(self, func):
        self.indicator_func = func

    def repo_name_for_backtest(self):
        return self.name + MasterIndicatorDB.BACKTEST_POSTFIX

    def repo_name_for_trade(self):
        return self.name + MasterIndicatorDB.TRADE_POSTFIX

    def read_train_for_backtest(self, codes, start, end):
        return DB_CLIENT.read_data_by_codes_and_date(
            self.repo_name_for_backtest(),
            codes, start, end=end, extra_filter={'train': True}
        )

    def read_test_for_backtest(self, codes, start, end):
        return DB_CLIENT.read_data_by_codes_and_date(
            self.repo_name_for_backtest(),
            codes, start, end=end, extra_filter={'train': False}
        )

    def read_for_backtest(self, codes, start, end):
        return DB_CLIENT.read_data_by_codes_and_date(
            self.repo_name_for_backtest(),
            codes, start, end=end
        )

    def read_for_trade(self):
        return DB_CLIENT.read_all_data(
            self.repo_name_for_trade()
        )

    def read_all_codes(self):
        return DB_CLIENT.read_codes_from_db(self.repo_name_for_backtest())

    def update_for_single_code(self, code, start, end):
        data = SECURITY_DATA_READ_SERVICE.read_qfq_security_data_for_single(code, start, end)
        if data is not None and len(data) > 1:
            data = dummy_appender.append_dummy_rows(data, 1)
            data = self.indicator_func(data)
            if data is None or data.shape[0] < 1:
                pass
            else:
                df_without_dummy = dummy_appender.exclude_dummy_rows(data)
                DB_CLIENT.update_data_append_newer(df_without_dummy, self.repo_name_for_backtest())

                df_with_dummy_for_trade = data.tail(self.dummy_rows)
                min_start_date = date_time_utils.Date.from_str(end). \
                    get_before(days=MasterIndicatorDB.MAX_CALENDAR_DAYS_TO_KEEP)
                df_with_dummy_for_trade = df_with_dummy_for_trade.reset_index(level=0)
                df_with_dummy_for_trade = df_with_dummy_for_trade.loc[df_with_dummy_for_trade.date > min_start_date.as_str()]
                DB_CLIENT.replace_data_by_codes(df_with_dummy_for_trade, self.repo_name_for_trade())

    def update_all_codes(self, days_to_update=60):
        code_list = SECURITY_DATA_READ_SERVICE.read_security_codes()
        end = date_time_utils.Date()
        start = end.get_before(days=int(days_to_update * 1.5))
        executor.execute_tasks(code_list, self.update_for_single_code, start.as_str(), end.as_str())

    def recreate_all_codes(self, start_date, end):
        DB_CLIENT.remove_all_from(self.repo_name_for_backtest())
        DB_CLIENT.remove_all_from(self.repo_name_for_trade())
        DB_CLIENT.create_index_for_collection_with_date_and_code(self.repo_name_for_backtest())
        DB_CLIENT.create_index_for_collection_with_date_and_code(self.repo_name_for_trade())

        code_list = SECURITY_DATA_READ_SERVICE.read_security_codes()
        start = date_time_utils.Date.from_str(start_date)
        end = date_time_utils.Date.from_str(end)
        executor.execute_tasks(code_list, self.update_for_single_code, start.as_str(), end.as_str())

