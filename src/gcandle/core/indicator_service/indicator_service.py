from gcandle.core.indicator_service.indicator_db_service import IndicatorDBService
from gcandle.objects.basic_objects import DB_CLIENT
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE
from gcandle.utils.date_time_utils import Date
from gcandle.data.se_types import SeFreq, SeType
import gcandle.utils.multi_process_pool as executor


INDEPENDENT_INDICATOR_REPO_PREFIX = 'indicator_'
DEFAULT_MIN_START_DATE = '2006-01-01'
DEFAULT_DAYS_TO_UPDATE = 15


class IndicatorService:
    def __init__(self, name: str, indicator_func, freq: SeFreq, days_to_update: int = DEFAULT_DAYS_TO_UPDATE):
        self.name = name
        self.indicator_func = indicator_func
        self.freq = freq
        self.days_to_update = days_to_update
        self.db_service = IndicatorDBService(name, freq)

    #to be overridden by subclass
    def pre_save(self, data):
        return data

    def update_for_single_code(self, code, start, end):
        data = self.read_data_and_calc_indicators(code, start, end)
        self.db_service.save(data)

    def do_update_by_dates_for_code(self, code, start, end):
        data = self.read_data_and_calc_indicators(code, start, end)
        self.db_service.replace_by_dates(data, code, start, end)

    def read_data_and_calc_indicators(self, code, start, end):
        if self.freq.is_minutes():
            data = SECURITY_DATA_READ_SERVICE.read_security_data_for_single(code, start, end, freq=self.freq, typ=SeType.Stock)
        else:
            data = SECURITY_DATA_READ_SERVICE.read_qfq_security_data_for_single(code, start, end)
        if data is None or data.shape[0] < 10:
            return

        data = self.indicator_func(data)
        if data is None or len(data) < 1:
            return None
        else:
            return self.pre_save(data)

    def update_all_codes(self, codes):
        end = Date()
        start = end.get_before(self.days_to_update)
        executor.execute_tasks(
            codes,
            self.update_for_single_code,
            start.as_str(),
            end.as_str()
        )

    def update_by_dates(self, codes, start, end):
        executor.execute_tasks(
            codes,
            self.do_update_by_dates_for_code,
            start,
            end
        )

    def recreate_all_codes(self, codes, create_start_date=DEFAULT_MIN_START_DATE):
        if codes is None:
            if self.db_service.confirm_and_drop():
                codes = SECURITY_DATA_READ_SERVICE.read_security_codes()
            else:
                return
        else:
            self.db_service.remove_all(codes)

        end = Date()
        start = Date.from_str(create_start_date)
        executor.execute_tasks(
            codes,
            self.update_for_single_code,
            start.as_str(),
            end.as_str()
        )

    def read_by_dates(
            self,
            codes,
            start='all',
            end=None
    ):
        return self.db_service.read_by_dates(codes, start, end)

    def read_by_data(self, data):
        return self.db_service.read_by_data(data)

    def read_all_codes(self):
        return self.db_service.read_all_codes()

    def __confirm_drop(self):
        confirm = input("确认删除所有的数据？请确认[yes/no]:")
        return confirm == 'yes'

