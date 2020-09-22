from gcandle.objects.basic_objects import DB_CLIENT
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE
from gcandle.utils.date_time_utils import Date
from gcandle.data.se_types import SeFreq, SeType
import gcandle.utils.multi_process_pool as executor


INDEPENDENT_INDICATOR_REPO_PREFIX = 'indicator_'
DEFAULT_MIN_START_DATE = '2006-01-01'
DEFAULT_DAYS_TO_UPDATE = 15


class IndicatorDBService:
    def __init__(self, name: str, freq: SeFreq):
        self.name = name
        self.freq = freq

    def replace_by_dates(self, data, code, start, end):
        DB_CLIENT.delete_by_code_and_dates(self.repo_name(), code, start, end)
        if data is not None and len(data) > 0:
            DB_CLIENT.create_index(self.repo_name(), self.freq.keys_to_index())
            repo_name = self.repo_name()
            DB_CLIENT.replace_data_by_date_range(data, repo_name, start, end)

    def save(self, data):
        DB_CLIENT.create_index(self.repo_name(), self.freq.keys_to_index())
        if data is not None and len(data) > 0:
            repo_name = self.repo_name()
            DB_CLIENT.update_data_append_newer(data, repo_name)

    def repo_name(self):
        return INDEPENDENT_INDICATOR_REPO_PREFIX + self.name

    def read_by_dates(
            self,
            codes,
            start='all',
            end=None
    ):
        repo_name = self.repo_name()
        return DB_CLIENT.read_data_by_codes_and_date(repo_name, codes, start, end=end)

    def read_by_data(self, data):
        codes = data.index.levels[1].unique().tolist()
        start = data.date.min()
        end = data.date.max()
        return self.read_by_dates(codes, start, end)

    def read_all_codes(self):
        repo_name = self.repo_name()
        return DB_CLIENT.read_codes_from_db(repo_name)

    def __confirm_drop(self):
        confirm = input("确认删除所有的数据？请确认[yes/no]:")
        return confirm == 'yes'

    def confirm_and_drop(self):
        if self.__confirm_drop():
            DB_CLIENT.drop(self.repo_name())
            print('{} dropped'.format(self.repo_name()))
            return True
        else:
            print("操作已取消。")
            return False

    def remove_all(self, codes):
        DB_CLIENT.remove_all_from(self.repo_name(), query={'code': {'$in': codes}})
