import gcandle.utils.date_time_utils as date_time_utils
import gcandle.utils.multi_process_pool as executor
from gcandle.objects.basic_objects import DB_CLIENT
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE


FP_FEATURE_REPO_NAME_PREFIX = 'fp_features_'
fpds = [20, 50, 120]
MIN_CREATE_DATE = '2006-01-01'


class SlaveIndicatorService:
    def __init__(self, master_service=None):
        self.master_service = master_service

    def set_master(self, master):
        self.master_service = master
        return self

    def save(self, data):
        if data is not None and len(data) > 0:
            repo_name = self.repo_name()
            df = data.drop(columns=['open', 'low', 'high', 'amount', 'vol'
                                  ], axis=1)
            DB_CLIENT.update_data_append_newer(df, repo_name)

    def repo_name(self):
        # not implemented. should be provided by concrete slave indicator service
        return None

    def days_to_update(self):
        # not implemented. should be provided by concrete slave indicator service
        return 0

    def compute_indicator(self, data):
        # not implemented. should be provided by concrete slave indicator service
        return None

    def update_for_single_code(self, code, start, end):
        data = SECURITY_DATA_READ_SERVICE.read_qfq_security_data_for_single(code, start, end)
        if data is None or data.shape[0] < 10:
            return

        master_data = self.master_service.read_master([code], start, end)
        if master_data is None or len(master_data) < 1:
            return
        data = self.compute_indicator(data)
        if data is None or len(data) < 1:
            return
        master_key = self.master_service.get_key()
        data[master_key] = master_data[master_key]
        data = data.loc[data[master_key] == True]
        data.drop(columns=[master_key], inplace=True)
        if data is not None and data.shape[0] > 0:
            self.save(data)

    def update_all_codes(self, codes):
        end = date_time_utils.Date()
        start = end.get_before(days=self.days_to_update())
        executor.execute_tasks(
            codes,
            self.update_for_single_code,
            start.as_str(),
            end.as_str()
        )

    def recreate_all_codes(self, codes, create_start_date, end):
        DB_CLIENT.remove_all_from(self.repo_name())
        DB_CLIENT.create_index_for_collection_with_date_and_code(self.repo_name())
        start = date_time_utils.Date.from_str(create_start_date)
        end = date_time_utils.Date.from_str(end)
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
        repo_name = self.repo_name()
        return DB_CLIENT.read_data_by_codes_and_date(repo_name, codes, start, end=end)


if __name__ == '__main__':
    print("Not callable, abstract class: slave indicator service")