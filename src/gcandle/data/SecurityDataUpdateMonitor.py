from gcandle.objects.basic_objects import DB_CLIENT
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE
from gcandle.utils.date_time_utils import Date

START_DATE = '2001-01-01'
COLLECTIONS_TO_CHECK = [
    'stock_day',
    'index_day',
    'stock_day_qfq',
    'security_list',
    'security_xdxr',
    'xian_backtest',
    'xian_dummy_for_trade',
    'da_lian_yang_backtest',
    'da_lian_yang_dummy_for_trade',
    'indicator_fp_marks_20_day',
    'indicator_fp_marks_50_day',
    'indicator_fp_marks_120_day',
    'fp_features_20_xian',
    'fp_features_50_xian',
    'fp_features_120_xian',
    'fp_features_20_da_lian_yang',
    'fp_features_50_da_lian_yang',
    'fp_features_120_da_lian_yang',
]


class SecurityDataUpdateMonitor:
    def __init__(self):
        self.repos_to_monitor = []
        self.baseline = {}
        self.info_after = {}

    def add_monitor_repo(self, repo_name):
        self.repos_to_monitor.append(repo_name)

    def add_monitor_repo_list(self, lst):
        self.repos_to_monitor.extend(lst)

    def collect_baseline(self):
        for repo_name in self.repos_to_monitor:
            cnt = DB_CLIENT.count_of(repo_name)
            self.baseline[repo_name] = cnt

    def collect_info_after_update(self, after: Date):
        query_before = {'update_time': {'$lte': after.as_str_with_time()}}
        query_after = {'update_time': {'$gt': after.as_str_with_time()}}
        info_dict = self.info_after
        for repo_name in self.repos_to_monitor:
            cnt_before = DB_CLIENT.count_of(repo_name, query=query_before)
            cnt_after = DB_CLIENT.count_of(repo_name, query=query_after)
            info_dict[repo_name] = {
                'cnt_before': cnt_before,
                'cnt_after': cnt_after,
            }

    def report_changes(self):
        if len(self.baseline) < 1 or len(self.info_after) < 1:
            print("Invalid request, no baseline or info after change collected")
        for k, v in self.baseline.items():
            cnt_changed = self.info_after[k]
            print('## Changes of {} ##'.format(k))
            if cnt_changed is None or len(cnt_changed) < 1:
                print('No info after collected for {}'.format(k))
                continue
            if v != cnt_changed['cnt_before']:
                print('Warning: base cnt changed: {} -> {}'.format(v, cnt_changed['cnt_before']))
            print('updated cnt: {}'.format(cnt_changed['cnt_after']))


if __name__ == '__main__':
    dataService = SECURITY_DATA_READ_SERVICE
    verifier = SecurityDataUpdateMonitor()
    verifier.collect_baseline()
    verifier.collect_info_after_update(after=Date())
    verifier.report_changes()

