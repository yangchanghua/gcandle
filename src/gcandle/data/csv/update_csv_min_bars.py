from gcandle.data.se_types import SeType, SeFreq
from gcandle.objects.data_service_objects import CSV_SECURITY_DATA_SERVICE, SECURITY_DATA_READ_SERVICE
from gcandle.data.SecurityDataUpdateMonitor import SecurityDataUpdateMonitor
from gcandle.utils.date_time_utils import Date
import time


def fetch_and_update_min_bars_by_csv():
    dataService = CSV_SECURITY_DATA_SERVICE
    monitor = SecurityDataUpdateMonitor()
    codes = SECURITY_DATA_READ_SERVICE.read_security_codes()
    # codes = ['000001']
    a = Date()
    start = '2018-01-01'
    end = '2018-12-31'
    # monitor.add_monitor_repo("stock_1min")
    # monitor.add_monitor_repo("stock_5min")
    # monitor.add_monitor_repo("stock_30min")
    # monitor.collect_baseline()
    # codes = ['000001', '300343'] + codes[:100]
    dataService.update_bars_by_dates(start, end, typ=SeType.Stock, freq=SeFreq.ONE_MIN, codes=codes)
    # dataService.refetch_and_save_bars(typ=SeType.Stock, freq=SeFreq.FIVE_MIN, codes=codes)
    # dataService.update_bars(typ=SeType.Stock, freq=SeFreq.THIRTY_MIN, codes=None)
    # monitor.collect_info_after_update(a)
    b = Date()
    print("time used: {} seconds".format(b.delta_to(a).seconds))
    # monitor.report_changes()


if __name__ == '__main__':
    fetch_and_update_min_bars_by_csv()