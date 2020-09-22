from gcandle.data.se_types import SeType, SeFreq
from gcandle.objects.data_service_objects import TDX_SECURITY_DATA_UPDATE_SERVICE
from gcandle.data.SecurityDataUpdateMonitor import SecurityDataUpdateMonitor
from gcandle.utils.date_time_utils import Date
import time


def fetch_and_update_min_bars_by_tdx():
    dataService = TDX_SECURITY_DATA_UPDATE_SERVICE
    monitor = SecurityDataUpdateMonitor()
    codes = TDX_SECURITY_DATA_UPDATE_SERVICE.read_security_codes()
    a = Date()
    # monitor.add_monitor_repo("stock_1min")
    # monitor.add_monitor_repo("stock_5min")
    # monitor.add_monitor_repo("stock_30min")
    # monitor.collect_baseline()
    # codes = ['000001', '300343'] + codes[:100]
    dataService.update_bars(typ=SeType.Stock, freq=SeFreq.ONE_MIN, codes=codes)
    dataService.update_bars(typ=SeType.Stock, freq=SeFreq.FIVE_MIN, codes=codes)
    dataService.update_bars(typ=SeType.Stock, freq=SeFreq.THIRTY_MIN, codes=None)
    # monitor.collect_info_after_update(a)
    b = Date()
    print("time used: {} seconds".format(b.delta_to(a).seconds))
    # monitor.report_changes()


if __name__ == '__main__':
    fetch_and_update_min_bars_by_tdx()