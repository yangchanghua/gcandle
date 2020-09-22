from gcandle.data.se_types import SeType, SeFreq
from gcandle.objects.data_service_objects import TDX_SECURITY_DATA_UPDATE_SERVICE
from gcandle.data.SecurityDataUpdateMonitor import SecurityDataUpdateMonitor
from gcandle.utils.date_time_utils import Date
import time


def fetch_and_update_by_tdx():
    dataService = TDX_SECURITY_DATA_UPDATE_SERVICE
    monitor = SecurityDataUpdateMonitor()
    a = Date()
    monitor.add_monitor_repo("security_list")
    monitor.add_monitor_repo("index_day")
    monitor.add_monitor_repo("stock_day")
    monitor.add_monitor_repo("stock_5min")
    monitor.add_monitor_repo("stock_30min")
    monitor.add_monitor_repo("security_xdxr")
    monitor.collect_baseline()
    dataService.update_code_list()
    dataService.update_bars(typ=SeType.Index, freq=SeFreq.DAY)
    dataService.update_bars(typ=SeType.Stock, freq=SeFreq.DAY)
    codes = dataService.read_security_codes(typ=SeType.Stock)
    dataService.refetch_and_save_xdxr(codes)
    monitor.collect_info_after_update(a)
    b = Date()
    print("time used: {} seconds".format(b.delta_to(a).seconds))
    monitor.report_changes()


if __name__ == '__main__':
    fetch_and_update_by_tdx()