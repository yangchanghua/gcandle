from gcandle.data.SecurityDataUpdateMonitor import SecurityDataUpdateMonitor
from gcandle.indicator.basic_indicators import MACD
from gcandle.core.indicator_service.indicator_service import IndicatorService
from gcandle.data.se_types import SeFreq
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE
from gcandle.utils.date_time_utils import Date


def macd_calc(data):
    CLOSE = data.close
    df = MACD(CLOSE, 12, 26, 9)
    return df.assign(date=data.date)


DAY_MACD_SERVICE = IndicatorService('stock_day_macd', macd_calc, SeFreq.DAY)
THIRTY_MIN_MACD_SERVICE = IndicatorService('stock_30min_macd', macd_calc, SeFreq.THIRTY_MIN)
FIVE_MIN_MACD_SERVICE = IndicatorService('stock_5min_macd', macd_calc, SeFreq.FIVE_MIN)
ONE_MIN_MACD_SERVICE = IndicatorService('stock_1min_macd', macd_calc, SeFreq.ONE_MIN)


def update_day_macd():
    # codes = SECURITY_DATA_READ_SERVICE.read_security_codes()
    codes = ['000001']
    DAY_MACD_SERVICE.update_all_codes(codes)


def update_30min_macd():
    codes = ['000001']
    THIRTY_MIN_MACD_SERVICE.recreate_all_codes(codes, create_start_date='2018-01-01')
    THIRTY_MIN_MACD_SERVICE.update_all_codes(codes)


def update_5min_macd():
    codes = ['000001']
    FIVE_MIN_MACD_SERVICE.recreate_all_codes(codes, create_start_date='2018-01-01')
    FIVE_MIN_MACD_SERVICE.update_all_codes(codes)


def update_1min_macd():
    codes = ['000001']
    ONE_MIN_MACD_SERVICE.recreate_all_codes(codes, create_start_date='2018-01-01')


def update_all_min_macd():
    monitor = SecurityDataUpdateMonitor()
    a = Date()
    monitor.add_monitor_repo("indicator_stock_day_macd")
    monitor.add_monitor_repo("indicator_stock_1min_macd")
    monitor.add_monitor_repo("indicator_stock_5min_macd")
    monitor.add_monitor_repo("indicator_stock_30min_macd")
    monitor.collect_baseline()
    update_1min_macd()
    update_5min_macd()
    update_30min_macd()
    update_day_macd()
    monitor.collect_info_after_update(a)
    b = Date()
    print("time used: {} seconds".format(b.delta_to(a).seconds))
    monitor.report_changes()

if __name__ == '__main__':
    update_all_min_macd()
