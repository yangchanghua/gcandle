from gcandle.core.indicator_service.master_indicator_service import MasterIndicatorService
from gcandle.utils.date_time_utils import Date


class BuyStrategy(object):
    pass


class SellStrategy(object):
    pass


class Backtester(object):
    pass


class TradeScanner(object):
    pass


class Factor(object):
    pass


class Strategy:
    def __init__(self):
        self._name = None
        self._main_factor: Factor = None
        self._factors = []
        self._buy_strategy: BuyStrategy = None
        self._sell_strategy: SellStrategy = None
        self._backtester: Backtester = None
        self._trade_scanner: TradeScanner = None
        self._candidates_updater = MasterIndicatorService(self._name, self._main_factor.calculate_function)
        self._auto_save = False

    def load(self, name):
        self.set_name(name)

    def save(self):
        pass

    def set_auto_save(self, auto_save: bool):
        self._auto_save = auto_save

    def run_backtest(self):
        self._backtester.run()

    def scan_trade_candidates(self, day: Date):
        self._trade_scanner.scan(day)

    def set_name(self, name):
        self._name = name

    def set_main_factor(self, factor: Factor):
        self._main_factor = factor

    def add_factor(self, factor: Factor):
        self._factors.append(factor)

    def init_main_factor_candidates(self):
        self._candidates_updater.recreate_for_all_codes()