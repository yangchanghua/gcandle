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
        self._trade_candidates_scanner: TradeScanner = None
        self._main_factor_updater = MasterIndicatorService(self._name, self._main_factor.calculate_function)
        self._auto_save = False

    def load(self, name):
        self.set_name(name)

    def save(self):
        pass

    def setAutoSave(self, autoSave: bool):
        self._auto_save = autoSave

    def backtest(self):
        self._backtester.run()

    def scanTradeCandidates(self, day: Date):
        self._trade_candidates_scanner.scan(day)

    def setName(self, name):
        self._name = name
        return self
    
    def setBuyStrategy(self, buyStrategy):
        self._buy_strategy = buyStrategy
        return self
    
    def setSellStrategy(self, sellStrategy: SellStrategy):
        self._sell_strategy = sellStrategy
        return self
    
    def setBacktester(self, backtester: Backtester):
        self._backtester = backtester
        return self
    
    def setTradeCandidatesScanner(self, tradeCandidatesScanner: TradeScanner):
        self._trade_candidates_scanner = tradeCandidatesScanner
        return self

    def setMainFactor(self, factor: Factor):
        self._main_factor = factor

    def addSecondaryFactor(self, factor: Factor):
        self._factors.append(factor)

    def initMainFactorCandidates(self, codes=None):
        if codes is None:
            self._main_factor_updater.recreate_for_all_codes()
        else:
            print("Not supported")

    def updateMainFactorCandidates(self, fromDate: Date=None, codes=None):
        if codes is None:
            self._main_factor_updater.update_for_all_codes()
        else:
            print("Not supported")

    def getCandidates(self, day: Date):
        return None