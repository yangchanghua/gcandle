import pandas as pd
import numpy as np

SPR_KEY = 'spr'
SELL_STRATEGY_KEY = 'sell_strategy'

class AbstractSellStrategy:

    def __init__(self, name):
        self.name = name
        self.hold_range = [0, 10000]
        self.exclude_filters = []
        self.extra_filters = []

    def get_name(self):
        return self.name

    def get_price_and_name(self, prices):
        res = pd.DataFrame(data={SPR_KEY: prices, SELL_STRATEGY_KEY: [self.name] * len(prices)},
                            index=prices.index
                            )
        if len(res.loc[res[SPR_KEY] <= 0]) > 0:
            res.loc[res[SPR_KEY] <= 0, SELL_STRATEGY_KEY] = ""
        return res

    def set_hold_range(self, left, right=10000):
        self.hold_range = [left, right]
        self.add_extra_filter(
            lambda x : (left <= x.hold_days) & (x.hold_days <= right)
        )
        return self

    def clear_exclude_rules(self):
        self.exclude_filters.clear()
        return self

    def clear_extra_rules(self):
        self.extra_filters.clear()
        return self

    def add_exclude_filter(self, f):
        self.exclude_filters.append(f)
        return self

    def add_extra_filter(self, f):
        self.extra_filters.append(f)
        return self

    def get_prices(self, data) -> pd.DataFrame:
        prices = self.calc_prices(data)
        matched = pd.Series(data=[True] * len(prices), index=prices.index)
        for f in self.exclude_filters:
            excluded = f(data)
            matched &= ~excluded
        for f in self.extra_filters:
            matched &= f(data)

        if len(prices.loc[~matched]) > 0:
            prices.loc[~matched] = 0
        return self.get_price_and_name(prices)

    def calc_prices(self, data) -> pd.Series:
        #not implemented. should return series with same index of data
        pass
