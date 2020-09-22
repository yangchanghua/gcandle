from gcandle.core.sell_strategy.AbstractSellStrategy import AbstractSellStrategy
import pandas as pd


class TargetPriceSellStrategy(AbstractSellStrategy):
    def __init__(self):
        super().__init__('TargetPrice')
        self.target_price_func = None
        self.add_exclude_filter(
            lambda x : x.open / x.pc >= 1.099
        )
        self.higher_than_pc = 0
        self.higher_than_bpr = 0

    def set_target_price_func(self, func):
        self.target_price_func = func
        return self

    def set_higher_than_pc(self, rate):
        self.higher_than_pc = rate
        return self

    def set_higher_than_bpr(self, rate):
        self.higher_than_bpr = rate
        return self

    #override
    def calc_prices(self, data):
        my_prices = pd.Series([0] * len(data), index=data.index)
        if self.target_price_func is not None:
            my_prices = data.apply(self.target_price_func, axis=1)
        elif self.higher_than_bpr > 0 and self.higher_than_pc > 0:
            tmp_df = pd.concat([data.pc * self.higher_than_pc, data.bpr * self.higher_than_bpr], axis=1)
            my_prices = tmp_df.min(axis=1)
        elif self.higher_than_bpr > 0:
            my_prices = data.bpr * self.higher_than_bpr
        elif self.higher_than_pc > 0:
            my_prices = data.pc * self.higher_than_pc
        else:
            print("Invalid target prices strategy, no function or higher rate is set")

        target_too_high = (data.high < my_prices)
        my_prices.loc[target_too_high] = 0
        my_prices.loc[(my_prices > 0) & (my_prices < data.open)] = data.open
        return my_prices

