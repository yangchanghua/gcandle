from gcandle.core.sell_strategy.AbstractSellStrategy import (AbstractSellStrategy, SPR_KEY, SELL_STRATEGY_KEY)
import pandas as pd
import numpy as np


class CombinedSellStrategy(AbstractSellStrategy):

    def __init__(self, name):
        super().__init__(name)
        self.strategy_chain = []
        self.sell_history = {}

    def append(self, strategy):
        self.strategy_chain.append(strategy)
        self.sell_history[strategy.get_name()] = 0
        return self

    def add_sold(self, spr):
        counts = spr.groupby(SELL_STRATEGY_KEY).count()
        for row in counts.itertuples():
            self.sell_history[row.Index] += row.spr

    def get_history(self):
        return self.sell_history

    @staticmethod
    def get_empty_price_df(data):
        return pd.DataFrame(
            data={SPR_KEY: np.zeros(len(data)), SELL_STRATEGY_KEY: ["None"] * len(data)},
            index=data.index
        )

    # override
    def get_prices(self, data) -> pd.DataFrame:
        spr = CombinedSellStrategy.get_empty_price_df(data)
        for item in self.strategy_chain:
            if len(spr.loc[spr[SPR_KEY] == 0]) <= 0:
                break
            tmp_prices = item.get_prices(data)
            tmp_prices[SPR_KEY].fillna(0, inplace=True)
            if len(tmp_prices.loc[tmp_prices[SPR_KEY] > 0]) > 0:
                spr.loc[spr[SPR_KEY] == 0, [SPR_KEY, SELL_STRATEGY_KEY]] = tmp_prices[[SPR_KEY, SELL_STRATEGY_KEY]]

        if len(spr.loc[spr[SPR_KEY] > 0]) > 0:
            self.add_sold(spr.loc[spr[SPR_KEY] > 0])
        return spr
