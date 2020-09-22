from gcandle.core.sell_strategy.AbstractSellStrategy import AbstractSellStrategy
from gcandle.indicator.basic_indicators import (increase_limit, decrease_limit)


class SellAtCloseStrategy(AbstractSellStrategy):
    def __init__(self, name='SellAtClose'):
        super().__init__(name)
        self.add_exclude_filter(
            increase_limit
        ).add_exclude_filter(
            decrease_limit
        )

    #override
    def calc_prices(self, data):
        my_prices = data.close.copy()
        return my_prices

