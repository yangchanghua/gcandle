from gcandle.core.sell_strategy.AbstractSellStrategy import AbstractSellStrategy


class SellAtOpenStrategy(AbstractSellStrategy):
    def __init__(self):
        super().__init__('SellAtOpen')
        self.add_exclude_filter(
            lambda data : data.open / data.pc >= 1.099
        )

    #override
    def calc_prices(self, data):
        my_prices = data.open.copy()
        return my_prices

