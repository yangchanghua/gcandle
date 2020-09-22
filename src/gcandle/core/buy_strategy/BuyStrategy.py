

# Interface for buy strategy
class BuyStrategy:
    def __init__(self, name):
        self.name = name

    def update_buy_prices(self, data):
        pass

    # can be used both for candidate scan and backtest.
    def get_buy_filter_before_buy_day(self, data):
        pass

    def get_final_buy_filter_on_buy_day(self, data):
        pass

    def calc_and_forward_fill_buy_filter(self, data, extra=True):
        pass

    def get_price_func(self):
        pass