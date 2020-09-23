

# Interface for buy strategy
class BuyStrategy:
    def __init__(self, name):
        self.name = name
        self._buy_filter_func = None
        self._price_func = None

    def update_buy_prices(self, data):
        pass

    # can be used both for candidate scan and backtest.
    def get_buy_filter_before_buy_day(self, data):
        return True

    def get_final_buy_filter_on_buy_day(self, data):
        if self._buy_filter_func is not None:
            return self._buy_filter_func(data)
        else:
            return False

    def calc_and_forward_fill_buy_filter(self, data, extra=True):
        return data

    def get_price_func(self):
        return self._price_func

    def set_filter_on_buy_day(self, filter_func):
        self._buy_filter_func = filter_func
        return self

    def set_price_func(self, price_func):
        self._price_func = price_func
        return self

