from enum import Enum


class PositionDir(Enum):
    OPEN = 1,
    CLOSE = 2


class SeTrade:
    def __init__(self, dir: PositionDir, date: str, price: float, amount: int, low: float, high: float):
        self.dir = dir
        self.date = date
        self.price = price
        self.amount = amount
        self.low = low
        self.high = high


class SePosition:

    def __init__(self, code):
        self.open_date = None
        self.clear_date = None
        self.today = None
        self.code = code
        self.amount = 0
        self.se_value = 0
        self.days = 0
        self.cleared = False
        self.trade_history = [] # SeTrade

    def get_amount(self):
        return self.amount

    def update_status(self, date, item):
        self.increase_day()
        self.today = date
        if item is not None and len(item) > 0:
            item = item.squeeze()
            self.se_value = self.amount * item.close

    def get_snapshot(self):
        return [self.today, self.code, self.amount, self.se_value, self.days]

    def profit(self):
        if len(self.trade_history) < 1:
            return 0
        total_pay = 0
        total_got = 0
        for r in self.trade_history:
            if r.dir == PositionDir.OPEN:
                total_pay += r.price * r.amount
            else:
                total_got += r.price * r.amount
        return (total_got - total_pay) / total_pay

    def to_dict(self):
        return {
            open: self.open_date
        }

    def get_trade_history(self):
        return self.trade_history

    def get_days(self):
        return self.days

    def increase_day(self, days=1):
        self.days += days
        return self

    def open(self, price, amount, data):
        assert amount > 0 and price > 0, "Invalide parameter for creating position. price={} amount={}".format(price, amount)
        assert self.cleared == False, "Cannot open for cleared"
        assert data.code == self.code, "Invalid parameter, code don't match {} {}".format(self.code, data.code)
        self.trade_history.append(SeTrade(
            PositionDir.OPEN,
            data.date,
            price,
            amount,
            data.low,
            data.high
        ))
        self.amount += amount
        if self.open_date is None:
            self.open_date = data.date
        return self

    def close(self, price, amount, data):
        if data.low > price or price > data.high:
            print("Warning: close price out of bound of data {}-{} ".format(data.low, data.high))
        assert amount > 0, "Invalid parameter of amount {}".format(amount)
        assert self.amount >= amount, "Position is less than amount to close"
        self.trade_history.append(SeTrade(
            PositionDir.CLOSE,
            data.date,
            price,
            self.amount,
            data.low,
            data.high
        ))
        self.amount -= amount
        if self.amount == 0:
            self.cleared = True
            self.clear_date = data.date
        return self

    def close_all(self, price, data):
        return self.close(price, self.amount, data)

