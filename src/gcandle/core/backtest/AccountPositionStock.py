from gcandle.core.backtest.seposition import SePosition
from collections import namedtuple

PositionKey = namedtuple('Position', ['date', 'code'])


class AccountPositionStock:
    def __init__(self):
        self.history_positions = {}
        self.active_positions = {}
        self.daily_pos_snapshot = []

    def get_active_position_days(self, code):
        assert code in self.active_positions, "Cannot get days for non-existing code {}".format(code)
        return self.active_positions[code].get_days()

    def open_position(self, price, amount, data):
        code = data.code
        if code in self.active_positions:
            self.active_positions[code].open(price, amount, data)
        else:
            self.active_positions[code] = SePosition(code).open(price, amount, data)

    def close_position(self, price, data):
        date = data.date
        code = data.code
        assert code in self.active_positions, "invalid parameter, {} not in active positions".format(code)
        position = self.active_positions[code]
        position.close_all(price, data)
        key = PositionKey(date=data.date, code=data.code)
        self.history_positions[key] = position
        del self.active_positions[code]

    def close_trade(self, data):
        # 收盘: 增加每日持仓的记录, 包含 date, code, close, amount, se_value, days
        date = data.date[0]
        for code, pos in self.active_positions.items():
            item = data[data.code == code]
            pos.update_status(date, item)
            self.daily_pos_snapshot.append(pos.get_snapshot())

    def get_daily_positions(self):
        return self.daily_pos_snapshot

    def get_all_positions(self):
        return self.active_positions, self.history_positions

    def get_won_positions(self):
        return [p for p in self.history_positions.values() if p.profit() > 0]

    def get_lost_positions(self):
        return [p for p in self.history_positions.values() if p.profit() < 0]

    def win_rate(self):
        return round(len(self.get_won_positions()) / len(self.history_positions), 3)
