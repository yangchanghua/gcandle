import copy
import datetime

import numpy as np
import pandas as pd

from gcandle.core.common_types import ORDER_DIRECTION
from gcandle.core.backtest.AccountPositionStock import AccountPositionStock


def up_limit_ultimate(data):
    return (data.open > data.pc) and (data.open == data.close == data.high == data.low)


def down_limit_ultimate(data):
    return (data.open < data.pc) and (data.open == data.close == data.high == data.low)


class BacktestAccount:
    def __init__(
            self,
            init_cash=1000000,
    ):
        self._history_headers = [
            'datetime',  # 日期/时间
            'code',  # 品种
            'price',  # 成交价
            'amount',  # 成交数量(股票 股数  期货 手数)
            'cash',  # 现金
            'commission',  # 手续费
            'tax',  # 税
            'direction'  # 方向
        ]
        self.init_cash = init_cash
        self.init_hold = {}
        self.current_hold = {}
        self.history = []
        self.hold = {}
        self.fee_ratio = 0.00015
        self.tax_ratio = 0.001
        self._cash_table = []
        self.cash_available = self.init_cash
        self.sell_available = copy.deepcopy(self.init_hold)
        self.buy_available = copy.deepcopy(self.init_hold)
        self.history = []
        self.static_balance = {
            'static_assets': [],
            'cash': [],
            'frozen': [],
            'hold': [],
            'date': []
        }
        self.today_trade = {'last': [], 'current': []}
        self.today_orders = {'last': [], 'current': []}
        self.positions = AccountPositionStock()

    def __repr__(self):
        return 'account'

    @property
    def message(self):
        'the standard message which can be transfer'
        return {
            'source': 'account',
            'init_cash': self.init_cash,
            'init_hold': self.init_hold,
            'cash': self.cash_available,
            'history': self.history,
        }

    @property
    def start_date(self):
        return self._cash_table[0][0]

    @property
    def end_date(self):
        return self._cash_table[-1][0]

    def start(self, today):
        self._cash_table.append([today, self.init_cash])

    def take_snapshot(self, today):
        self._cash_table.append([today, self.cash_available])

    def settle(self, data):
        self.sell_available = copy.deepcopy(self.hold)
        self.cash_available = self.cash_available
        date = data.date[0]
        self.take_snapshot(date)
        self.positions.close_trade(data)

    def get_daily_assets(self):
        daily_pos = self.positions.get_daily_positions()
        daily_pos = np.array(daily_pos).T
        dp = pd.DataFrame(data={'date': daily_pos[0], 'se_value': daily_pos[3]})
        daily_se_value = dp.groupby(['date'], as_index=True).agg({'se_value': sum})
        daily_cash = self.daily_cash
        daily_assets = pd.concat([daily_se_value, daily_cash], join='outer', axis=1)
        daily_assets.se_value.fillna(0, inplace=True)
        daily_assets.cash.fillna(method='ffill', inplace=True)
        daily_assets['asset'] = daily_assets['se_value'] + daily_assets['cash']
        daily_assets = daily_assets.round(0)
        return daily_assets

    def get_active_position_days(self, code):
        return self.positions.get_active_position_days(code)

    def get_all_positions(self):
        return self.positions.get_all_positions()

    def win_rate(self):
        return self.positions.win_rate()

    def deal(self, data, price, amount, direction, money, fee, tax=0):
        cash_after_deal = self.cash_available - money * direction
        if cash_after_deal < 0:
            print('Invalid deal!!!! Cash less than 0')
            return
        today = data.date
        self.history.append(
            [
                str(today),
                data.code,
                price,
                amount*direction,
                self.cash_available,
                fee,
                tax,
                direction
            ]
        )
        self.cash_available = cash_after_deal
        hold = self.hold
        if data.code in hold:
            orig_hold = hold[data.code]
            orig_hold['amount'] += amount * direction
            if orig_hold['amount'] <= 0:
                # print('{} sold out.'.format(data.code))
                del hold[data.code]
        else:
            hold[data.code] = {'date': today, 'amount': amount, 'price': price}

    def validate_amount(self, amount, data, buy=True):
        # 1手100股
        max_ratio = 0.05 * 100 * 1000
        if amount > (data.vol * max_ratio):
            print('{} {} order too much, fulfill ratio = {}'.format(
                str(data.date)[0:10], data.code, round(data.vol * max_ratio / amount, 2)))
            amount = int(data.vol * max_ratio)
        if buy:
            amount = amount - amount % 100
        return amount

    def validate_price(self, order_price, data):
        return data.high >= order_price >= data.low

    def exclude_yi_zi_ban_up(self, data):
        if 'pc' in data.columns:
            f = data.close / data.pc > 1
            f &= (data.low == data.high)
            return data.loc[~f]
        else:
            return data

    def batch_buy(self, data, price_func, max_ratio=1.0):
        cash_today = self.cash_available
        data = self.exclude_yi_zi_ban_up(data)

        ratio = round(1.0/len(data), 2) - 0.01

        if ratio > max_ratio:
            ratio = max_ratio
        for row in data.itertuples():
            price = price_func(row)
            amount = (cash_today * ratio) / (price * (1+self.fee_ratio + self.tax_ratio))
            self.order(row, ORDER_DIRECTION.BUY, amount=amount, price=price)

    def order(self, data, towards, ratio=0, amount=0, price=0, price_func=None):
        cash = self.cash_available
        if price is None:
            if price_func is None:
                print("Invalid parameters, both price and price_func are None!")
                return False
            else:
                price = price_func(data)
        order_dir = ''
        if towards == ORDER_DIRECTION.BUY:
            order_dir = 'BUY'
        else:
            order_dir = 'SELL'
        #for debug
        # print('{} {} {}  - {}'.format(data.date, order_dir, data.code, cash))
        if not self.validate_price(price, data):
            print(order_dir + ' Order price {} out of range [{}, {}]'.format(price, data.low, data.high))
            return False
        if towards == ORDER_DIRECTION.BUY:
            if up_limit_ultimate(data):
                print(data.date, data.code, '涨停一字，无法买入')
                return False
            # price = (data.low + data.high) * 0.5
            # print('Buy {} {} '.format(data.code, price))
            if amount == 0:
                if ratio == 0:
                    print("Invalid parameters, both amount and ratio are 0.")
                    return False
                else:
                    to_pay = min(cash * ratio, cash)
                    amount = to_pay / price * (1 + self.fee_ratio + self.tax_ratio)
            amount = self.validate_amount(amount, data)
            if amount <= 0:
                # print('Failed to deal due to cash or vol')
                return False
            fee = self.fee_ratio * price * amount
            fee = max(fee, 5)
            tax = 0
            to_pay = amount * price + fee
            if cash < to_pay:
                print('Not enough money or invalid money calc', data.date, data.code, price, amount)
                return False
            self.positions.open_position(price, amount, data)
            self.deal(data=data, price=price, amount=amount, direction=1, money=to_pay, fee=fee, tax=tax)
        else:
            if down_limit_ultimate(data):
                print(data.date, data.code, '跌停一字，无法卖出。')
                #价格已检验过，此处只是假设再跌20%。
                # price = price * 0.8
                return False
            if data.code not in self.hold:
                print('Invalid order, no sellable stocks for ', data.code)
                return False
            sellable = self.hold[data.code]
            amount = sellable['amount'] * ratio
            amount = self.validate_amount(amount, data)
            if amount <= 0:
                # print('Failed to deal due to vol')
                return False
            # print('Sell {} {} '.format(data.code, price))
            fee = price * amount * self.fee_ratio
            tax = price * amount * self.tax_ratio
            money = price * amount - fee - tax
            # print('sell {} deal OK'.format(data.code))
            self.positions.close_position(price, data)
            self.deal(data=data, price=price, amount=amount, direction=-1, money=money, fee=fee, tax=tax)
        return True

    @property
    def history_table(self):
        if len(self.history) > 0:
            lens = len(self.history[0])
        else:
            lens = len(self._history_headers)

        return pd.DataFrame(
            data=self.history,
            columns=self._history_headers[:lens]
        ).sort_index()

    @property
    def daily_cash(self):
        _cash = pd.DataFrame(
            data=self._cash_table, columns=['date', 'cash']).\
            set_index('date', drop=True)
        return _cash

    @property
    def profit_by_stock(self):
        h = self.history_table

        def _calc_prof_sum(x):
            r = pd.Series({'profit_sum': x.amount.sum() * x.iloc[-1].price - (x['amount'] * x.price).sum()})
            return r

        if h.shape[0] > 0:
            return h.groupby('code').apply(_calc_prof_sum)
        else:
            return None

    @property
    def profit_rate_by_stock(self):
        h = self.history_table

        def _calc_prof_rate(x):
            x.loc[x.direction == -1, 'bprice'] = x.price.shift(1)
            x['order_rate'] = x.price / x.bprice
            x.order_rate.fillna(1, inplace=True)
            x['profit_rate'] = x.order_rate.prod()
            return pd.Series({'profit_rate': x.profit_rate.iloc[-1]})

        if h.shape[0] > 0:
            return h.groupby('code').apply(_calc_prof_rate)
        else:
            return None

    def max_profitable_stocks(self, n):
        profits = self.profit_rate_by_stock
        if profits is not None:
            if n > 0:
                return profits.sort_values(by=['profit_rate']).tail(n)
            else:
                return profits.sort_values(by=['profit_rate']).head(abs(n))
        else:
            return None
