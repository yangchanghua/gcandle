import logging
import time
import numpy as np
import pandas as pd
from gcandle.core.backtest.backtest_accounting import BacktestAccount
from gcandle.core.backtest_analysis import BacktestAnalysor
from gcandle.core.sell_strategy.AbstractSellStrategy import (AbstractSellStrategy, SPR_KEY, SELL_STRATEGY_KEY)
from gcandle.core.sell_strategy.CombinedSellStrategy import CombinedSellStrategy
from gcandle.core.buy_strategy.BuyStrategy import BuyStrategy
from gcandle.core.common_types import ORDER_DIRECTION

DEFAULT_CASH = 100000


class DayBarBacktest:
    def __init__(self, name):
        self.name = name
        self.data = None
        self.init_cash = DEFAULT_CASH
        self.buy_strategy: BuyStrategy = None
        self.sell_strategy: CombinedSellStrategy = None

    def set_data(self, data):
        self.data = data
        return self

    def set_init_cash(self, cash):
        self.init_cash = cash
        return self

    def set_buy_strategy(self, strategy: BuyStrategy):
        self.buy_strategy = strategy
        return self

    def set_sell_strategy(self, strategy: AbstractSellStrategy):
        self.sell_strategy = strategy
        return self

    def init_account(self, init_cash):
        AC = BacktestAccount(init_cash=init_cash)
        self.AC = AC
        self.orders = []

    def choose_cand_without_limit(self, buy_cand):
        max_ratio = 1.0
        chosen_buy = self.choose_cand(buy_cand)
        return chosen_buy, max_ratio

    def choose_cand_with_max_ratio(self, buy_cand, hold_cnt):
        can_buy_cnt = max(1, 3 - hold_cnt)
        if hold_cnt == 0:
            max_ratio = 0.33
        elif hold_cnt == 1:
            max_ratio = 0.5
        else:
            max_ratio = 1
        chosen_buy = self.choose_cand(buy_cand, can_buy_cnt)
        return chosen_buy, max_ratio

    def print_sell_stock(self, item):
        arr = self.get_item_attrs(item)
        print('SELL ', arr)

    def get_item_attrs(self, item):
        arr = []
        arr.append(item.date)
        arr.append(item.code)
        arr.append(item.xian_a)
        arr.append(item.xianBC)
        arr.append(item.pLAXChg)
        arr.append(item.pHAXChg)
        arr.append(item.low)
        arr.append(item.close)

    def output_order_stats(self):
        tt = pd.DataFrame(self.orders, columns=['date', 'code', 'blow', 'bpr', 'spr', 'shigh', 'sdate'])
        tt['profit'] = tt.spr / tt.bpr
        tt['win'] = tt.profit >= 1.03
        tt['loss'] = (tt.win == False) & (tt.profit <= 0.97)
        tt['avg'] = (tt.win == False) & (tt.loss == False)
        date_filter = (tt.date > '2000-10-01') & (tt.date < '2020-12-30')
        profit_filter = tt.profit < 0.9
        df = tt.loc[date_filter & profit_filter]
        win_cnt = len(tt.loc[tt.win])
        loss_cnt = len(tt.loc[tt.loss])
        win_ratio = win_cnt / len(tt)
        loss_ratio = loss_cnt / len(tt)
        print(
            'win, loss cnt {}, {}, ratio: {}, {}'.format(win_cnt, loss_cnt, round(win_ratio, 2), round(loss_ratio, 2)))

    def print_buy_stock(self, item):
        arr = self.get_item_attrs(item)
        print('BUY ', arr)

    def choose_cand(self, cand, max_cnt=30):
        cnt = len(cand)
        chosen = []
        if cnt < 1:
            chosen = []
        elif cnt <= max_cnt:
            chosen = [c for c in cand]
        else:
            n = np.random.randint(cnt - max_cnt)
            chosen = [cand[i] for i in range(n, n + max_cnt)]
        return chosen

    def buy(self, code, item, ratio, price_func):
        self.AC.order(data=item, towards=ORDER_DIRECTION.BUY, ratio=ratio, \
                      price_func=price_func)

    def sell(self, code, item, price):
        self.AC.order(data=item, towards=ORDER_DIRECTION.SELL, price=price, ratio=1.0)

    def get_active_position_days(self, data):
        return self.AC.get_active_position_days(data.code)

    def sell_immediately(self, sell_data):
        data = sell_data.copy()
        today = data['date'][0]
        today = str(today)[0:10]
        # print('sell on {}'.format(today))
        sellable = self.AC.sell_available
        if len(sellable) < 1:
            return
        data = data.loc[data.code.isin(sellable)].copy()
        if len(data) <= 0:
            logging.debug("{} No data for sell, sellable = {}".format(today, sellable))
            return
        hold_days = data.apply(self.get_active_position_days, axis=1)
        data['hold_days'] = hold_days
        spr = self.sell_strategy.get_prices(data)
        cols = [SPR_KEY, SELL_STRATEGY_KEY]
        data[cols] = spr[cols]
        for c in sellable:
            if c not in data['code'].values:  # fixed bug: 股票当日停牌,无法交易
                # print('{} No data for today, maybe paused for trading'.format(c))
                pass
            else:
                item = data.loc[data.code == c].squeeze()
                price = item[SPR_KEY]
                name = item[SELL_STRATEGY_KEY]
                if price > 0:
                    self.sell(item.code, item, price=price)
                else:
                    pass

    def onbar(self, data):
        data = data.reset_index().copy()
        today = str(data.date[0])[0:10]
        if today == '2016-11-25':
            print(today)
        AC = self.AC

        f = self.buy_strategy.get_final_buy_filter_on_buy_day(data)
        f &= ~data.code.isin(self.AC.sell_available)
        buy_data = data.loc[f]

        if self.sell_first:
            self.sell_immediately(data)

        if len(buy_data) > 0:
            buy_cand_codes = buy_data.code.unique().tolist()
            chosen_buy_codes, max_ratio = self.choose_cand_without_limit(buy_cand_codes)
            buy_data = buy_data.loc[buy_data.code.isin(chosen_buy_codes)].copy()

            price_func = self.buy_strategy.get_price_func()
            self.AC.batch_buy(buy_data, price_func, max_ratio=max_ratio)
            for c in chosen_buy_codes:
                item = buy_data.loc[buy_data['code'] == c].squeeze()

        if not self.sell_first:
            self.sell_immediately(data)

        AC.settle(data)

    def run(self, y, yy, m=1, mm=12, sell_first=False, extra_filter=False, silent=False):
        if not self._ready_to_run():
            print('Exit now.')
            return
        bg = time.time()
        self.init_account(self.init_cash)
        self.sell_first = sell_first
        self.use_extra_filter = extra_filter

        data = self.data.copy()
        # data = data.loc[datetime(y, m, 1):datetime(yy, mm, 30)]

        data = self.buy_strategy.calc_and_forward_fill_buy_filter(data, extra=self.use_extra_filter)

        self.buy_strategy.update_buy_prices(data)
        tm = time.time()
        print('Time used for calc buy and price: ', round(tm - bg, 2))
        AC = self.AC
        data = data.sort_index(level=0)
        data.groupby(level=0).apply(self.onbar)

        if not silent:
            print('Time used for on bar: ', time.time() - tm)
            print('Win rate: ', AC.win_rate())
            risk = BacktestAnalysor(AC)
            fig = risk.plot_assets_curve()
            fig.show()

        AC.market_data = data
        his_table = AC.history_table

        hist_file = open('{}_history.html'.format(self.name), 'w')
        print(his_table.to_html(), file=hist_file)
        hist_file.close()

        cols = ['datetime', 'code', 'price', 'amount', 'direction']

        if not silent:
            print('### Total orders ###: ', his_table.shape[0] / 2)
            print(self.sell_strategy.get_history())

        top = AC.max_profitable_stocks(n=20)
        bottom = AC.max_profitable_stocks(n=-20)
        f = open('{}_top_profit.html'.format(self.name), 'w')
        print(top.T.to_html(), file=f)
        top_codes = list(top.index.values)
        print('Top profit codes: ', top_codes)
        top_hist = his_table.loc[his_table['code'].isin(top_codes), cols]
        print(top_hist.to_html(), file=f)
        f.close()

        f = open('{}_bottom_profit.html'.format(self.name), 'w')
        print(bottom.to_html(), file=f)
        bottom_codes = list(bottom.index.values)
        print('Bottom profit codes: ', bottom_codes)
        bottom_hist = his_table.loc[his_table['code'].isin(bottom_codes), cols]
        print(bottom_hist.to_html(), file=f)
        f.close()

    def _ready_to_run(self):
        if self.data is None:
            print('No data is set for backtest')
            return False
        if self.buy_strategy is None:
            print('Buy strategy is not set yet.')
            return False
        if self.sell_strategy is None:
            print('Sell strategy is not set yet.')
            return False
        return True
