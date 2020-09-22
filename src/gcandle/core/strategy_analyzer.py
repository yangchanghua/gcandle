from gcandle.core.indicator_service.master_indicator_service import MasterIndicatorService
import pandas as pd

show_cols = ['low', 'high', 'profit']


def fill_future_prices(data):
    cols = ['high', 'close']
    def _fill(data):
        for n in [1, 2]:
            for col in cols:
                data[col + 'A' + str(n)] = data[col].shift(-1 * n)
        return data
    data = data.groupby(level=1).apply(_fill).sort_index(level=0)
    return data


class StrategyAnalyzer:
    BUY_PR_KEY='bpr'
    SELL_PR_KEY='spr'
    PROFIT_KEY='profit'

    def __init__(self, master_service: MasterIndicatorService, start: str, end: str, sample=1.0):
        self.master_service = master_service
        self.sample = sample
        self.data = master_service.read_with_slave_for_backtest(start, end, ratio=sample)

    def fill_master_filter(self, data):
        #to be implemented by concrete analyzer
        return None

    def calc_buy_and_sell_price(self, data):
        #to be implemented by concrete analyzer
        return None

    def get_buy_day_filter(self, data):
        #to be implemented by concrete analyzer
        return None

    def calc_profit(self):
        data = fill_future_prices(self.data)
        data = self.fill_master_filter(data)
        self.calc_buy_and_sell_price(data)
        buy_filter = self.get_buy_day_filter(data)
        data = data.loc[buy_filter]
        data[StrategyAnalyzer.PROFIT_KEY] = \
            data[StrategyAnalyzer.SELL_PR_KEY] / data[StrategyAnalyzer.BUY_PR_KEY]
        self.data = data.copy()
        return self

    def get_stats(self):
        bins = [0, 1, 100]
        n = len(bins)
        data = self.data
        ntotal = len(self.data)
        cols = ['gratio', 'pr0', 'pr1', 'glen', 'gmean', 'gmedian', 'gprod']
        res = []
        for i in range(0, n - 1):
            gf = (data.profit >= bins[i]) & (data.profit < bins[i + 1])
            g = data.loc[gf]
            glen = len(g)
            gratio = round(glen / ntotal, 2)
            gmean = round(g.profit.mean(), 3)
            gmedian = round(g.profit.median(), 3)
            gprod = g.profit.product()
            res.append([gratio, bins[i], bins[i + 1], glen, gmean, gmedian, gprod])
        res.append([1.0, 0, 100, len(data), round(data.profit.mean(), 3),
                    round(data.profit.median(), 3), data.profit.product()])
        return pd.DataFrame(res, columns=cols)

    def show_losses(self, pr=0.94, extra_cols=None):
        data = self.data
        loss = data.loc[data.profit < pr]
        print('len: ', len(loss))
        cols = show_cols.copy()
        if extra_cols is not None:
            cols += extra_cols
        # loss[cols].to_csv('lian_yang_loss.csv')
        return loss[cols].tail(20)

    def show_win(self, data, pr=1.05, extra_cols=None):
        win = data.loc[data.profit > pr]
        print('len: ', len(win))
        cols = show_cols.copy()
        if extra_cols is not None:
            cols += extra_cols
        #     print(win[cols])
        # win[cols].to_csv('lian_yang_win.csv')
        return win[cols].tail(20)

    def get_filtered(self, data, f, extra_cols=None):
        win = data.loc[f]
        print('len: ', len(win))
        cols = show_cols.copy()
        print(cols)
        print('extra, ', extra_cols)
        if extra_cols is not None:
            cols += extra_cols
        if len(win) < 200:
            print(win[cols])
        win[cols].to_csv('lian_yang_filtered.csv')
        return win
