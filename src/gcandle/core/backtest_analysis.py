import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
from gcandle.core.backtest.backtest_accounting import BacktestAccount
from gcandle.data.se_types import SeType
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE


class BacktestAnalysor():
    def __init__(
            self,
            account: BacktestAccount,
            benchmark_code='000300',
            benchmark_name='沪深300',
    ):
        self.account = account
        self.benchmark_code = benchmark_code
        self.benchmark_type = benchmark_name
        bm_data = self._read_bm_data(self.benchmark_code)
        assets_df = self.account.get_daily_assets()
        daily_snapshot = pd.concat([bm_data, assets_df], axis=1, join='outer').fillna(method='pad')
        daily_snapshot.to_csv('backtest_daily_assets_snapshot.csv')
        self._daily_assets = daily_snapshot['asset']
        self._daily_cash = daily_snapshot['cash']
        self._daily_se_value = daily_snapshot['se_value']
        self._bm_assets = (daily_snapshot.close / float(daily_snapshot.close.iloc[0])) * float(self._daily_assets[0])
        self.days_of_benchmark = len(bm_data)
        self.init_cash = self.account.init_cash

    @property
    def max_dropback(self):
        return round(
            float(
                max(
                    [
                        (self._daily_assets.iloc[idx] - self._daily_assets.iloc[idx::].min())
                        / self._daily_assets.iloc[idx]
                        for idx in range(len(self._daily_assets))
                    ]
                )
            ),
            2
        )

    @property
    def total_commission(self):
        return float(
            -abs(round(self.account.history_table.commission.sum(),
                       2))
        )

    @property
    def total_tax(self):
        return float(-abs(round(self.account.history_table.tax.sum(), 2)))

    @property
    def profit_money(self):
        return float(round(self._daily_assets.iloc[-1] - self._daily_assets.iloc[0], 2))

    @property
    def profit(self):
        return round(float(self.calc_profit(self._daily_assets)), 2)

    @property
    def profit_pct(self):
        return self.calc_profitpctchange(self._daily_assets)

    @property
    def annualize_return(self):
        return round(
            float(self.calc_annualize_return(self._daily_assets,
                                             self.days_of_benchmark)),
            2
        )

    def _read_bm_data(self, bm_code):
        return SECURITY_DATA_READ_SERVICE.read_security_data_for_single(
            bm_code,
            self.account.start_date,
            self.account.end_date,
            typ=SeType.Index
        ).reset_index().\
            drop(columns=['code', 'open', 'high', 'low', 'vol', 'amount',
                          'up_count', 'down_count']).\
            set_index('date')

    @property
    def benchmark_profit(self):
        return round(float(self.calc_profit(self._bm_assets)), 2)

    @property
    def benchmark_annualize_return(self):
        return round(
            float(
                self.calc_annualize_return(
                    self._bm_assets,
                    self.days_of_benchmark
                )
            ),
            2
        )

    @property
    def benchmark_profitpct(self):
        return self.calc_profitpctchange(self._bm_assets)

    def set_benchmark(self, code, market_type):
        self.benchmark_code = code
        self.benchmark_type = market_type

    def calc_annualize_return(self, assets, days):
        return round(
            pow(float(assets.iloc[-1]) / float(assets.iloc[0]),
            250 / (float(days))) - 1,
            2
        )

    def calc_profitpctchange(self, assets):
        return assets[::-1].pct_change()[::-1]

    def calc_profit(self, assets):
        return (float(assets.iloc[-1]) / float(assets.iloc[0])) - 1

    def calc_sharpe(self, annualized_returns, volatility_year, r=0.05):
        if volatility_year == 0:
            return 0
        return (annualized_returns - r) / volatility_year

    def plot_assets_curve(self, length=16, height=12):
        print("Ploting assets curve")
        plt.style.use('ggplot')
        plt.figure(figsize=(length, height))
        plt.subplot(211)
        plt.title('Assets/Benchmark')
        self._daily_assets.plot()
        self._bm_assets.plot()
        asset_p = mpatches.Patch(
            color='red',
            label='{}'.format('account')
        )
        asset_b = mpatches.Patch(
            label='benchmark {}'.format(self.benchmark_code)
        )
        plt.legend(handles=[asset_p, asset_b], loc=0)

        plt.subplot(212)
        plt.axis('off')
        xtext = 0.05
        summary = self.summary()
        self.plot_indicator_text(xtext, summary, length)
        return plt

    def plot_indicator_text(self, x, summary, length):
        x_step = 0.2
        y_step = 0.2
        y = 0.8
        items = summary.keys()
        for item in items:
            if x > 0.8:
                x = 0.05
                y -= y_step
            plt.text(
                x,
                y,
                '{} : {}'.format(item,
                                 summary[item]),
                fontsize=12,
                ha='left',
                rotation=0,
                wrap=True
            )
            x += x_step

    def summary(self):
        return {
            'profit rate': round(self.profit, 2),
            'annual profit': round(self.annualize_return, 2),
            'max drop': self.max_dropback,
            'trade days': self.days_of_benchmark,
            'bm annual profit': self.benchmark_annualize_return,
            'initial assets': "%0.2f" % (float(self._daily_assets[0])),
            'final assets': "%0.2f" % (float(self._daily_assets.iloc[-1])),
            'total tax': self.total_tax,
            'total commission': self.total_commission,
        }
