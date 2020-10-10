from gcandle.core.buy_strategy.BuyStrategy import BuyStrategy
from gcandle.core.indicator_service.master_indicator_service import MasterIndicatorService
from gcandle.utils.date_time_utils import Date
DAYS_BEFORE_TO_SCAN = 15


def to_jzsec_code(c):
    if c.startswith('00') or c.startswith('300'):
        return 'SZSE.' + c
    else:
        return 'SHSE.' + c


class DayBarCandidatesScanner:
    def __init__(self, name, master_service: MasterIndicatorService, buy_strategy: BuyStrategy):
        self.name = name
        self.master_service = master_service
        self.buy_strategy = buy_strategy
        self.buy_ratio = None

    def set_buy_ratio(self, ratio):
        self.buy_ratio = ratio

    def scan(self, argv):
        if len(argv) > 1:
            codes = self.master_service.read_all_codes()
            end = argv[1]
            start = Date(end).get_before(DAYS_BEFORE_TO_SCAN).as_str()
            print('## Scan {} candidates with backtest data from {} to {}'.format(self.name, start, end))
            data = self.master_service.read_with_slave_for_backtest(codes, start, end=end)
        else:
            print('## Scan {} candidates for trade'.format(self.name))
            data = self.master_service.read_with_slave_for_trade()
        self.__do_scan(data)

    def __do_scan(self, data):
        next_trade_day = str(data.index.levels[0].max())[0:10]
        print("Target trade date: {}".format(next_trade_day))
        buy_strategy = self.buy_strategy
        data = buy_strategy.calc_and_forward_fill_buy_filter(data, True)
        buy_strategy.update_buy_prices(data)

        data = data.reset_index()
        buy_filter = buy_strategy.get_buy_filter_before_buy_day(data)
        day_filter = (data.date == next_trade_day)
        data.loc[buy_filter & day_filter, 'to_buy'] = data.bpr

        data['target_chg'] = data['to_buy'] / data.pc
        data = data.loc[data.target_chg > 0]

        if data.shape[0] > 0:
            self.__save_candidates(data, next_trade_day)
        else:
            print('No candidates on {}'.format(next_trade_day))

    def __smart_buy_ratio(self, n):
        r = 0.35
        if n > 20:
            r = 0.25
        elif n > 40:
            r = 0.2
        return r

    def __save_candidates(self, data, next_day):

        if data.shape[0] > 0:
            print('{} candidates identified on {} for {}'.format(data.shape[0], next_day, self.name))
            buy_ratio = self.buy_ratio
            if buy_ratio is None:
                buy_ratio = self.__smart_buy_ratio(len(data))

            target_list = [[to_jzsec_code(r.code), round(r.to_buy, 2),
                            buy_ratio,  round(r.target_chg, 4)]
                           for r in data.itertuples(index=False)]
            fname = '{}_juejin_list.txt'.format(self.name)
            f = open(fname, 'w')
            print('g_today = \'{}\''.format(next_day), file=f)
            print('g_targets_to_buy = [', file=f)
            for r in target_list:
                print(r, ',', file=f)
            print(']', file=f)
            f.close()
        else:
            print('No candidates on {}'.format(next_day))


if __name__ == '__main__':
    print("Common Day Bar Candidates Scanner class")
