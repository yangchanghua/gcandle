import pandas as pd
from gcandle.core.indicator_service.master_indicator_db import MasterIndicatorDB
from gcandle.core.indicator_service.slave_indicator_service import SlaveIndicatorService
from gcandle.utils.date_time_utils import Date

DEFAULT_START_CREATE_DATE = '2006-01-01'


class MasterIndicatorService:
    def __init__(self, name, indicator_func):
        self.name = name
        self.indicator_func = indicator_func
        self.repo = MasterIndicatorDB(self.name, self.indicator_func)
        self.slave_services = []

    def get_key(self):
        return self.name

    def add_slave_service(self, service: SlaveIndicatorService):
        service.set_master(self)
        self.slave_services.append(service)

    def read_all_codes(self):
        return self.repo.read_all_codes()

    def read_master(self, codes, start, end):
        return self.repo.read_for_backtest(codes, start, end)

    def read_master_for_trade(self):
        return self.repo.read_for_trade()

    def read_with_slave_for_backtest(self, codes, start, end, train=None):
        code_list = codes
        if codes is None:
            code_list = self.repo.read_all_codes()
        # code_list = ['600587']
        if train is None:
            master_data = self.repo.read_for_backtest(code_list, start, end)
        elif train:
            master_data = self.repo.read_train_for_backtest(code_list, start, end)
        else:
            master_data = self.repo.read_test_for_backtest(code_list, start, end)

        print('{} loaded'.format(self.name))
        data = self.load_slave_and_join(master_data)
        return data

    def read_with_slave_for_trade(self):
        master_data = self.repo.read_for_trade()
        data = self.load_slave_and_join(master_data)
        return data

    def load_slave_and_join(self, master_data):
        codes = master_data.index.levels[1].unique().tolist()
        start = master_data.index.levels[0].min()
        end = master_data.index.levels[0].max()
        slave_data = self.read_slaves(codes, start, end)
        data = pd.concat([master_data] + slave_data, axis=1, join='outer', sort=True)
        data = data.sort_index(level=0)
        print('Joined')
        return data

    def read_slaves(self, codes, start, end):
        slave_data_list = []
        for service in self.slave_services:
            slave_data_list.append(service.read_by_dates(codes, start, end))
        print('slave data loaded')
        return slave_data_list

    def update_for_all_codes(self):
        self.repo.update_all_codes()
        codes = self.repo.read_all_codes()
        for service in self.slave_services:
            service.update_all_codes(codes)

    def recreate_for_all_codes(self, start=DEFAULT_START_CREATE_DATE, end=None):
        if self.user_confirm_to_continue():
            if end is None:
                end = Date().as_str()
            self.repo.recreate_all_codes(start, end)
            print("{} master indicator recreate done".format(self.name))
            codes = self.repo.read_all_codes()
            for service in self.slave_services:
                service.recreate_all_codes(codes, start, end)
                print("{} slave indicator recreate done".format(service.repo_name()))
        else:
            print("Cancelled")

    def user_confirm_to_continue(self):
        answer = input("Do you really want to recreate all indicators? Enter yes or no: ")
        return answer == 'yes'


if __name__ == '__main__':
    print("Not callable, master indicator service")
