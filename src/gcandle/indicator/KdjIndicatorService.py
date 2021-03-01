import pandas as pd

import gcandle.indicator.price_amount_features as fp_features
from gcandle.core.indicator_service.fp_mark_service import FPMarkService
from gcandle.core.indicator_service.slave_indicator_service import SlaveIndicatorService
from gcandle.data.se_types import SeFreq
import talib

REPO_NAME_PREFIX = 'kdj_'


class KdjIndicatorService(SlaveIndicatorService):
    def __init__(self):
        super().__init__()
        self._freq = SeFreq.DAY

    #override
    def repo_name(self):
        return REPO_NAME_PREFIX + self._freq + '_' + self.master_service.get_key()

    #override
    def days_to_update(self):
        return 12

    #override
    def compute_indicator(self, data):
        codes = data.index.levels[1].unique().tolist()
        start = data.index.levels[0].min()
        end = data.index.levels[0].max()
        close = data.close
        RSI = talib.RSI(close)

        data = pd.concat([data, RSI], axis=1)
        return data


if __name__ == '__main__':
    print("This is fp features common db utils")

