import pandas as pd

import gcandle.indicator.price_amount_features as fp_features
from gcandle.core.indicator_service.fp_mark_service import FPMarkService
from gcandle.core.indicator_service.slave_indicator_service import SlaveIndicatorService
from gcandle.data.se_types import SeFreq

FP_FEATURE_REPO_NAME_PREFIX = 'fp_features_'


class FpFeatureService(SlaveIndicatorService):
    def __init__(self, fpd):
        super().__init__()
        self.fpd = fpd
        self.fp_mark_service = FPMarkService(fpd, freq=SeFreq.DAY)

    #override
    def repo_name(self):
        return FP_FEATURE_REPO_NAME_PREFIX + str(self.fpd) + '_' + self.master_service.get_key()

    #override
    def days_to_update(self):
        return int(self.fpd * 15)

    #override
    def compute_indicator(self, data):
        codes = data.index.levels[1].unique().tolist()
        start = data.index.levels[0].min()
        end = data.index.levels[0].max()
        fpd = self.fpd
        fp_mark_data = self.fp_mark_service.read_by_dates(codes, start, end=end)
        if fp_mark_data is None or fp_mark_data.shape[0] < 1:
            return None

        data = pd.concat([data, fp_mark_data], axis=1)
        data = data.groupby(level=1).apply(fp_features.long_pivot_chg_features, fpd)
        return data


if __name__ == '__main__':
    print("This is fp features common db utils")