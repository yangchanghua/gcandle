from gcandle.core.indicator_service.indicator_service import IndicatorService
from gcandle.data.se_types import SeFreq
from gcandle.indicator.price_amount_features import (
    price_foot_peak_mark,
)

FP_MARK_REPO_NAME_PREFIX = 'fp_marks_'
MIN_FP_MARK_DATE = '2016-01-01'


class FPMarkService(IndicatorService):
    def __init__(self, fpd, freq: SeFreq):
        name = FP_MARK_REPO_NAME_PREFIX + str(fpd) + '_' + freq.value
        days = fpd * 3
        super().__init__(name, self.calc_fp_marks, freq=freq, days_to_update=days)
        self.fpd = fpd

    def calc_fp_marks(self, data):
        price_foot_peak_mark(data, self.fpd)
        return data

    def pre_save(self, data):
        if data is not None and len(data) > 0:
            data = data.loc[(data['fp_around_' + str(self.fpd)] > 0)].copy()
            cols = ['open', 'close', 'low', 'high', 'vol', 'amount', 'day_idx']
            if 'hour' in data.columns:
                cols.append('hour')
            if 'minute' in data.columns:
                cols.append('minute')
            cols.extend([c for c in data.columns if 'highest_' in c or 'lowest' in c])
            data.drop(columns=cols, inplace=True)
        return data

