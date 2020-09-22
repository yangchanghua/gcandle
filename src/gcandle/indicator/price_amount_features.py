from enum import Enum

import numpy as np


class FOOT_PEAK_TYPE(Enum):
    FOOT = 1
    PEAK = 2

FP_CHANGES_COUNT=3


def price_foot_peak_mark(data, fpd):
    data['day_idx'] = np.arange(1, data.shape[0] + 1)

    data['highest_b_' + str(fpd)] = data.high.rolling(fpd+1, min_periods=1).max()
    data['lowest_b_' + str(fpd)] = data.low.rolling(fpd+1, min_periods=1).min()
    data['highest_a_' + str(fpd)] = data['highest_b_' + str(fpd)].shift(-fpd)
    data['lowest_a_' + str(fpd)] = data['lowest_b_' + str(fpd)].shift(-fpd)

    data['max_around_' + str(fpd)] = data[['highest_b_' + str(fpd), 'highest_a_' + str(fpd)]].max(axis=1, skipna=False)
    data['min_around_' + str(fpd)] = data[['lowest_b_' + str(fpd), 'lowest_a_' + str(fpd)]].min(axis=1, skipna=False)

    foot_filter = data['min_around_' + str(fpd)] == data['low']
    peak_filter = data['max_around_' + str(fpd)] == data['high']
    data.loc[foot_filter, 'fp_type'] = FOOT_PEAK_TYPE.FOOT.value
    data.loc[peak_filter, 'fp_type'] = FOOT_PEAK_TYPE.PEAK.value
    fp_col = 'fp_around_' + str(fpd)
    data.loc[foot_filter | peak_filter, fp_col] = int(fpd)

    # drop duplicates
    if data.loc[data[fp_col] == fpd].shape[0] > 0:
        data.loc[data[fp_col] == int(fpd), 'last_fp'] = data['fp_type']
        data['last_fp'].fillna(method='ffill', inplace=True)
        data.loc[data[fp_col] == int(fpd), 'last_fp_idx'] = data['day_idx']
        data['last_fp_idx'].fillna(method='ffill', inplace=True)
        duplicates = (data[fp_col] == int(fpd)) & (data['last_fp'].shift(1) == data['fp_type']) & (
                data['day_idx'] - data['last_fp_idx'].shift(1) < fpd)
        # print(data.loc[duplicates, ['high', 'low', 'fp_type', fp_col]])
        data.loc[duplicates, fp_col] = np.nan
        data.drop(['last_fp', 'last_fp_idx'], axis=1, inplace=True)

    mask = (data['fp_around_' + str(fpd)].isna())
    data.loc[mask, 'fp_type'] = np.nan
    to_drop = [s for s in data.columns if 'max_' in s or 'min_' in s]
    data.drop(to_drop, axis=1, inplace=True)


def long_pivot_chg_features(data, fpd):

    data['day_idx'] = np.arange(1, data.shape[0] + 1)
    fp_around_col = 'fp_around_' + str(fpd)
    fp_rows = data[fp_around_col] == fpd

    chg_type = 'p' + str(fpd) + '_'
    n_points = FP_CHANGES_COUNT
    data.loc[fp_rows, 'fp_idx'] = data.loc[fp_rows, 'day_idx']
    data[chg_type + 'last_0_fp_price'] = data.close
    data[chg_type + 'last_0_fp_idx'] = data.day_idx

    #mark the pivot points
    cols = ['low', 'high', 'fp_idx', 'fp_type']
    for n in range(0, n_points):
        prefix = chg_type + 'last_' + str(n + 1) + '_'
        for col in cols:
            last_col = prefix + col
            data[last_col] = np.nan
            if n == 0:
                if data.loc[data[fp_around_col].shift(fpd) == fpd].shape[0] > 0:
                    data.loc[data[fp_around_col].shift(fpd) == fpd, last_col] = data[col].shift(fpd)
                # on fp day, last_col == col. So for fpd later, shift(1) should be the correct last_col.
                # data[last_col] = data[last_col].fillna(method='ffill')
            else:
                pre_last_col = chg_type + 'last_' + str(n) + '_' + col
                last_rows = data[pre_last_col].notna()
                data.loc[last_rows, last_col] = data.loc[last_rows, pre_last_col].shift(1)

    #fill:
    for n in range(1, n_points+1):
        prefix = chg_type + 'last_' + str(n) + '_'
        cols_to_adjust = [prefix + col for col in cols]
        data[cols_to_adjust] = data[cols_to_adjust].fillna(method='ffill')

        data[prefix + 'fp_price'] = np.nan
        if data.loc[data[prefix + 'fp_type'] == 1].shape[0] > 0:
            data.loc[data[prefix + 'fp_type'] == 1, prefix + 'fp_price'] = data[prefix + 'low']
        if data.loc[data[prefix  + 'fp_type'] == 2].shape[0] > 0:
            data.loc[data[prefix + 'fp_type'] == 2, prefix + 'fp_price'] = data[prefix + 'high']

        data[prefix + 'fp_dist'] = data.day_idx - data[prefix + 'fp_idx']
        data[prefix + 'fp_chg_len'] = data[chg_type + 'last_' + str(n-1) + '_fp_idx'] - data[prefix + 'fp_idx']
        data[prefix + 'fp_chg' ] = data[chg_type + 'last_' + str(n-1) + '_fp_price'] / data[prefix + 'fp_price']

    #special filling for ci xin
    treat_ci_xin_as_big_rise_fp(chg_type, data)

    to_drop = [s for s in data.columns if
               '_idx' in s or '_amount' in s or '_low' in s \
               or '_high' in s or 'shangshi' in s \
               or 'fp_around' in s or 'fp_type' in s \
               or 'fp_price' in s or 'last_0' in s\
               or 'close' in s]
    data.drop(to_drop, axis=1, inplace=True)
    return data


def treat_ci_xin_as_big_rise_fp(chg_type, data):
    data.loc[data.day_idx == 1, 'shangshi'] = data.open / 2
    data['shangshi'].fillna(method='ffill', inplace=True)
    ci_xin_has_only_one_fp_chg = (data[chg_type + 'last_1_fp_chg'].notna()) & (data[chg_type + 'last_2_fp_chg'].isna()) &\
                                 (data[chg_type + 'last_1_fp_idx'] != 1)

    if len(data.loc[ci_xin_has_only_one_fp_chg]) > 0:
        data.loc[ci_xin_has_only_one_fp_chg, chg_type + 'last_2_fp_chg'] = \
            data[chg_type + 'last_1_fp_price'] / data.shangshi
        data.loc[ci_xin_has_only_one_fp_chg, chg_type + 'last_2_fp_dist'] = \
            data.day_idx
        data.loc[ci_xin_has_only_one_fp_chg, chg_type + 'last_2_fp_chg_len'] = \
            data[chg_type + 'last_1_fp_idx']


def label_increase(data, days=8, threshold=1.3):
    col = 'a' + str(days) + '_max_prc'
    data[col] = data.close.shift(-days).rolling(days).max()
    data['win' + str(days)] = data[col] / data.close > threshold
