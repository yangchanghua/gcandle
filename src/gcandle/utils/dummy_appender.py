import gcandle.utils.date_time_utils as date_time_utils
import pandas as pd

dummy_key = 'dummy_row'
dummy_dict = {
    'open': 0,
    'close': 0,
    'low': 0,
    'high': 0,
    'volume': 0,
    'amount': 0,
    dummy_key: True
}


def get_dummy_day_row(date, code):
    index = pd.MultiIndex.from_arrays([[date], [code]], names=('date', 'code'))
    return pd.DataFrame(data=dummy_dict, index=index)


def append_dummy_rows(data, n_rows=1):
    last_date = data.index.levels[0][-1]
    code = data.index.levels[1][0]
    next_day = date_time_utils.Date.from_datetime(last_date)
    for n in range(n_rows):
        next_day = next_day.next_trade_day()
        dummy_row = get_dummy_day_row(next_day.as_datetime(), code)
        data = data.append(dummy_row, sort=True)
    return data


def get_dummy_rows(data):
    if dummy_key in data.columns:
        return data.loc[data[dummy_key] == True]
    else:
        return None


def exclude_dummy_rows(data):
    if dummy_key in data.columns:
        data = data.loc[data[dummy_key].isna() | (data[dummy_key] == False)]
        data = data.drop(columns=[dummy_key])
    return data
