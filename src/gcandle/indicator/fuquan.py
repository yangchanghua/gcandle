# coding:utf-8

from decimal import *
import numpy as np
import pandas as pd
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE

category = {
    '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动', '5': '股本变化',
    '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市', '10': '可转债上市',
    '11': '扩缩股', '12': '非流通股缩股', '13': '送认购权证', '14': '送认沽权证'}


def stock_to_qfq(data, verbose=False):
    start = str(data.index.levels[0].min())[0:10]
    end = str(data.index.levels[0].max())[0:10]

    code = data.index.levels[1][0]
    xdxr_data = SECURITY_DATA_READ_SERVICE.read_fq_info(code, start, end)

    return do_prices_qfq(
        bfq_data=data,
        xdxr_data=xdxr_data,
        verbose=verbose
    )


def do_prices_qfq(bfq_data, xdxr_data, verbose=False):
    if xdxr_data is None:
        print("{} has no xdxr data ".format(bfq_data.index.levels[1][0]))
        return bfq_data

    if len(xdxr_data) <= 0:
        print('Warn: no xdxr info, market value will be missing for {}'.format(bfq_data.index.levels[1][bfq_data.index.codes[1]]))
        return bfq_data

    data = bfq_data.assign(if_trade=True)

    # 复权股本
    # data = to_qfq_shares(data, xdxr_data)
    data = data.query('if_trade==True')

    #复权股价
    info = xdxr_data.query('category==1').loc[bfq_data.index[0]:bfq_data.index[-1]]
    if len(info) > 0:
        fq_columns = ['h', 'r']
        cummulate_rights(info)
        data = pd.concat(
            [
                data,
                info.loc[bfq_data.index[0]:bfq_data.index[-1], fq_columns]
            ],
            axis=1
        )
        rows_to_watch = None
        if verbose:
            rows_to_watch = data[fq_columns].notna().any(axis=1)
            rows_to_watch = rows_to_watch.replace(False, np.nan).\
                fillna(method='bfill', limit=3).fillna(method='ffill', limit=3).fillna(False).replace(1, True)
        data[fq_columns] = data[fq_columns].shift(-1).fillna(method='bfill').astype(float)
        data['h'].fillna(0, inplace=True)
        data['r'].fillna(1, inplace=True)

        # 如果中间有除权信息但是没有交易， if_trade 就会是False
        data['if_trade'].fillna(value=False, inplace=True)

        # 如果中间有除权信息但是没有交易，股价会用上个交易日的信息填充
        data = data.fillna(method='ffill')

        data = data.fillna(0)
        if verbose:
            print('before qfq, data:\n', data.loc[rows_to_watch, ['close', 'volume', 'h', 'r']])

        data['old_close'] = data['close']
        for col in ['open', 'high', 'low', 'close']:
            data[col] = (data['r'] * (data[col] * 10 - data['h']) / 10).round(3)
        if 'volume' in data.columns:
            vol = 'volume'
        else:
            vol = 'vol'
        data[vol] = (data['old_close'] * data[vol] / data['close']).round(3)

        if verbose:
            print('after qfq, data:\n', data.loc[rows_to_watch, ['close', 'volume', 'h', 'r']])
    res = data.query('if_trade==True').drop(
        ['if_trade',
         'gubenadj',
         'old_close',
         'h',
         'r',
         'category'],
        axis=1,
        errors='ignore'
    )
    return res


def do_qfq_guben(data, xdxr_data):
    # 股本送配之后/送配前的比例
    xdxr_data = xdxr_data.query('category!=6')
    xdxr_data['gubenadj'] = (10 + xdxr_data['peigu'] + xdxr_data['songzhuangu']) / 10
    xdxr_data['gubenadj'].fillna(1, inplace=True)
    d = xdxr_data[['houzongguben', 'panhouliutong', 'gubenadj']]
    guben = {'zgb': [], 'lgb': []}
    i = 0
    for r in d.itertuples():
        if pd.isnull(r.houzongguben):
            if i == 0: # The first one is just zero
                guben['zgb'].append(0)
                guben['lgb'].append(0)
            else:
                guben['zgb'].append(guben['zgb'][i - 1] * r.gubenadj)
                guben['lgb'].append(guben['lgb'][i - 1] * r.gubenadj)
        else:
            guben['zgb'].append(r.houzongguben)
            guben['lgb'].append(r.panhouliutong)
        i = i + 1
    nv = pd.DataFrame(guben, index=xdxr_data.index)
    xdxr_data = xdxr_data.join(nv)

    mv = xdxr_data.loc[:data.index[-1],
                                       ['zgb',
                                        'lgb']].drop_duplicates()
    data = pd.concat([data, mv], axis=1)
    data[['zgb', 'lgb']] = data[['zgb', 'lgb']].fillna(method='ffill')
    # 市值计算，股本使用复权的数据, 股价不复权, 单位万元。
    data = data.assign(zsz=data.close*data.zgb, lsz=data.close*data.lgb)
    return data


def fq_info(hong, pei, peij, song, h, ratio):
    hong = str(hong)
    pei = str(pei)
    peij = str(peij)
    song = str(song)
    # h = str(h)
    # ratio = str(ratio)
    count = Decimal(10) / (Decimal(10) + Decimal(pei) + Decimal(song))

    fenhong = Decimal(h)/Decimal(count)

    fenhong += Decimal(hong) - Decimal(pei) * Decimal(peij)

    count =  Decimal(count) * Decimal(ratio)
    return (fenhong, count)


def cummulate_rights(xdxr_data):
    idf = xdxr_data[::-1]
    hh = [0]
    rr = [1]
    for r in idf.itertuples():
        lh = hh[-1]
        lr = rr[-1]
        nv = fq_info(r.fenhong, r.peigu, r.peigujia, r.songzhuangu, lh, lr)
        hh.append(nv[0])
        rr.append(nv[1])

    # cumulative fenhong and guben ratio
    xdxr_data['h'] = hh[-1:0:-1]
    xdxr_data['r'] = rr[-1:0:-1]



