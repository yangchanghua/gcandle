import math
import numpy as np
import pandas as pd

def increase_limit(item):
    return item.close / item.pc >= 1.099


def decrease_limit(item):
    return item.close / item.pc <= 0.9001


def ultimate_increase(item):
    return item.close > item.pc and (item.open == item.close == item.high == item.low)


def ultimate_decrease(item):
    return item.close < item.pc and (item.open == item.close == item.high == item.low)



def EMA(Series, N):
    return pd.Series.ewm(Series, span=N, min_periods=N - 1, adjust=True).mean()


def MA(Series, N):
    return pd.Series.rolling(Series, N).mean()


def SMA(Series, N, M=1):
    ret = []
    i = 1
    length = len(Series)
    # 跳过X中前面几个 nan 值
    while i < length:
        if np.isnan(Series.iloc[i]):
            i += 1
        else:
            break
    preY = Series.iloc[i]  # Y'
    ret.append(preY)
    while i < length:
        Y = (M * Series.iloc[i] + (N - M) * preY) / float(N)
        ret.append(Y)
        preY = Y
        i += 1
    return pd.Series(ret, index=Series.tail(len(ret)).index)


def DIFF(Series, N=1):
    return pd.Series(Series).diff(N)


def HHV(Series, N):
    return pd.Series(Series).rolling(N).max()


def LLV(Series, N):
    return pd.Series(Series).rolling(N).min()


def SUM(Series, N):
    return pd.Series.rolling(Series, N).sum()


def ABS(Series):
    return abs(Series)


def MAX(A, B):
    var = IF(A > B, A, B)
    return var


def MIN(A, B):
    var = IF(A < B, A, B)
    return var


def SINGLE_CROSS(A, B):
    if A.iloc[-2] < B.iloc[-2] and A.iloc[-1] > B.iloc[-1]:
        return True
    else:
        return False


def CROSS(A, B):
    var = np.where(A < B, 1, 0)
    return (pd.Series(var, index=A.index).diff() < 0).apply(int)


def COUNT(COND, N):
    return pd.Series(np.where(COND, 1, 0), index=COND.index).rolling(N).sum()


def IF(COND, V1, V2):
    var = np.where(COND, V1, V2)
    return pd.Series(var, index=V1.index)


def IFAND(COND1, COND2, V1, V2):
    var = np.where(np.logical_and(COND1, COND2), V1, V2)
    return pd.Series(var, index=V1.index)


def IFOR(COND1, COND2, V1, V2):
    var = np.where(np.logical_or(COND1, COND2), V1, V2)
    return pd.Series(var, index=V1.index)


def REF(Series, N):
    var = Series.diff(N)
    var = Series - var
    return var


def LAST(COND, N1, N2):
    N2 = 1 if N2 == 0 else N2
    assert N2 > 0
    assert N1 > N2
    return COND.iloc[-N1:-N2].all()


def STD(Series, N):
    return pd.Series.rolling(Series, N).std()


def AVEDEV(Series, N):
    return Series.rolling(N).apply(lambda x: (np.abs(x - x.mean())).mean(), raw=True)


def MACD(CLOSE, FAST=12, SLOW=26, MID=9):
    EMAFAST = EMA(CLOSE, FAST)
    EMASLOW = EMA(CLOSE, SLOW)
    DIFF = EMAFAST - EMASLOW
    DEA = EMA(DIFF, MID)
    MACD = (DIFF - DEA) * 2
    DICT = {'DIFF': DIFF, 'DEA': DEA, 'MACD': MACD}
    VAR = pd.DataFrame(DICT)
    return VAR


def BBIBOLL(Series, N1, N2, N3, N4, N, M):

    bbiboll = BBI(Series, N1, N2, N3, N4)
    UPER = bbiboll + M * STD(bbiboll, N)
    DOWN = bbiboll - M * STD(bbiboll, N)
    DICT = {'BBIBOLL': bbiboll, 'UPER': UPER, 'DOWN': DOWN}
    VAR = pd.DataFrame(DICT)
    return VAR


def BBI(Series, N1, N2, N3, N4):
    bbi = (MA(Series, N1) + MA(Series, N2) +
           MA(Series, N3) + MA(Series, N4)) / 4
    DICT = {'BBI': bbi}
    VAR = pd.DataFrame(DICT)
    return VAR


def BARLAST(cond, yes=True):
    if isinstance(cond.index, pd.MultiIndex):
        return len(cond) - cond.index.levels[0].tolist().index(cond[cond != yes].index[-1][0]) - 1
    elif isinstance(cond.index, pd.DatetimeIndex):
        return len(cond) - cond.index.tolist().index(cond[cond != yes].index[-1]) - 1


XARROUND = lambda x, y: np.round(y * (round(x / y - math.floor(x / y) + 0.00000000001) + math.floor(x / y)), 2)

