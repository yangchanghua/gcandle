from enum import Enum


class SeFreq(Enum):
    ONE_MIN = '1min'
    FIVE_MIN = '5min'
    FIFTEEN_MIN = '15min'
    THIRTY_MIN = '30min'
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'

    def is_minutes(self):
        return self == SeFreq.ONE_MIN or \
               self == SeFreq.FIVE_MIN or \
               self == SeFreq.FIFTEEN_MIN or \
               self == SeFreq.THIRTY_MIN

    def keys_to_index(self):
        keys = ['code', 'update_time']
        if self.is_minutes():
            keys.extend(['datetime', 'date'])
        else:
            keys.append('date')
        return keys

    def upper_freq_for_chan(self):
        if self == SeFreq.ONE_MIN:
            return SeFreq.FIVE_MIN
        if self == SeFreq.FIVE_MIN:
            return SeFreq.THIRTY_MIN
        if self == SeFreq.THIRTY_MIN:
            return SeFreq.DAY
        return None

    def lower_freq_for_chan(self):
        if self == SeFreq.MONTH:
            return SeFreq.WEEK
        if self == SeFreq.MONTH:
            return SeFreq.DAY
        if self == SeFreq.DAY:
            return SeFreq.THIRTY_MIN
        if self == SeFreq.THIRTY_MIN:
            return SeFreq.FIVE_MIN
        if self == SeFreq.FIVE_MIN:
            return SeFreq.ONE_MIN
        return None


class SeType(Enum):
    Stock = 'stock'
    Index = 'index'
    ETF = 'etf'

    def prefix_key(self):
        return self.value + '_prefix'

    def repo_name(self, freq: SeFreq):
        return self.value + '_' + freq.value


class Exchange(Enum):
    SZSE = {
        SeType.Stock.prefix_key(): ['00', '30', '02'],
        SeType.Index.prefix_key(): ['39'],
        SeType.ETF.prefix_key(): ['15']
    }
    SHSE = {
        SeType.Stock.prefix_key(): ['6'],
        SeType.Index.prefix_key(): ['000'],
        SeType.ETF.prefix_key(): ['51']
    }

    def contains_code(self, typ:SeType, code):
        for prefix in self.value[typ.prefix_key()]:
            if code.startswith(prefix):
                return True
        return False

    def code_to_type(self, code):
        for t in SeType:
            if self.contains_code(t, code):
                return t
        return None

    def code_to_type_str(self, code):
        t = self.code_to_type(code)
        if t is None:
            return None
        else:
            return t.value

    @classmethod
    def from_type_and_code(cls, typ: SeType, code):
        if Exchange.SHSE.contains_code(typ, code):
            return Exchange.SHSE
        elif Exchange.SZSE.contains_code(typ, code):
            return Exchange.SZSE
        else:
            return None

