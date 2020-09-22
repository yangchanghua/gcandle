import datetime

DAY_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H%M%S'

class Date:
    def __init__(self, s=None, obj=None):
        if s is not None:
            self.obj = datetime.datetime.strptime(s, DAY_FORMAT)
        elif obj is not None:
            self.obj = obj
        else:
            self.obj = datetime.datetime.now()

    @classmethod
    def from_str(cls, s, format=DAY_FORMAT):
        obj = datetime.datetime.strptime(s, format)
        return cls(obj=obj)

    @classmethod
    def from_datetime(cls, obj):
        return cls(obj=obj)

    def as_str(self):
        return datetime.datetime.strftime(self.obj, DAY_FORMAT)

    def as_str_with_time(self):
        return datetime.datetime.strftime(self.obj, DATETIME_FORMAT)

    def as_datetime(self):
        return self.obj

    def get_after(self, days=0, hours=0, minutes=0):
        delta = datetime.timedelta(days=days, hours=hours, minutes=minutes)
        return Date.from_datetime(self.obj + delta)

    def get_before(self, days=0, hours=0, minutes=0):
        delta = datetime.timedelta(days=days, hours=hours, minutes=minutes)
        return Date.from_datetime(self.obj - delta)

    def delta_to(self, other):
        if type(other) == str:
            other = Date.from_str(other)
        return self.obj - other.obj

    #Not considering public holidays, use with caution.
    def next_trade_day(self):
        next_day = self.obj
        if next_day.weekday() == 4:
            next_day = next_day + datetime.timedelta(days=3)
        else:
            next_day = next_day + datetime.timedelta(days=1)
        return Date.from_datetime(next_day)
