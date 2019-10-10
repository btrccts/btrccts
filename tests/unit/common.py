import pandas


def pd_ts(s):
    return pandas.Timestamp(s, tz='UTC')
