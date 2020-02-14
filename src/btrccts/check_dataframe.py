import numpy


def _check_dataframe(ohlcvs, timeframe, needed_columns=['low', 'high']):
    index = ohlcvs.index
    if index[0] > timeframe.start_date() or index[-1] < timeframe.end_date():
        raise ValueError('ohlcv needs to cover timeframe')
    for col in needed_columns:
        if col not in ohlcvs.columns:
            raise ValueError('ohlcv {} needs to be provided'.format(col))
    try:
        ohlcvs.index.freq = '1T'
    except ValueError:
        raise ValueError('ohlcv needs to be in 1T format')
    try:
        result = ohlcvs.astype(numpy.float)
        if not numpy.isfinite(result).values.all():
            raise ValueError('ohlcv needs to finite')
    except ValueError as e:
        raise ValueError('ohlcv {}'.format(str(e)))
    return result
