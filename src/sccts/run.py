import pandas
import numpy
import os


def serialize_symbol(symbol):
    return symbol.replace('/', '_')


def load_ohlcvs(basedir, exchange_names, symbols):
    result = {}
    for exchange_name in exchange_names:
        exchange_result = {}
        result[exchange_name] = exchange_result
        for symbol in symbols:
            file_path = os.path.join(basedir, exchange_name,
                                     '{}.csv'.format(serialize_symbol(symbol)))
            try:
                exchange_result[symbol] = pandas.read_csv(
                    file_path,
                    index_col=0, parse_dates=[0], dtype=numpy.float)
            except FileNotFoundError:
                raise FileNotFoundError(
                    'Cannot find symbol ({}) file for exchange ({})'
                    .format(symbol, exchange_name))
    return result
