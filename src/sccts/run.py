import logging
import numpy
import os
import pandas
from enum import Enum


def serialize_symbol(symbol):
    return symbol.replace('/', '_')


def load_ohlcvs(ohlcv_dir, exchange_names, symbols):
    result = {}
    complete_exchange = False
    if len(symbols) == 0:
        complete_exchange = True
    for exchange_name in exchange_names:
        exchange_result = {}
        result[exchange_name] = exchange_result
        exchange_path = os.path.join(ohlcv_dir, exchange_name)
        if complete_exchange:
            try:
                symbols = [x[:-4].replace('_', '/')
                           for x in os.listdir(exchange_path)
                           if x.endswith('.csv')]
            except FileNotFoundError:
                raise FileNotFoundError(
                    'Cannot find ohlcv directory for exchange ({})'
                    .format(exchange_name))
        for symbol in symbols:
            file_path = os.path.join(exchange_path,
                                     '{}.csv'.format(serialize_symbol(symbol)))
            try:
                ohlcv = pandas.read_csv(
                    file_path,
                    index_col=0, parse_dates=[0], dtype=numpy.float,
                    date_parser=partial(pandas.to_datetime, utc=True)
                    )
                exchange_result[symbol] = ohlcv
            except FileNotFoundError:
                raise FileNotFoundError(
                    'Cannot find symbol ({}) file for exchange ({})'
                    .format(symbol, exchange_name))
            except ValueError:
                raise ValueError(
                    'Cannot parse symbol ({}) file for exchange ({})'
                    .format(symbol, exchange_name))
    return result


class ExitReason(Enum):

    STOPPED = 'stopped'
    EXCEPTION = 'exception'
    FINISHED = 'finished'


def main_loop(timeframe, algorithm):
    logger = logging.getLogger(__package__)
    logger.info('Starting main_loop')
    while timeframe.date() is not None:
        try:
            algorithm.next_iteration()
        except (SystemExit, KeyboardInterrupt) as e:
            logger.info('Stopped because of {}: {}'.format(
                type(e).__name__, e))
            algorithm.exit(reason=ExitReason.STOPPED)
            raise e
        except BaseException as e:
            logger.error('Error occured during next_iteration')
            logger.exception(e)
            try:
                algorithm.handle_exception(e)
            except BaseException as e:
                logger.error(
                    'Exiting because of exception in handle_exception')
                logger.exception(e)
                algorithm.exit(reason=ExitReason.EXCEPTION)
                raise e
        timeframe.add_timedelta()
    algorithm.exit(reason=ExitReason.FINISHED)
    logger.info('Finished main_loop')
    return algorithm


def execute_algorithm(exchange_names, symbols, AlgorithmClass, args,
                      start_balances,
                      pd_start_date, pd_end_date, pd_timedelta,
                      data_dir=USER_DATA_DIR):
    timeframe = Timeframe(pd_start_date=pd_start_date,
                          pd_end_date=pd_end_date,
                          pd_timedelta=pd_timedelta)
    ohlcv_dir = os.path.join(data_dir, 'ohlcv')
    ohlcvs = load_ohlcvs(ohlcv_dir=ohlcv_dir,
                         exchange_names=exchange_names,
                         symbols=symbols)
    exchange_backends = {}
    for exchange_name in exchange_names:
        exchange_backends[exchange_name] = ExchangeBackend(
            timeframe=timeframe,
            balances=start_balances.get(exchange_name, {}),
            ohlcvs=ohlcvs.get(exchange_name, {}))
    context = BacktestContext(timeframe=timeframe,
                              exchange_backends=exchange_backends)
    algorithm = AlgorithmClass(context=context,
                               args=args)
    return main_loop(timeframe=timeframe,
                     algorithm=algorithm)
