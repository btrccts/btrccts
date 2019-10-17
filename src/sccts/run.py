import argparse
import appdirs
import json
import logging
import numpy
import os
import pandas
from ccxt.base.errors import NotSupported
from ccxt.base.exchange import Exchange
from enum import Enum, auto
from functools import partial
from sccts.backtest import BacktestContext
from sccts.exchange_backend import ExchangeBackend
from sccts.timeframe import Timeframe

USER_CONFIG_DIR = appdirs.user_config_dir(__package__)
USER_DATA_DIR = appdirs.user_data_dir(__package__)


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

    STOPPED = auto()
    EXCEPTION = auto()
    FINISHED = auto()


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


def parse_params_and_execute_algorithm(AlgorithmClass):
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', default='',
                        help='Date to start backtesting, ignored in live mode')
    parser.add_argument('--end-date', default='2009-01-01',
                        help='Date to end backtesting')
    parser.add_argument('--timedelta', default='1m',
                        help='Timedelta between each iteration')
    parser.add_argument('--exchanges', default='',
                        help='Exchange ids comma separated to load ohlcv')
    parser.add_argument('--symbols', default='',
                        help='Symbols (comma separated) to load ohlcv '
                             'per exchange')
    parser.add_argument('--data-directory', default=USER_DATA_DIR,
                        help='directory where data is stored'
                             ' (e.g. ohlcv data')
    parser.add_argument('--config-directory', default=USER_CONFIG_DIR,
                        help='directory where config is stored'
                             ' (e.g. exchange parameters')
    parser.add_argument('--auth-aliases', default=None,
                        help='Auth aliases for different exchange'
                             ' config files')
    parser.add_argument('--live', action='store_true',
                        help='Trade live on exchanges')
    parser.add_argument('--start-balances', default='{}',
                        help='Trade live on exchanges')
    AlgorithmClass.configure_argparser(parser)
    args = parser.parse_args()
    logger = logging.getLogger(__package__)

    def split_parameters(p):
        if p == '':
            return []
        return p.split(',')

    exchange_names = split_parameters(args.exchanges)
    symbols = split_parameters(args.symbols)
    if not args.live:
        if len(exchange_names) == 0:
            logger.warning('No exchanges specified, do not load ohlcv')
        if len(symbols) == 0:
            logger.warning('No symbols specified, load all ohlcvs per each '
                           'exchange. This can lead to long start times')
    if args.live:
        raise ValueError('Live mode is not supported yet')
        if args.start_date != '':
            raise ValueError('Start date cannot be set in live mode')
        if args.start_balance != '':
            raise ValueError('Start balance cannot be set in live mode')
        pd_start_date = pandas.Timestamp.now()
        start_balances = None
    else:
        pd_start_date = pandas.to_datetime(args.start_date, utc=True)
        start_balances = json.loads(args.start_balances)
    pd_end_date = pandas.to_datetime(args.end_date, utc=True)
    try:
        pd_timedelta = pandas.Timedelta(
            Exchange.parse_timeframe(args.timedelta), unit='s')
    except (NotSupported, ValueError):
        raise ValueError('Timedelta is not valid')
    if pandas.isnull(pd_start_date):
        raise ValueError('Start date is not valid')
    if pandas.isnull(pd_end_date):
        raise ValueError('End date is not valid')

    return execute_algorithm(exchange_names=exchange_names,
                             symbols=symbols,
                             pd_start_date=pd_start_date,
                             pd_end_date=pd_end_date,
                             pd_timedelta=pd_timedelta,
                             data_dir=args.data_directory,
                             AlgorithmClass=AlgorithmClass,
                             args=args,
                             start_balances=start_balances)
