import appdirs
import argparse
import asyncio
import json
import logging
import numpy
import os
import pandas
from asyncio import events, tasks, coroutines
from ccxt.base.errors import NotSupported
from ccxt.base.exchange import Exchange
from enum import Enum, auto
from functools import partial
from btrccts.context import BacktestContext, LiveContext, StopException
from btrccts.exchange_backend import ExchangeBackend
from btrccts.timeframe import Timeframe

USER_CONFIG_DIR = appdirs.user_config_dir(__package__)
USER_DATA_DIR = appdirs.user_data_dir(__package__)
SLEEP_SECONDS = 1
HELP_EPILOG = """
Default config directory: {data}
Default data directory: {config}
""".format(config=USER_CONFIG_DIR, data=USER_DATA_DIR)


async def _run_a_or_sync(func, *args, **kwargs):
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return func(*args, **kwargs)


if hasattr(asyncio, 'run'):
    _run_async = asyncio.run
else:
    def _cancel_all_tasks(loop):
        if hasattr(asyncio, 'all_tasks'):
            all_tasks = asyncio.all_tasks
        else:
            all_tasks = asyncio.Task.all_tasks
        to_cancel = all_tasks(loop)
        if not to_cancel:
            return

        for task in to_cancel:
            task.cancel()

        loop.run_until_complete(
            tasks.gather(*to_cancel, loop=loop, return_exceptions=True))

        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                loop.call_exception_handler({
                    'message': 'unhandled exception during '
                               '_run_async() shutdown',
                    'exception': task.exception(),
                    'task': task,
                })

    def _run_async(main, *, debug=False):
        if events._get_running_loop() is not None:
            raise RuntimeError(
                "asyncio.run() cannot be called from a running event loop")

        if not coroutines.iscoroutine(main):
            raise ValueError("a coroutine was expected, got {!r}".format(main))

        loop = events.new_event_loop()
        try:
            events.set_event_loop(loop)
            loop.set_debug(debug)
            return loop.run_until_complete(main)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                events.set_event_loop(None)
                loop.close()


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
                symbols = []
                for base in os.listdir(exchange_path):
                    quote_path = os.path.join(exchange_path, base)
                    if os.path.isdir(quote_path):
                        for quote in os.listdir(quote_path):
                            if quote.endswith('.csv'):
                                symbols.append(os.path.join(base, quote[:-4]))
            except (FileNotFoundError, NotADirectoryError):
                raise FileNotFoundError(
                    'Cannot find ohlcv directory for exchange ({})'
                    .format(exchange_name))
        for symbol in symbols:
            file_path = os.path.join(exchange_path,
                                     '{}.csv'.format(symbol))
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


async def sleep_until(date):
    # Use a loop, so if the system clock changes, we dont sleep too long/short
    while True:
        now = pandas.Timestamp.now(tz='UTC')
        if date <= now:
            return
        sleep_sec = SLEEP_SECONDS
        diff = (date - now).value / 10**9
        if diff < SLEEP_SECONDS:
            sleep_sec = diff
        await asyncio.sleep(sleep_sec)


async def main_loop(timeframe, algorithm, live=False):
    logger = logging.getLogger(__package__)
    logger.info('Starting main_loop')
    while not timeframe.finished():
        try:
            try:
                await _run_a_or_sync(algorithm.next_iteration)
            except (SystemExit, KeyboardInterrupt, StopException,
                    asyncio.CancelledError) as e:
                logger.info('Stopped because of {}: {}'.format(
                    type(e).__name__, e))
                await _run_a_or_sync(algorithm.exit,
                                     reason=ExitReason.STOPPED)
                return algorithm
            except BaseException as e:
                logger.error('Error occured during next_iteration')
                logger.exception(e)
                try:
                    await _run_a_or_sync(algorithm.handle_exception, e)
                except BaseException as e:
                    logger.error(
                        'Exiting because of exception in handle_exception')
                    logger.exception(e)
                    await _run_a_or_sync(algorithm.exit,
                                         reason=ExitReason.EXCEPTION)
                    raise e
            timeframe.add_timedelta()
            if live:
                # We already added a timedelta.
                # If the algo took longer then timedelta,
                # we want to do the next round immediately
                timeframe.add_timedelta_until(pandas.Timestamp.now(tz='UTC'))
                next_date = timeframe.date()
                if next_date is not None:
                    await sleep_until(next_date)
        except (SystemExit, KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info('Stopped because of {}: {}'.format(
                type(e).__name__, e))
            await _run_a_or_sync(algorithm.exit, reason=ExitReason.STOPPED)
            return algorithm
    await _run_a_or_sync(algorithm.exit, reason=ExitReason.FINISHED)
    logger.info('Finished main_loop')
    return algorithm


def execute_algorithm(exchange_names, symbols, AlgorithmClass, args,
                      start_balances,
                      pd_start_date, pd_end_date, pd_interval,
                      live, auth_aliases,
                      data_dir=USER_DATA_DIR,
                      conf_dir=USER_CONFIG_DIR):
    timeframe = Timeframe(pd_start_date=pd_start_date,
                          pd_end_date=pd_end_date,
                          pd_interval=pd_interval)
    ohlcv_dir = os.path.join(data_dir, 'ohlcv')
    if live:
        context = LiveContext(timeframe=timeframe,
                              conf_dir=conf_dir,
                              auth_aliases=auth_aliases)
    else:
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

    async def func():
        algorithm = AlgorithmClass(context=context,
                                   args=args)
        return await main_loop(timeframe=timeframe,
                               algorithm=algorithm,
                               live=live)
    return _run_async(func())


def parse_params_and_execute_algorithm(AlgorithmClass):
    parser = argparse.ArgumentParser(
        epilog=HELP_EPILOG, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--start-date', default='',
                        help='Date to start backtesting, ignored in live mode')
    parser.add_argument('--end-date', default='2009-01-01',
                        help='Date to end backtesting')
    parser.add_argument('--interval', default='1m',
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
    parser.add_argument('--auth-aliases', default='{}',
                        help='Auth aliases for different exchange'
                             ' config files')
    parser.add_argument('--live', action='store_true',
                        help='Trade live on exchanges')
    parser.add_argument('--start-balances', default='{}',
                        help='Balance at start (json): '
                             '{"exchange": {"BTC": 3}}')
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
    try:
        pd_interval = pandas.Timedelta(
            Exchange.parse_timeframe(args.interval), unit='s')
    except (NotSupported, ValueError):
        raise ValueError('Interval is not valid')
    auth_aliases = {}
    if args.live:
        if args.start_date != '':
            raise ValueError('Start date cannot be set in live mode')
        if args.start_balances != '{}':
            raise ValueError('Start balance cannot be set in live mode')
        pd_start_date = pandas.Timestamp.now(tz='UTC').floor(pd_interval)
        start_balances = None
        auth_aliases = json.loads(args.auth_aliases)
    else:
        pd_start_date = pandas.to_datetime(args.start_date, utc=True)
        start_balances = json.loads(args.start_balances)
    pd_end_date = pandas.to_datetime(args.end_date, utc=True)
    if pandas.isnull(pd_start_date):
        raise ValueError('Start date is not valid')
    if pandas.isnull(pd_end_date):
        raise ValueError('End date is not valid')

    return execute_algorithm(exchange_names=exchange_names,
                             symbols=symbols,
                             pd_start_date=pd_start_date,
                             pd_end_date=pd_end_date,
                             pd_interval=pd_interval,
                             conf_dir=args.config_directory,
                             data_dir=args.data_directory,
                             AlgorithmClass=AlgorithmClass,
                             args=args,
                             auth_aliases=auth_aliases,
                             live=args.live,
                             start_balances=start_balances)
