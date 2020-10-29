# btrccts
## BackTest and Run CryptoCurrency Trading Strategies

### [Install](#install) - [Usage](#usage) - [Manual](#manual) - [Development](#development)

This library provides an easy way to backtest trading strategies and run them live with ccxt.
The purpose of this library is to provide a framework and an backtest exchange with the same
interface than ccxt - nothing less and nothing more.
If you want an library to compute performance metrics out of trades/orders,
you need an additional library.

## Install

The easiest way to install the BTRCCTS library is to use a package manager:

- https://pypi.org/project/btrccts/

The python package hashes can be found in the `version_hashes.txt`.

You can also clone the repository, see [Development](development)

## Usage

For example algorithms see in [Examples](examples/)
```python
from btrccts import parse_params_and_execute_algorithm, AlgorithmBase


class Algorithm(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        # Here you can add additional arguments to the argparser
        argparser.add_argument('--pyramiding', default=1, type=int)

    def __init__(self, context, args):
        # Context is used to create exchanges or get the current time
        self._context = context
        self._args = args

        # This will create a kraken exchange instance
        # The interface in backtesting and live mode is identical to CCXT.
        # See: [CCXT](https://github.com/ccxt/ccxt/wiki)
        # In live mode, this will be a plain ccxt instance of the exchange
        # The exchange keys will be read from the config directory (see --help)
        # You can create sync or async versions of the exchange.
        # If ccxtpro is available in your python environment, the async
        # call will create a ccxtpro instance.
        self._kraken = context.create_exchange('kraken', async_ccxt=True)

        # You can access your own defined parameters
        print('Pyramiding:', args.pyramiding)

        # You can access predefined parameters like exchanges and symbols
        print('Exchanges:', args.exchanges)
        print('Symbols:', args.symbols)

    async def next_iteration(self):
        # This method is executed each time interval
        # This method can be async or a normal method.

        # This is the current context date:
        print('context date', self._context.date())

        # In live mode, markets are not loaded by the library
        # If you need access to the exchanges market object, you need
        # to load them first
        await self._kraken.load_markets()
        # Use the exchange to load OHLCV data
        ohlcv_len = 10
        ohlcv_offset = ohlcv_len * 60 * 1000
        ohlcv_start = int(self._context.date().value / 1000000 - ohlcv_offset)
        print(await self._kraken.fetch_ohlcv(
            'BTC/USD', '1m', ohlcv_start, ohlcv_len))

        # Use the exchange to create a market order
        self._order_id = self._kraken.create_order(
            type='market', side='buy', symbol='BTC/USD', amount=0.1)

        # If you want to stop the algorithm in context or live mode, you can
        # do this:
        self._context.stop('stop message')

    async def handle_exception(self, e):
        # This method is called, when next_iteration raises an exception, e.g.
        # because of an exchange error or a programming error.
        # If this method raises an exception, the algorith will stop with
        # reason EXCEPTION
        # This method can be async or a normal method.
        # If you are not in live mode, it is advicable to rethrow the
        # exception to fix the programming error.
        print(e)
        if not self._args.live:
            raise e

    async def exit(self, reason):
        # This method is called, when the algorithm exits and should be used
        # to cleanup (e.g. cancel open orders).
        # This method can be async or a normal method.
        # reason contains information on why the algorithm exits.
        # e.g. STOPPED, EXCEPTION, FINISHED
        print("Done", reason)
        self.closed_orders = await self._kraken.fetch_closed_orders()
        # Async versions of an exchange needs to be closed, because
        # btrccts will close the asyncio loop after the run.
        await self._kraken.close()


# This method parses commandline parameters (see --help)
# and runs the Algorithm according to the parameters
result = parse_params_and_execute_algorithm(Algorithm)
# The result is an instance of Algorithm, you can now use saved
# information. If you used a sync version of the exchange you can
# still use them. For async exchages the asyncio loop is already
# destroyed.
print(result.orders)
```

To run this algorithm, just execute the file with python.
e.g. `.venv/bin/python examples/algo_readme.py --start-date 2017-12-01 --end-date 2017-12-02 --timedelta 1h --exchanges kraken --symbols BTC/USD --start-balances '{"kraken": {"USD": 10000}}'`

If you dont want the function to parse commandline parameters for you, you can use
```python
from btrccts.run import execute_algorithm
execute_algorithm(...)
```


## Manual

### Data and directories

Run your algorithm with `--help` to see the path to your config and data directories.

The data directory contains the ohlcv data:
`data_directory/ohlcv/EXCHANGE/BASE/QUOTE.csv`
e.g.
`data_directory/ohlcv/binance/BTC/USD.csv`

Data files are in the following format (readable with `pandas.read_csv`)
```csv
,open,high,low,close,volume
2019-10-01 10:10:00+00:00,200,300,100,300,1000
2019-10-01 10:11:00+00:00,300,400,200,400,2000
2019-10-01 10:12:00+00:00,400,500,300,500,3000
```
The data files are not yet provided with this library. You have to provide them yourself.
The data file needs to cover the complete period (you want to run the bot) in 1 minute interval.
You can specify the period with `--start-date` and `--end-date`.


The config directory contains exchange keys.
e.g. `config_directory/binance.json`:
```json
{
    "apiKey": "key material",
    "secret": "secret stuff"
}
```
If an alias is provided (e.g. `--auth-aliases '{"kraken": "kraken_wma"}'`,
the file `config_directory/kraken_wma.json` is used.


### Differences between live and backtesting mode

- In backtesting mode the markets from the exchanges are loaded upon exchange creation.
This needs to be done, because market information is needed for order handling.
In live mode, the markets are not loaded via the library, because the library does not
know how you want to handle e.g. errors or reloading the market.


### How orders get filled

- Market order

Market orders are executed immediatly with a price a little worse than current low/high.
Since we only have ohlcv data, we cannot use the next data, because this would introduce
a look-ahead bias
Some other backtesting libraries would wait until the next round to fill market orders,
but this is not what is happening in the real world (executing market orders immediatly).

- Limit order

Limit orders are filled, when the price is reached. Limit orders get filled
all at once, there is no volume calculation yet. If your bot uses huge limit orders,
keep in mind that the behavior on the exchange can be a partiall fill and leaving the
order open until filled.


### When next round is initiated in live mode / How interval is handled in live mode

When the algorithm is started, it will immediatly execute `next_iteration`.
Now the library waits until the next time interval and executes `next_iteration`.
If the `next_iteration` call takes longer than the interval, `next_iteration` is
called immediatly again. If `next_iteration` takes longer than multiple intervals,
only the last interval is rescheduled.

## Development

Setup a virtualenv:

```shell
git clone git@github.com:btrccts/btrccts.git
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install -e . --no-deps
```

### Run tests

Install the dev dependencies:
```shell
.venv/bin/pip install -e .[dev]
```
Run the tests:
```shell
.venv/bin/python -m unittest tests/unit/tests.py
.venv/bin/python -m unittest tests/integration/tests.py
```

## Contact us

btrccts@gmail.com
