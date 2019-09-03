class BacktestExchangeBase:

    def __init__(self, config, backtest):
        super().__init__(config=config)
        self._backtest = backtest
