"""
Trend pair list filter
"""
import logging
from typing import Any, Dict
from datetime import datetime

import arrow

from freqtrade.exceptions import OperationalException
from freqtrade.misc import plural
from freqtrade.pairlist.IPairList import IPairList
from freqtrade.data.converter import ohlcv_to_dataframe
from pandas import DataFrame
import talib.abstract as ta


logger = logging.getLogger(__name__)


class TrendFilter(IPairList):

    # Symbols cache (dictionary of ticker symbol => { counter: int, timestamp: int, result: bool })
    _symbolsCache: Dict[str, Any] = {}

    def __init__(self, exchange, pairlistmanager,
                 config: Dict[str, Any], pairlistconfig: Dict[str, Any],
                 pairlist_pos: int) -> None:
        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

        self._trend = pairlistconfig.get('trend', "")

        if self._trend not in ["any", "bull", "bear", "sideways"]:
            raise OperationalException("TrendFilter requires trend to be any of: any, bull, bear, sideways")
        self._enabled = self._trend != "any"
        if self._enabled:
            if 'timeframe' not in self._config:
                raise OperationalException(
                    'TrendFilter can only work with timeframe defined. Please add the '
                    'timeframe key to your configuration (overwrites eventual strategy settings).')
            self._timeframe = self._config['timeframe']
            self._window_periods = pairlistconfig.get('window_periods', 100)
            self._smooth_ema = pairlistconfig.get('smooth_ema', 25)
            self._total_periods = self._window_periods + self._smooth_ema
            self._sideways_pct = pairlistconfig.get('sideways_pct', 10)
            self._refresh_period = pairlistconfig.get('refresh_period', 180)
            if self._timeframe[-1] == 's':
                self._seconds = int(self._timeframe[:-1])
                self._minutes = None
                self._hours = None
                self._days = None
            elif self._timeframe[-1] == 'm':
                self._seconds = None
                self._minutes = int(self._timeframe[:-1])
                self._hours = None
                self._days = None
            elif self._timeframe[-1] == 'h':
                self._seconds = None
                self._minutes = None
                self._hours = int(self._timeframe[:-1])
                self._days = None
            elif self._timeframe[-1] == 'd':
                self._seconds = None
                self._minutes = None
                self._hours = None
                self._days = int(self._timeframe[:-1])
            else:
                raise OperationalException(
                    'Timeframe not valid. Please provide a timeframe in seconds, minutes, hours or days.')

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        If no Pairlist requires tickers, an empty List is passed
        as tickers argument to filter_pairlist
        """
        return True

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages
        """
        return (f"{self.name} - Filtering pairs with {self._trend} trend.")

    def _validate_pair(self, ticker: dict) -> bool:
        """
        Validate trend for the ticker
        :param ticker: ticker dict as returned from ccxt.load_markets()
        :return: True if the pair can stay, False if it should be removed
        """

        # Check symbol in cache
        if ticker['symbol'] in self._symbolsCache:
            cache_data = self._symbolsCache[ticker['symbol']]
            if self._symbolsCache[ticker['symbol']]['last_refresh'] + self._refresh_period < datetime.now().timestamp():
                del self._symbolsCache[ticker['symbol']]
            else:
                return cache_data['result']

        self.log_on_refresh(logger.info, f"Filtering {ticker['symbol']} from whitelist, "
                                            f"check if in {self._trend} trend.")
        since_ms = None
        if self._seconds:
            since_ms = int(arrow.utcnow()
                        .floor('second')
                        .shift(seconds=-self._seconds*self._total_periods)
                        .float_timestamp) * 1000
        elif self._minutes:
            since_ms = int(arrow.utcnow()
                        .floor('minute')
                        .shift(minutes=-self._minutes*self._total_periods)
                        .float_timestamp) * 1000
        elif self._hours:
            since_ms = int(arrow.utcnow()
                        .floor('hour')
                        .shift(hours=-self._hours*self._total_periods)
                        .float_timestamp) * 1000
        elif self._days:
            since_ms = int(arrow.utcnow()
                        .floor('day')
                        .shift(days=-self._days*self._total_periods)
                        .float_timestamp) * 1000

        candles = self._exchange.get_historic_ohlcv(pair=ticker['symbol'],
                                                    timeframe=self._timeframe,
                                                    since_ms=since_ms)

        if candles is not None:
            # convert to dataframe
            dataframe = ohlcv_to_dataframe(candles, self._timeframe, pair=ticker['symbol'], drop_incomplete=True)
            # populate ema indicator
            dataframe['ema'] = ta.EMA(dataframe, timeperiod=self._smooth_ema, price='close')
            trend = self._get_ticker_trend(dataframe)
            self.log_on_refresh(logger.info, f"{ticker['symbol']} is in {trend} trend.")
            result = False
            if trend == self._trend:
                result = True
                # save data on cache
                cache_data = {
                    'last_refresh': int(datetime.now().timestamp()),
                    'result': result
                }
                self._symbolsCache[ticker['symbol']] = cache_data
            return result

        self.log_on_refresh(logger.info, f"Could not get data for {ticker['symbol']}.")
        return False

    def _get_ticker_trend(self, dataframe: DataFrame) -> str:
        """
        Caculate trend for the ticker
        :param ticker: ticker dict as returned from ccxt.load_markets()
        :return: name of the calculated trend ('bull', 'bear' or 'sideways')
        """
        w_dataframe = dataframe.iloc[-self._window_periods:]
        max_price = dataframe.iloc[-self._window_periods:]['high'].max()
        min_price = dataframe.iloc[-self._window_periods:]['low'].min()
        rel_max = max_price - min_price
        df = dataframe.iloc[-self._window_periods]
        start_rel = (dataframe.iloc[-self._window_periods]['ema'] - min_price) * 100 / rel_max
        end_rel = (dataframe.iloc[-1]['ema'] - min_price) * 100 / rel_max
        if start_rel > (end_rel + self._sideways_pct):
            return 'bear'    
        if start_rel < (end_rel - self._sideways_pct):
            return 'bull'
        return 'sideways'
