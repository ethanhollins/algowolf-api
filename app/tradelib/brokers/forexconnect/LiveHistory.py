import datetime
import threading
import calendar
import math
from typing import Callable
from enum import Enum

import pandas as pd
import numpy as np
from . import fxcorepy
from . import ForexConnect


class LiveHistoryCreator:
    """The class is intended for updating the price history with live prices."""
    def __init__(self,
                 timeframe,
                 history=None,
                 limit=300,
                 candle_open_price_mode=fxcorepy.O2GCandleOpenPriceMode.PREVIOUS_CLOSE):
        """ The constructor.
        
            Parameters
            ----------
            timeframe : 
                The price history timeframe.
            history : numpy.ndarray
                The price history obtained using the method ForexConnect.get_history.
            limit : 
                The maximum number of bars/ticks in the price history. The default value of the parameter is 300.
            candle_open_price_mode : 
                The candles open price mode that indicates how the open price is determined. See O2GCandleOpenPriceMode.
        
            Returns
            -------
            None
        
        """
        self.buffer = []
        self.limit = limit
        self._listeners = []
        self.history_lock = threading.Lock()
        self.buffer_lock = threading.Lock()
        self.last_ask_price = 0
        self.last_bid_price = 0
        self.last_volume = 0
        self._history = None
        self.history = history
        self.timeframe_unit, self.timeframe_size = ForexConnect.parse_timeframe(timeframe)
        self._candle_open_price_mode = candle_open_price_mode

    @property
    def candle_open_price_mode(self) -> fxcorepy.O2GCandleOpenPriceMode:
        return self._candle_open_price_mode

    @property
    def history(self):
        """ Gets or sets the updated price history.
        
            Returns
            -------
            pandas.DataFrame
        
        """
        return self._history

    @history.setter
    def history(self, history):
        if history is None:
            self._history = None
            return
        with self.history_lock:
            if isinstance(history, pd.DataFrame):
                self._history = history
            elif isinstance(history, np.ndarray):
                self._history = pd.DataFrame(data=history)
                self._history.set_index('Date', inplace=True)
            else:
                raise TypeError("Living history creator accept as history only object of type pandas. "
                                "DataFrame of numpy.ndarray. Received {item_type}".format(
                                         item_type=type(history)))
            if len(self._history.index) > self.limit:
                self._history = self._history.tail(self.limit)
            last_row = self._history.tail(1)
            if self.timeframe_unit != fxcorepy.O2GTimeFrameUnit.TICK:
                self.last_ask_price = last_row.AskClose.item()
                self.last_bid_price = last_row.BidClose.item()
                self.last_volume = last_row.Volume.item()

            while self._process_buffer(last_row):
                last_row = self._history.tail(1)

    def _process_buffer(self, last_row) -> bool:
        with self.buffer_lock:
            buffer = self.buffer.copy()
            self.buffer.clear()

        any_processed = False
        date = last_row.index
        for row in buffer:
            if date > row['Date']:
                continue
            self._add_or_update_internal(row)
            date = row['Date']
            any_processed = True
        return any_processed

    def add_or_update(self, row):
        """ Adds or updates ticks/bars.
        
            Parameters
            ----------
            row : O2GOfferRow
                An instance of O2GOfferRow.
        
            Returns
            -------
            None
        
        """
        dict_row = LiveHistoryCreator._convert_to_history_item(row)
        self.add_or_update_dict(dict_row)

    def add_or_update_dict(self, dict_row):
        """Reserved for future use."""
        if self._history is None:
            with self.buffer_lock:
                self.buffer.append(dict_row)
            return
        if not self.history_lock.acquire(True, 100):
            with self.buffer_lock:
                self.buffer.append(dict_row)
            return
        self._add_or_update_internal(dict_row)
        self.history_lock.release()

    def subscribe(self, on_add_bar_callback: Callable[
                     [pd.DataFrame], None
                 ]):
        """ Subscribes the function that is called when a bar/tick is added to or updated in the price history.
        
            Parameters
            ----------
            on_add_bar_callback : typing.Callable[[pandas.DataFrame], None]
                The function that is called when a bar/tick is added to or updated in the price history.
        
            Returns
            -------
            None
        
        """
        self._listeners.append(on_add_bar_callback)

    def unsubscribe(self, on_add_bar_callback: Callable[
                     [pd.DataFrame], None
                 ]):
        """ Unsubscribes the function that is called when a bar/tick is added to or updated in the price history.
        
            Parameters
            ----------
            on_add_bar_callback : typing.Callable[[pandas.DataFrame], None]
                The function that is called when a bar/tick is added to or updated in the price history.
        
            Returns
            -------
            None
        
        """
        try:
            self._listeners.remove(on_add_bar_callback)
        except ValueError:
            pass

    def _add_or_update_internal(self, row):
        current_unit = self.timeframe_unit
        if current_unit == fxcorepy.O2GTimeFrameUnit.TICK:
            item = pd.DataFrame([{'Date': row['Date'],
                                  'Bid': row['Bid'],
                                  'Ask': row['Ask']
                                  }]).set_index('Date')
            self._history = self._history.append(item, sort=True)
        else:
            timeframe = self._get_timeframe_by_time(row['Date'])
            if timeframe in self._history.index:
                last_row = self._history.loc[timeframe]
                if last_row.BidHigh.item() < row['Bid']:
                    self._history.at[timeframe, 'BidHigh'] = row['Bid']
                if last_row.AskHigh.item() < row['Ask']:
                    self._history.at[timeframe, 'AskHigh'] = row['Ask']
                if last_row.BidLow.item() > row['Bid']:
                    self._history.at[timeframe, 'BidLow'] = row['Bid']
                if last_row.AskLow.item() > row['Ask']:
                    self._history.at[timeframe, 'AskLow'] = row['Ask']
                self._history.at[timeframe, 'BidClose'] = row['Bid']
                self._history.at[timeframe, 'AskClose'] = row['Ask']
                if row['Volume'] > self.last_volume:
                    self._history.at[timeframe, 'Volume'] = last_row.Volume.item() + row['Volume'] - self.last_volume

            else:
                if len(self._history.index) == self.limit:
                    self._history = self._history.iloc[1:]

                ask_open = self.last_ask_price
                bid_open = self.last_bid_price

                if self._candle_open_price_mode == fxcorepy.O2GCandleOpenPriceMode.FIRST_TICK:
                    ask_open = row['Ask']
                    bid_open = row['Bid']

                item = pd.DataFrame([{'Date': np.datetime64(timeframe),
                                      'BidOpen': bid_open,
                                      'BidHigh': row['Bid'],
                                      'BidLow': row['Bid'],
                                      'BidClose': row['Bid'],
                                      'AskOpen': ask_open,
                                      'AskHigh': row['Ask'],
                                      'AskLow': row['Ask'],
                                      'AskClose': row['Ask'],
                                      'Volume': row['Volume'],
                                      }]).set_index('Date')
                self._history = self._history.append(item, sort=True)
                for callback in self._listeners:
                    callback(self._history)
            self.last_ask_price = row['Ask']
            self.last_bid_price = row['Bid']
            self.last_volume = row['Volume']

    @staticmethod
    def _convert_to_history_item(row):

        return {'Date': pd.to_datetime(row.time),
                'Bid': row.bid,
                'Ask': row.ask,
                'Volume': row.volume
                }

    def _get_timeframe_by_time(self, dt: datetime.datetime):
        current_unit = self.timeframe_unit
        current_size = self.timeframe_size

        step = 0
        if current_unit == fxcorepy.O2GTimeFrameUnit.MIN:
            step = datetime.timedelta(minutes=current_size)

        elif current_unit == fxcorepy.O2GTimeFrameUnit.HOUR:
            step = datetime.timedelta(hours=current_size)

        elif current_unit == fxcorepy.O2GTimeFrameUnit.DAY:
            step = datetime.timedelta(days=current_size)

        elif current_unit == fxcorepy.O2GTimeFrameUnit.WEEK:
            step = datetime.timedelta(weeks=current_size)

        elif current_unit == fxcorepy.O2GTimeFrameUnit.MONTH:
            month = (round(dt.month - 1 / current_size) * current_size) + 1
            return dt.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

        step = step.total_seconds()  # step in seconds
        timespamp = dt.timestamp()
        new_timespamp = math.floor(timespamp / step) * step
        return datetime.datetime.utcfromtimestamp(new_timespamp)