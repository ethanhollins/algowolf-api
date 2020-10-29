import pandas as pd
import numpy as np
import os
import time
from copy import copy
from datetime import datetime, timedelta


class ChartItem(dict):

	def convertKey(self, key):
		if key == 'ONE_MINUTE':
			return tl.period.ONE_MINUTE
		elif key == 'TWO_MINUTES':
			return tl.period.TWO_MINUTES
		elif key == 'THREE_MINUTES':
			return tl.period.THREE_MINUTES
		elif key == 'FIVE_MINUTES':
			return tl.period.FIVE_MINUTES
		elif key == 'TEN_MINUTES':
			return tl.period.TEN_MINUTES
		elif key == 'FIFTEEN_MINUTES':
			return tl.period.FIFTEEN_MINUTES
		elif key == 'THIRTY_MINUTES':
			return tl.period.THIRTY_MINUTES
		elif key == 'ONE_HOUR':
			return tl.period.ONE_HOUR
		elif key == 'TWO_HOURS':
			return tl.period.TWO_HOURS
		elif key == 'THREE_HOURS':
			return tl.period.THREE_HOURS
		elif key == 'FOUR_HOURS':
			return tl.period.FOUR_HOURS
		elif key == 'DAILY':
			return tl.period.DAILY
		elif key == 'WEEKLY':
			return tl.period.WEEKLY
		elif key == 'MONTHLY':
			return tl.period.MONTHLY
		else:
			return key

	def __getattr__(self, key):
		return self[self.convertKey(key)]

	def __setattr__(self, key, value):
		self[self.convertKey(key)] = value


class IndicatorItem(dict):

	def __getattr__(self, key):
		return self[key]


class Chart(object):

	def __init__(self, strategy, product, periods=[], data_path='data/'):
		self.strategy = strategy
		self.product = product
		self.periods = copy(periods)
		
		self.timestamps = ChartItem({p:[] for p in self.periods})
		self.asks = ChartItem({p:[] for p in self.periods})
		self.mids = ChartItem({p:[] for p in self.periods})
		self.bids = ChartItem({p:[] for p in self.periods})
		self.indicators = IndicatorItem({})

		self._data_path = data_path
		self._idx = {p:0 for p in self.periods}
		self._data = {p:self._create_empty_df() for p in self.periods}
		self._next = {p:None for p in self.periods}
		self._subscriptions = {p:[] for p in self.periods}

		self._connections = []


	def _create_empty_df(self):
		columns = [
			'timestamp', 
			'ask_open', 'ask_high', 'ask_low', 'ask_close', 
			'bid_open', 'bid_high', 'bid_low', 'bid_close'
		]
		return pd.DataFrame(columns=columns).set_index('timestamp')


	def _set_idx(self, period, idx):
		self._idx[period] = idx
		for ind in self.indicators.values():
			if ind.period == period:
				ind._set_idx(idx)


	def _load_prices(self, period, start, end):
		# Load saved data from data_path
		df = pd.DataFrame()
		data_dir = os.path.join(self._data_path, '{}/{}/{}/'.format(self.strategy.getBroker().name, self.product, period))
		data_path = os.path.join(data_dir, '{}-{}.csv'.format(start.year, start.year+1))
		if os.path.exists(data_path):
			ask_keys = ['ask_open', 'ask_high', 'ask_low', 'ask_close']
			bid_keys = ['bid_open', 'bid_high', 'bid_low', 'bid_close']
			dtypes = {k:float for k in ask_keys + bid_keys}
			dtypes['timestamp'] = np.int32

			df = pd.read_csv(data_path, sep=' ', dtype=dtypes)

			ts_start = tl.utils.convertTimeToTimestamp(start)
			df = df.loc[df['timestamp'] >= ts_start]
			
			ts_end = tl.utils.convertTimeToTimestamp(end)
			df = df.loc[df['timestamp'] <= ts_end]

			df = df.set_index('timestamp')
			return df[ask_keys + bid_keys]

		return df


	def _save_prices(self, period, df):
		data_dir = os.path.join(self._data_path, '{}/{}/{}/'.format(self.strategy.getBroker().name, self.product, period))
		if not os.path.exists(data_dir):
			os.makedirs(data_dir)

		start = tl.utils.convertTimestampToTime(df.index.values[0])
		end = tl.utils.convertTimestampToTime(df.index.values[-1])

		for y in range(start.year, end.year+1):
			dt_start = datetime(year=y, month=1, day=1)
			dt_end = datetime(year=y+1, month=1, day=1)
			ts_start = tl.utils.convertTimeToTimestamp(dt_start)
			ts_end = tl.utils.convertTimeToTimestamp(dt_end)

			# Check if file exists
			data_path = os.path.join(data_dir, '{}-{}.csv'.format(y, y+1))
			if os.path.exists(data_path): continue

			# Get correct time range
			t_data = df.loc[(ts_start <= df.index) & (df.index < ts_end)]
			if t_data.size == 0: continue
			
			# Round all data to 5 decimal places
			t_data = t_data.round(pd.Series([5]*8, index=t_data.columns))
			# Save to CSV
			t_data.to_csv(data_path, sep=' ', header=True)


	def _quick_download_prices(self, period, start, end):
		ts_start = tl.utils.convertTimeToTimestamp(start)
		ts_end = tl.utils.convertTimeToTimestamp(end)
		df = self.strategy.getBroker()._download_historical_prices(self.product, period, start, end, None)

		df = df.loc[(ts_start <= df.index) & (df.index < ts_end)]

		# Return requested time range
		return df


	def _download_prices(self, period, start, end, save=True):
		ts_start = tl.utils.convertTimeToTimestamp(start)
		ts_end = tl.utils.convertTimeToTimestamp(end)

		start = start.replace(start.year,1,1,0,0,0,0)
		end = start.replace(start.year+1,1,1,0,0,0,0)

		df = self.strategy.getBroker()._download_historical_prices(self.product, period, start, end, 5000)

		if save:
			self._save_prices(period, df)
			
		# Return requested time range
		return df.loc[(ts_start <= df.index) & (df.index < ts_end)]


	def _handle_indicators(self, period):
		for ind in self.indicators.values():
			if ind.period == period:
				ind.calculate(self._data[period], self._idx[period])


	def _limit_indicators(self, period):
		for ind in self.indicators.values():
			if ind.period == period:
				ind.limit()


	def _on_tick(self, item):
		'''Handle on tick item'''

		queue_id = self.strategy.broker.generateReference()
		self.strategy.tick_queue.append(queue_id)
		queue_idx = self.strategy.tick_queue.index(queue_id)
		while queue_idx != 0 or self.strategy.getBroker().state != State.LIVE:
			queue_idx = self.strategy.tick_queue.index(queue_id)
			time.sleep(0.01)

		# Update current ask/bid prices
		ohlc = item['item']['ask'] + item['item']['bid']

		last_ts = self._data[item['period']].index.values[-1]

		if item['timestamp'] < last_ts - tl.period.getPeriodOffsetSeconds(item['period']):
			# Skip tick
			del self.strategy.tick_queue[queue_idx]
			return

		elif not item['bar_end'] and item['timestamp'] >= last_ts:
			new_ts = item['timestamp'] + tl.period.getPeriodOffsetSeconds(item['period'])
			self._idx[item['period']] += 1
			self._data[item['period']].loc[new_ts] = ohlc

		else:
			self._data[item['period']].iloc[-1] = ohlc

		# Handle Indicators
		self._handle_indicators(item['period'])

		# Limit Data
		self._data[item['period']] = self._data[item['period']].iloc[-1000:]
		self._idx[item['period']] = self._data[item['period']].shape[0]-1
		self._limit_indicators(item['period'])

		idx = self._idx[item['period']]
		self.timestamps[item['period']] = self._data[item['period']].index.values[:idx+1][::-1]
		self.asks[item['period']] = self._data[item['period']].values[:idx+1,:4][::-1]
		self.bids[item['period']] = self._data[item['period']].values[:idx+1,4:][::-1]

		# Send to subscribed functions
		if self.strategy.getBroker().isLive() and item['period'] in self._subscriptions:
			# self.strategy.getBroker()._check_stoploss(item['product'], round(time.time()), ohlc)
			# self.strategy.getBroker()._check_takeprofit(item['product'], round(time.time()), ohlc)

			tick = BrokerItem({
				'chart': self, 
				'timestamp': item['timestamp'],
				'period': item['period'], 
				'ask': item['item']['ask'],
				'bid': item['item']['bid'],
				'bar_end': item['bar_end']
			})
			self.strategy.setTick(tick)

			for func in self._subscriptions[item['period']]:
				func(tick)

		del self.strategy.tick_queue[queue_idx]
		

	def _set_data(self, df, period, append=False):
		'''Store data in memory'''
		if df.size == 0:
			return
		# if tl.utils.isCurrentBar(period, df.index.values[-1]):
		# 	self._next[period] = df.values[-1]
		# 	df.drop(df.tail(1).index, inplace=True)

		if append:
			# Concatenate with current data if append is true
			self._data[period] = pd.concat((self._data[period], df)).sort_index()
			# Remove duplicates
			self._data[period] = self._data[period][~self._data[period].index.duplicated(keep='first')]
			# Round to 5 decimal places
			self._data[period] = self._data[period].round(pd.Series([5]*8, index=self._data[period].columns))

		else:
			# Set df to data period
			self._data[period] = df
			# Round to 5 decimal places
			self._data[period] = self._data[period].round(pd.Series([5]*8, index=self._data[period].columns))
			# Reset idx
			self._idx[period] = 0

		# Handle Indicators
		self._handle_indicators(period)


	def getNextTimestamp(self, period, ts, now=None):
		new_ts = ts + tl.period.getPeriodOffsetSeconds(period)
		dt = tl.convertTimestampToTime(new_ts)
		if tl.isWeekend(dt):
			new_ts = tl.convertTimeToTimestamp(tl.getWeekstartDate(dt))

		if now is not None:
			while new_ts + tl.period.getPeriodOffsetSeconds(period) <= now:
				new_ts += tl.period.getPeriodOffsetSeconds(period)
				dt = tl.convertTimestampToTime(new_ts)
				if tl.isWeekend(dt):
					new_ts = tl.convertTimeToTimestamp(tl.getWeekstartDate(dt))
			
		return new_ts


	def prepareLive(self):
		for period in self.periods:
			self._idx[period] = self._data[period].index.size-1


	def addPeriods(self, *periods):
		for period in periods:
			if period not in self.periods:
				self.periods.append(period)
				self._idx[period] = 0
				self._data[period] = self._create_empty_df()
				self._next[period] = None
				self._subscriptions[period] = []


	def quickDownload(self, period, start, end):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')
		
		df = self._quick_download_prices(period, start, end)
		df = df.dropna()

		# Set data to chart data store
		self._set_data(df, period)
		return df


	def getPrices(self, period, start=None, end=None, count=None, 
					append=False, save=True, download=True):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')

		now_ts = tl.utils.convertTimeToTimestamp(datetime.utcnow())
		if not count:
			if not start:
				start = tl.TS_START_DATE
			if not end:
				end = datetime.utcnow()

		else:
			if start:
				end = tl.utils.getCountDate(period, count, start=start)
			elif end:
				start = tl.utils.getCountDate(period, count, end=end)
			else:
				start = tl.utils.getCountDate(period, count)
				end = datetime.utcnow()

		frags = []
		for y in range(start.year, end.year+1):
			if y == start.year:
				t_start = start
			else:
				t_start = datetime(y,1,1)

			temp_data = self._load_prices(period, t_start, end)
			# If no loadable data, download
			if download:
				if temp_data.size == 0:
					temp_data = self._download_prices(
						period, t_start, end, 
						save=save
					)

				# If current year and data not up to date, do quick download
				elif (y == datetime.utcnow().year and
					not tl.utils.isCurrentBar(period, temp_data.index.values[-1], off=2)):
					# Get start date at end of load data
					t_start = tl.utils.convertTimestampToTime(temp_data.index.values[-1])
					
					# Concatenate load data with latest download data
					temp_data = pd.concat((
						temp_data,
						self._quick_download_prices(
							period, t_start, end
						)
					))

					# Remove duplicates
					temp_data = temp_data[~temp_data.index.duplicated(keep='first')]

					# Save data
					if save:
						self._save_prices(period, temp_data)

			frags.append(temp_data)

		if len(frags) > 0:
			df = pd.concat(frags)
		else:
			df = df.DataFrame()

		self._set_data(df, period)

		return df


	def connectAll(self):
		for period in self.periods:
			# Check if no existing connection exists
			if not period in self._connections:	
				# Emit socket ontick subscribe message
				self.strategy.getBroker().sio.emit(
					'subscribe',
					{
						'strategy_id': self.strategy.getBroker().strategyId,
						'field': 'ontick',
						'items': [{
							'broker': self.strategy.getBroker().name,
							'product': self.product,
							'period': period
						}]
					},
					namespace='/user'
				)
				self._connections.append(period)


	def subscribe(self, period, func):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')

		self._subscriptions[period].append(func)


	def unsubscribe(self, period, func):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')

		idx = self._subscriptions[period].index(func)
		del self._subscriptions[period][idx]


	def addIndicator(self, name, period, ind):
		ind.setPeriod(period)
		if name not in self.indicators:
			self.indicators[name] = ind
		return self.indicators[name]


	def getIndicator(self, name):
		return self.indicators.get(name)


	def deleteIndicator(self, name):
		if name in self.indicators:
			del self.indicators[name]


	def getLowestPeriod(self):
		min_period = None
		for period in self.periods:
			period_off = tl.period.getPeriodOffsetSeconds(period)
			if (not min_period or 
					period_off < tl.period.getPeriodOffsetSeconds(min_period)):
				min_period = period
		return min_period


	def getTsOffset(self, period, ts):
		search = np.where(self.ts == ts)[0]
		if len(search): return search[0]
		else: return None


	def getTimestamp(self, period):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')
		
		return self._data[period].index.values[self._idx[period]]


	def getOHLC(self, period, offset, amount):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')
		
		return self._data[period].values[
			(self._idx[period]+1)-offset-amount:(self._idx[period]+1)-offset
		]


	def getAskOHLC(self, period, offset, amount):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')
		
		return self._data[period].values[
			(self._idx[period]+1)-offset-amount:(self._idx[period]+1)-offset
		][:4]


	def getBidOHLC(self, period, offset, amount):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')
		
		return self._data[period].values[
			(self._idx[period]+1)-offset-amount:(self._idx[period]+1)-offset
		][4:]


	def getLastOHLC(self, period):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')
		
		return self._data[period].values[self._idx[period]]


	def getLastAskOHLC(self, period):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')

		return self._data[period].values[self._idx[period]][:4]


	def getLastBidOHLC(self, period):
		if not period in self.periods:
			raise TradelibException('Period not found in chart.')

		return self._data[period].values[self._idx[period]][4:]


	def isChart(self, broker, product):
		return (
			broker.name == self.strategy.getBroker().name and
			product == self.product
		)


'''
Imports
'''

from app import pythonsdk as tl
from .broker import State, BrokerItem
from .error import TradelibException


