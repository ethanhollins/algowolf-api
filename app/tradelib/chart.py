import pandas as pd
import numpy as np
import datetime
import time
import traceback
from threading import Thread
from app import tradelib as tl

class Chart(object):

	__slots__ = (
		'ctrl', 'broker', 'product', 'ask', 'bid', 'barReset',
		'lastTs', '_subscriptions', '_unsubscriptions'
	)
	def __init__(self, ctrl, broker, product):
		self.ctrl = ctrl
		self.broker = broker
		self.product = product

		self.ask = self._generate_period_dict()
		self.bid = self._generate_period_dict()
		self.barReset = self._generate_period_dict()
		self.lastTs = self._generate_period_dict()
		self._subscriptions = self._generate_period_dict()
		self._unsubscriptions = []

		self.start()


	def start(self):
		# Handle live connection
		self.broker._subscribe_chart_updates(self.product, self._on_chart_update)

		# Quickstart
		self._load_current_bars([tl.period.ONE_MINUTE])

		# Generate Tick
		self.ask[tl.period.TICK] = self.ask[tl.period.ONE_MINUTE][3]
		self.bid[tl.period.TICK] = self.bid[tl.period.ONE_MINUTE][3]
		self._subscriptions[tl.period.TICK] = {}

		# Finish other bars
		Thread(
			target=self._load_current_bars,
			args=([period for period in self.ask if period != tl.period.TICK],)
		).start()

		# self.ctrl.continuousThreadHandler.addJob(self._mock_ticks)


	def getActivePeriods(self):
		return list(self.ask.keys())


	def _generate_period_dict(self):
		return {
			tl.period.ONE_MINUTE: None,
			tl.period.TWO_MINUTES: None,
			tl.period.THREE_MINUTES: None,
			tl.period.FIVE_MINUTES: None,
			tl.period.TEN_MINUTES: None,
			tl.period.FIFTEEN_MINUTES: None,
			tl.period.THIRTY_MINUTES: None,
			tl.period.ONE_HOUR: None,
			tl.period.TWO_HOURS: None,
			tl.period.THREE_HOURS: None,
			tl.period.FOUR_HOURS: None,
			tl.period.DAILY: None,
			tl.period.WEEKLY: None,
			tl.period.MONTHLY: None,
		}


	def _load_current_bars(self, periods):
		# Use _load_data to load current bar
		for period in periods:
			df = self._load_data(period, count=2, force_download=True)
			if not self.lastTs.get(period):
				self.lastTs[period] = int(df.index.values[-2])
				self.barReset[period] = False
				self.ask[period] = df.values[-1][:4]
				self.bid[period] = df.values[-1][4:]

				self._subscriptions[period] = {}

			if period == tl.period.ONE_MINUTE:
				self.broker.save_data(df.iloc[:1], self.product, period)


	def _load_data(self, period, start=None, end=None, count=None, force_download=False):
		df = self.broker._download_historical_data(
			self.product, period, start=start, end=end,
			count=count, force_download=force_download
		)
		df = df[~df.index.duplicated(keep='first')]
		return df


	def _on_chart_update(self, *args, **kwargs):
		self.broker.onChartUpdate(self, *args, **kwargs)
		self.handle_unsubscriptions()


	def _mock_ticks(self):
		period = tl.period.ONE_MINUTE
		while self.broker.is_running:
			time.sleep(10)
			if self.lastTs.get(period):
				result =[{
					'broker': 'ig',
					'product': self.product,
					'period': period,
					'bar_end': False,
					'timestamp': self.lastTs[period],
					'item': {
						'ask': self.ask[period].tolist(),
						'bid': self.bid[period].tolist()
					}
				}]
				self.handleTick(result)


	def handleTick(self, result):
		for res in result:
			period = res.get('period')

			if self._subscriptions.get(period) is not None:
				for s in self._subscriptions[period].keys():
					try:
						for sub_id in self._subscriptions[period][s]:
							func = self._subscriptions[period][s][sub_id]
							Thread(target=func, args=(res,)).start()
					except Exception as e:
						pass

			self.ctrl.emit(
				'ontick', res, 
				namespace='/admin'
			)

			# if res.get('bar_end'):
			# 	self.broker.handle_live_data_save(res)

	def getNextTimestamp(self, period, ts):
		new_ts = ts + tl.period.getPeriodOffsetSeconds(period)
		dt = tl.convertTimestampToTime(new_ts)
		if tl.isWeekend(dt):
			new_ts = tl.convertTimeToTimestamp(tl.getWeekstartDate(dt))
		return new_ts


	def isNewBar(self, period, ts):
		# TODO: Add special cases for WEEK and MONTH periods
		return ts >= self.getNextTimestamp(
			period, 
			self.lastTs[period] + tl.period.getPeriodOffsetSeconds(period)
		)


	def getLatestTimestamp(self, period):
		return self.lastTs[period]


	def getLatestAsk(self, period):
		return self.ask[period]


	def getLatestBid(self, period):
		return self.bid[period]


	def subscribe(self, period, strategy_id, sub_id, func):
		# Wait for chart to initialize
		while not isinstance(self._subscriptions.get(period), dict):
			pass

		if not self._subscriptions[period].get(strategy_id):
			self._subscriptions[period][strategy_id] = {sub_id: func}
		else:
			self._subscriptions[period][strategy_id][sub_id] = func

		return True


	def unsubscribe(self, period, strategy_id, sub_id):
		self._unsubscriptions.append((period, strategy_id, sub_id))


	def handle_unsubscriptions(self):
		for i in range(len(self._unsubscriptions)-1,-1,-1):
			unsub = self._unsubscriptions[i]
			try:
				sub = self._subscriptions[unsub[0]][unsub[1]]
				if unsub[2] in sub:
					del sub[unsub[2]]
			except ValueError:
				pass
			del self._unsubscriptions[i]


	def isChart(self, broker, product):
		return (
			broker.name == self.broker.name and
			product == self.product
		)






