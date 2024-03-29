import pandas as pd
import numpy as np
import datetime
import time
import traceback
import json
import zmq
from threading import Thread
from app import tradelib as tl
from copy import copy

class Chart(object):

	__slots__ = (
		'ctrl', 'broker', 'product', 'ask', 'mid', 'bid', 'volume', 'barReset',
		'lastTs', '_subscriptions', '_unsubscriptions', '_tick_queue'
	)
	def __init__(self, ctrl, broker, product, await_completion=False):
		print(f'[Chart] {broker.name} {product}')

		self.ctrl = ctrl
		self.broker = broker
		self.product = product

		self.ask = self._generate_period_dict()
		self.mid = self._generate_period_dict()
		self.bid = self._generate_period_dict()
		self.volume = { period:0 for period in self._generate_period_dict() }
		self.barReset = self._generate_period_dict()
		self.lastTs = self._generate_period_dict()
		self._subscriptions = self._generate_period_dict()
		self._unsubscriptions = []
		self._tick_queue = []

		self.start(await_completion)

		# Thread(target=self._mock_ticks).start()


	def start(self, await_completion):
		# Handle live connection
		self.broker._subscribe_chart_updates(self.product, self._on_chart_update)

		# Quickstart
		self._load_current_bars([tl.period.ONE_MINUTE])

		# Generate Tick
		self.ask[tl.period.TICK] = self.ask[tl.period.ONE_MINUTE][3]
		self.bid[tl.period.TICK] = self.bid[tl.period.ONE_MINUTE][3]
		self.mid[tl.period.TICK] = np.around(
			(self.ask[tl.period.TICK] + self.bid[tl.period.TICK])/2,
			decimals=5
		)
		self._subscriptions[tl.period.TICK] = {}

		# Finish other bars
		if not await_completion:
			Thread(
				target=self._load_current_bars,
				args=([period for period in self.ask if period != tl.period.TICK],)
			).start()
		else:
			self._load_current_bars([period for period in self.ask if period != tl.period.TICK])


	def getActivePeriods(self):
		return list(self.ask.keys())

	
	def getAllData(self):
		data = self.ctrl.redis_client.hget(self.broker, self.product)
		if data is None:
			data = {}
		else:
			data = json.loads(data)
		return data


	def setAllData(self, data):
		self.ctrl.redis_client.hset(self.broker, self.product, json.dumps(data))
		

	def getAsk(self, period):
		data = self.getAllData()
		if "asks" in data and period in data["asks"]:
			return data["asks"][period]
		else:
			return None


	def setAsk(self, period, value):
		data = self.getAllData()
		if not "asks" in data:
			data["asks"] = {}
		data["asks"][period] = value
		self.setAllData(data)


	def getMid(self, period):
		data = self.getAllData()
		if "mids" in data and period in data["mids"]:
			return data["mids"][period]
		else:
			return None

	
	def setMid(self, period, value):
		data = self.getAllData()
		if not "mids" in data:
			data["mids"] = {}
		data["mids"][period] = value
		self.setAllData(data)


	def getBid(self, period):
		data = self.getAllData()
		if "bids" in data and period in data["bids"]:
			return data["bids"][period]
		else:
			return None

	
	def setBid(self, period, value):
		data = self.getAllData()
		if not "bids" in data:
			data["bids"] = {}
		data["bids"][period] = value
		self.setAllData(data)


	def getVolume(self, period):
		data = self.getAllData()
		if "volume" in data and period in data["volume"]:
			return data["volume"][period]
		else:
			return None

	
	def setVolume(self, period, value):
		data = self.getAllData()
		if not "volume" in data:
			data["volume"] = {}
		data["volume"][period] = value
		self.setAllData(data)


	def getLastTs(self, period):
		data = self.getAllData()
		if "last_ts" in data and period in data["last_ts"]:
			return data["last_ts"][period]
		else:
			return None

	
	def setLastTs(self, period, value):
		data = self.getAllData()
		if not "last_ts" in data:
			data["last_ts"] = {}
		data["last_ts"][period] = value
		self.setAllData(data)


	def _generate_period_dict(self):
		PERIODS = [
			tl.period.ONE_MINUTE,
			tl.period.TWO_MINUTES, tl.period.THREE_MINUTES,
			tl.period.FIVE_MINUTES, tl.period.TEN_MINUTES,
			tl.period.FIFTEEN_MINUTES, tl.period.THIRTY_MINUTES,
			tl.period.ONE_HOUR, tl.period.TWO_HOURS, 
			tl.period.THREE_HOURS, tl.period.FOUR_HOURS, 
			tl.period.DAILY, tl.period.WEEKLY, 
			tl.period.MONTHLY
		]

		return {period: None for period in PERIODS if self.broker.isPeriodCompatible(period)}


	def _load_current_bars(self, periods):
		print(f'[_load_current_bars] {periods}')
		# Use _load_data to load current bar
		for period in periods:
			df = self._load_data(period, count=2, force_download=True)
			if df.size > 0:
				if not self.lastTs.get(period):
					self.lastTs[period] = int(df.index.values[-1])
					self.barReset[period] = False
					self.ask[period] = df.values[-1][:4]
					self.mid[period] = df.values[-1][4:8]
					self.bid[period] = df.values[-1][8:]

					self._subscriptions[period] = {}

				if period == tl.period.ONE_MINUTE:
					self.broker.save_data(df.iloc[:1], self.product, period)
			else:
				if not self.lastTs.get(period):
					self.lastTs[period] = np.nan
					self.barReset[period] = False
					self.ask[period] = [np.nan]*4
					self.mid[period] = [np.nan]*4
					self.bid[period] = [np.nan]*4

					self._subscriptions[period] = {}
					

	def _load_data(self, period, start=None, end=None, count=None, force_download=False):
		print(f'[_load_data] {period}, {start}, {end}, {count}')
		if self.broker.name == 'fxcm':
			df = self.broker._download_historical_data_broker(
				self.product, period, start=start, end=end,
				count=count, force_download=force_download
			)
		else:
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
			time.sleep(60)
			# if self.lastTs.get(period):
			result = [{
				'broker': 'fxcm',
				'product': self.product,
				'period': period,
				'bar_end': True,
				'timestamp': 1636241788,
				# 'timestamp': self.lastTs[period],
				'item': {
					# 'ask': self.ask[period].tolist(),
					# 'mid': self.mid[period].tolist(),
					# 'bid': self.bid[period].tolist()
					'ask': [1.15603, 1.15606, 1.15599, 1.15601],
					'mid': [1.15603, 1.15606, 1.15599, 1.15601],
					'bid': [1.15603, 1.15606, 1.15599, 1.15601]
				}
			}]
			self.handleTick(result)


	def handleTick(self, result):
		queue_id = self.broker.generateReference()
		self._tick_queue.append(queue_id)
		queue_idx = self._tick_queue.index(queue_id)
		while queue_idx != 0:
			queue_idx = self._tick_queue.index(queue_id)
			time.sleep(0.01)

		try:
			if self.ctrl.connection_id == 0:
				self.ctrl.zmq_dealer_socket.send_json(
					{ 
						"type": "ontick", 
						"message": {
							'broker': self.broker.name,
							'product': self.product,
							'period': 'all',
							'items': result
						}
					},
					zmq.NOBLOCK
				)

				self.ctrl.emit(
					'ontick', 
					{
						'broker': self.broker.name,
						'product': self.product,
						'period': 'all',
						'items': result
					}, 
					namespace='/admin'
				)

			for res in result:
				period = res.get('period')

				if self._subscriptions.get(period) is not None:
					for s in copy(list(self._subscriptions[period].keys())):
						try:
							for sub_id in copy(self._subscriptions[period][s]):
								func = self._subscriptions[period][s][sub_id]
								Thread(target=func, args=(res,)).start()
						except Exception as e:
							pass
				
				# self.ctrl.zmq_dealer_socket.send_json(
				# 	{ 
				# 		"type": "ontick", 
				# 		"message": res
				# 	},
				# 	zmq.NOBLOCK
				# )

				if self.ctrl.connection_id == 0:
					self.ctrl.emit(
						'ontick', res, 
						namespace='/admin'
					)

		except Exception:
			print(traceback.format_exc(), flush=True)

		finally:
			del self._tick_queue[queue_idx]


	def getNextTimestamp(self, period, ts):
		new_ts = ts + tl.period.getPeriodOffsetSeconds(period)
		dt = tl.convertTimestampToTime(new_ts)
		if tl.isWeekend(dt):
			new_ts = tl.convertTimeToTimestamp(tl.getWeekstartDate(dt))
		return new_ts


	def isNewBar(self, period, ts):
		# TODO: Add special cases for WEEK and MONTH periods
		return ts >= self.lastTs[period] + tl.period.getPeriodOffsetSeconds(period)
		# return ts >= self.getNextTimestamp(
		# 	period, 
		# 	self.lastTs[period] + tl.period.getPeriodOffsetSeconds(period)
		# )


	def getLatestTimestamp(self, period):
		return self.lastTs[period]


	def getLatestAsk(self, period):
		# return self.ask[period]
		return self.mid[period]


	def getLatestBid(self, period):
		# return self.bid[period]
		return self.mid[period]


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






