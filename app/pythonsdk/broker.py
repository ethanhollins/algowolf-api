import numpy as np
import pandas as pd
import datetime
import random
import string
import requests
import time
import json
import asyncio
import threading
import sys
import shortuuid
import traceback
from copy import copy
from enum import Enum
from app import pythonsdk as tl


'''
Broker Names
'''

BACKTEST_NAME = 'backtest'
IG_NAME = 'ig'
OANDA_NAME = 'oanda'
PAPERTRADER_NAME = 'papertrader'

def get_list():
	return [
		OANDA_NAME,
		IG_NAME,
	]

class State(Enum):
	IDLE = 0
	BACKTEST = 1
	BACKTEST_AND_RUN = 2
	LIVE = 3
	STOPPED = 4


class BacktestMode(Enum):
	RUN = 'run'
	STEP = 'step'


class BrokerItem(dict):

	def __getattr__(self, key):
		return self[key]

	def __setattr__(self, key, value):
		self[key] = value


from .app import App

URI = 'http://127.0.0.1'
PORT = 5000

'''
Parent Broker Class
'''
class Broker(object):

	# __slots__ = (
	# 	'name', 'controller', 'charts', 'positions', 'closed_positions', 'orders', 
	# 	'on_tick', 'on_new_bar', 'on_trade', 'on_stop_loss', 'on_take_profit'
	# )
	def __init__(self, strategy, api, strategy_id=None, broker_id=None, data_path='data/'):
		self.strategy = strategy
		self.api = api
		self.strategyId = strategy_id
		self.brokerId = broker_id

		self.name = None
		self.brokerId = None
		self.state = State.IDLE
		self.backtester = None
		self.isUploadBacktest = False
		self.isClearBacktestPositions = False
		self.isClearBacktestOrders = False
		self.isClearBacktestTrades = False
		self._start_from = None
		self._data_path = data_path
		self._app = None

		# Containers
		self.accounts = ['ACCOUNT_1']
		self.charts = []
		self.positions = []
		self.orders = []
		self.ontrade_subs = []
		self.handled = {}


	'''
	Run Utilities
		- Functions that start backtest or live runs
	'''

	# TODO: DO TOKENS INSTEAD and VALIDATION
	def run(self, broker_id=None):
		if self.brokerId is None:
			self.brokerId = broker_id

		self.setName(self.api.name)
		self.brokerId = self.api.brokerId
		self.accounts = self.api.getAccounts()

		# Subscribe all charts
		self._chart_subs = {}
		self._subscribe_charts(self.charts)

		# Subscribe on trade
		self._on_trade_subs = []
		self._subscribe_on_trade()

		# If `_start_from` set, run `_backtest_and_run`
		if self._start_from is not None:
			self._backtest_and_run(self._start_from, quick_download=True)
		else:
			end = datetime.datetime.utcnow()
			for chart in self.charts:
				for period in chart.periods:
					start = tl.utils.getCountDate(period, 1000, end=end)
					self._collect_data(start, end, download=False, quick_download=True)

		self.updateAllPositions()
		self.updateAllOrders()

		self._prepare_for_live()

		self.state = State.LIVE


	def stop(self):
		print('STOPPED')
		self.state = State.STOPPED

		for product in self._chart_subs:
			api_chart = self.api.getChart(product)
			for period in self._chart_subs[product]:
				if period in self._chart_subs[api_chart.product]:
					for sub_id in self._chart_subs[api_chart.product][period]:
						api_chart.unsubscribe(period, self.brokerId, sub_id)

		for sub_id in self._on_trade_subs:
			self.api.unsubscribeOnTrade(sub_id)


	def startFrom(self, dt):
		if tl.utils.isOffsetAware(dt):
			self._start_from = tl.utils.convertTimezone(dt, 'UTC')
		else:
			self._start_from = tl.utils.setTimezone(dt, 'UTC')


	def uploadBacktest(self, is_upload=True):
		self.isUploadBacktest = is_upload


	def setClearBacktestPositions(self, is_clear=True):
		self.isClearBacktestPositions = is_clear


	def setClearBacktestOrders(self, is_clear=True):
		self.isClearBacktestOrders = is_clear


	def setClearBacktestTrades(self, is_clear=True):
		self.isClearBacktestTrades = is_clear


	def _clear_backtest_positions(self):
		for i in range(len(self.positions)-1,-1,-1):
			pos = self.positions[i]
			if pos.isBacktest():
				del self.positions[i]


	def _clear_backtest_orders(self):
		for i in range(len(self.orders)):
			order = self.orders[i]
			if order.isBacktest():
				del self.orders[i]


	def _clear_backtest_trades(self):
		self._clear_backtest_positions()
		self._clear_backtest_orders()


	def _perform_backtest(self, start, end, mode=BacktestMode.RUN, download=True, quick_download=False):
		# Collect relevant data
		self._collect_data(start, end, download=download, quick_download=quick_download)

		# Run backtest
		self.backtester.performBacktest(mode.value, start=start, end=end)


	def _generate_backtest(self, start, end):
		return {
			'transactions': self.backtester.result,
			'properties': {
				'start': tl.convertTimeToTimestamp(start),
				'end': tl.convertTimeToTimestamp(end)
			}
		}


	def backtest(self, start, end, mode=BacktestMode.RUN, upload=False, download=True, quick_download=False):
		self.state = State.BACKTEST

		self._perform_backtest(start, end, mode=mode, download=download, quick_download=quick_download)

		backtest_id = self.api.userAccount.uploadBacktest(self.strategyId, self._generate_backtest(start, end))

		self.state = State.LIVE

		return backtest_id


	def _backtest_and_run(self, start, quick_download=False):
		self.state = State.BACKTEST_AND_RUN
		# Collect relevant data and connect to live broker
		end = datetime.datetime.utcnow()
		self._perform_backtest(start, end, quick_download=quick_download)


		if self.isUploadBacktest:

			# Update GUI Drawings
			# for layer in self.backtester.drawings:
			# 	threading.Thread(
			# 		target=self.api.userAccount.createDrawings,
			# 		args=(self.strategyId, layer, self.backtester.drawings[layer])
			# 	).start()
			pass

		# Clear backtest trades
		if self.isClearBacktestPositions:
			self._clear_backtest_positions()

		if self.isClearBacktestOrders:
			self._clear_backtest_orders()

		if self.isClearBacktestTrades:
			self._clear_backtest_trades()

		# Update positions/orders
		# self.updateAllPositions()
		# self.updateAllOrders()

		self.state = State.LIVE


	def isBacktest(self):
		return self.state == State.BACKTEST


	def isBacktestAndRun(self):
		return self.state == State.BACKTEST_AND_RUN


	def isLive(self):
		return self.state == State.LIVE


	def _collect_data(self, start, end, download=True, quick_download=False):
		for chart in self.charts:
			for period in chart._subscriptions:
				if quick_download:
					chart.quickDownload(
						period, 
						tl.utils.getCountDate(period, 1000, end=start), end
					)
				else:
					chart.getPrices(
						period, 
						start=tl.utils.getCountDate(period, 1000, end=start), 
						end=end,
						download=download
					)

	'''
	Utilities
	'''
	def generateReference(self):
		return shortuuid.uuid()

	def setApp(self, app):
		self._app = app

	def setName(self, name):
		self.name = name
		if self.name == OANDA_NAME:
			self.backtester = tl.OandaBacktester(self)
		elif self.name == IG_NAME:
			self.backtester = tl.IGBacktester(self)


	def _subscribe_charts(self, charts):
		for chart in charts:
			api_chart = self.api.getChart(chart.product)

			if chart.product not in self._chart_subs:
				self._chart_subs[chart.product] = {}

			for period in chart.periods:
				if period not in self._chart_subs[chart.product]:
					self._chart_subs[chart.product][period] = []

				ref_id = self.generateReference()
				self._chart_subs[chart.product][period].append(ref_id)
				api_chart.subscribe(
					period, self.brokerId, 
					ref_id, self._stream_ontick
				)


	def _subscribe_on_trade(self):
		ref_id = self.generateReference()
		self._on_trade_subs.append(ref_id)
		self.api.subscribeOnTrade(self._stream_ontrade, ref_id)


	def _prepare_for_live(self):
		for chart in self.charts:
			chart.prepareLive()
			period = chart.getLowestPeriod()
			if period is not None:
				chart.subscribe(period, self._handle_tick_checks)


	def _handle_tick_checks(self, item):
		product = item.chart.product
		timestamp = int(item.timestamp)
		ohlc = np.array([item.ask[3]]*4 + [item.bid[3]]*4, dtype=np.float64)

		self.backtester.handleOrders(product, timestamp, ohlc)
		self.backtester.handleStopLoss(product, timestamp, ohlc)
		self.backtester.handleTakeProfit(product, timestamp, ohlc)


	def _create_chart(self, product, *periods):
		chart = None
		if isinstance(self._app, App):
			chart = self._app.getChart(self, product)
			if chart is None:
				chart = tl.Chart(self.strategy, product, data_path=self._data_path)
				self._app.addChart(chart)
		else:
			chart = tl.Chart(self.strategy, product, data_path=self._data_path)

		chart.addPeriods(*periods)
		self.charts.append(chart)
		return chart

	def getAllCharts(self):
		return self.charts

	def getChart(self, product, *periods):
		for chart in self.charts:
			if chart.isChart(self, product):
				chart.addPeriods(*periods)
				return chart

		return self._create_chart(product, *periods)


	def getApiChart(self, product):
		return self.api.getChart(product)


	def chartExists(self, product):
		for chart in self.charts:
			if chart.isChart(self, product):
				return True

		return False

	def getBrokerAsk(self, product):
		chart = self.getApiChart(product)
		return chart.getLatestAsk(tl.period.TICK)

	def getBrokerBid(self, product):
		chart = self.getApiChart(product)
		return chart.getLatestBid(tl.period.TICK)

	def getAsk(self, product):
		if self.chartExists(product):
			chart = self.getChart(product)
			period = chart.getLowestPeriod()
			if period is not None:
				return chart.getLastAskOHLC(period)[3]
			else:
				raise tl.error.BrokerException(f'No {product} data found.')
		else:
			raise tl.error.BrokerException(f'Chart {product} doesn\'t exist.')	

	def getBid(self, product):
		if self.chartExists(product):
			chart = self.getChart(product)
			period = chart.getLowestPeriod()
			if period is not None:
				return chart.getLastBidOHLC(period)[3]
			else:
				raise tl.error.BrokerException(f'No {product} data found.')
		else:
			raise tl.error.BrokerException(f'Chart {product} doesn\'t exist.')

	def getTimestamp(self, product, period):
		return int(self.getChart(product).getTimestamp(period))

	def updateAllPositions(self):
		self.positions = (
			[pos for pos in self.positions if pos.isBacktest()] +
			[tl.position.Position.fromDict(self, pos) for pos in self.api.getAllPositions() if pos.account_id in self.accounts]
		)

	def getAllPositions(self, account_id=None):
		return [
			pos for pos in self.positions 
			if not account_id or pos.account_id == account_id
		]

	def getPositionByID(self, order_id):
		for pos in self.getAllPositions():
			if pos.order_id == order_id:
				return pos
		return None

	def updateAllOrders(self):
		self.orders = (
			[order for order in self.orders if order.isBacktest()] +
			[tl.order.Order.fromDict(self, order) for order in self.api.getAllOrders() if order.account_id in self.accounts]
		)

	def getAllOrders(self, account_id=None):
		return [
			order for order in self.orders 
			if not account_id or order.account_id == account_id
		]

	def getOrderByID(self, order_id):
		for order in self.orders:
			if order.order_id == order_id:
				return order
		return None


	'''
	Account Utilities
		- All functions access brokerage directly
	'''

	def updateAccountInfo(self):
		info = self.api.getAccountInfo(accounts, override=True)

	# Public
	def getAccounts(self):
		return self.accounts

	def getAccountInfo(self, accounts=[]):
		if self.isLive():
			return self.api.getAccountInfo(accounts, override=True)
		else:
			result = {}
			for account in accounts:
				result[account] = {
					'currency': 'AUD',
					'balance': 10000,
					'pl': 0,
					'margin': 0,
					'available': 10000
				}
			return result

	'''
	Dealing Utilities
		- All functions access brokerage directly
	'''

	# Broker Functions

	# TODO: ORDERS
	def buy(self,
		product, lotsize, accounts,
		order_type=tl.MARKET_ORDER,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		result = []
		if not self.isLive():
			if order_type == tl.MARKET_ORDER:
				for account_id in accounts:
					result.append(self.backtester.createPosition(
						product, lotsize, tl.LONG,
						account_id, entry_range, entry_price,
						sl_range, tp_range, sl_price, tp_price
					))

			elif order_type == tl.LIMIT_ORDER or order_type == tl.STOP_ORDER:
				for account_id in accounts:
					result.append(self.backtester.createOrder(
						product, lotsize, tl.LONG, account_id,
						order_type, entry_range, entry_price,
						sl_range, tp_range, sl_price, tp_price
					))

			else:
				raise tl.error.BrokerException('Unrecognisable order type specified.')



		else:
			res = {}
			for account_id in accounts:
				res.update(self.api.buy(
					product, lotsize, account_id, order_type=order_type,
					entry_range=entry_range, entry_price=entry_price,
					sl_range=sl_range, tp_range=tp_range,
					sl_price=sl_price, tp_price=tp_price,
					override=True
				))

			for ref_id, item in res.items():
				if item.get('accepted'):
					func = self._get_trade_handler(item.get('type'))
					wait_result = self._wait(ref_id, func, (ref_id, item))
					if wait_result not in result:
						result.append(wait_result)

		return result

	def sell(self,
		product, lotsize, accounts,
		order_type=tl.MARKET_ORDER,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if not self.isLive():
			result = []
			if order_type == tl.MARKET_ORDER:
				for account_id in accounts:
					result.append(self.backtester.createPosition(
						product, lotsize, tl.SHORT,
						account_id, entry_range, entry_price,
						sl_range, tp_range, sl_price, tp_price
					))

			elif order_type == tl.LIMIT_ORDER or order_type == tl.STOP_ORDER:
				for account_id in accounts:
					result.append(self.backtester.createOrder(
						product, lotsize, tl.SHORT, account_id,
						order_type, entry_range, entry_price,
						sl_range, tp_range, sl_price, tp_price
					))

			else:
				raise tl.error.BrokerException('Unrecognisable order type specified.')

			return result

		else:
			res = {}
			for account_id in accounts:
				res.update(self.api.sell(
					product, lotsize, account_id, order_type=order_type,
					entry_range=entry_range, entry_price=entry_price,
					sl_range=sl_range, tp_range=tp_range,
					sl_price=sl_price, tp_price=tp_price,
					override=True
				))

			result = []
			for ref_id, item in res.items():
				if item.get('accepted'):
					func = self._get_trade_handler(item.get('type'))
					wait_result = self._wait(ref_id, func, (ref_id, item))
					if wait_result not in result:
						result.append(wait_result)

			return result


	def stopAndReverse(self,
		product, lotsize, accounts,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if len(self.positions) > 0:
			direction = self.positions[-1].direction
			self.closeAllPositions()
		else:
			raise tl.error.OrderException('Must be in position to stop and reverse.')

		if direction == tl.LONG:
			res = self.sell(
				product, lotsize, accounts=accounts,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)
		else:
			res = self.buy(
				product, lotsize, accounts=accounts,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		return res

	def marketOrder(self,
		product, lotsize, direction, accounts,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if direction == tl.LONG:
			res = self.buy(
				product, lotsize, accounts=accounts,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)
		else:
			res = self.sell(
				product, lotsize, accounts=accounts,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		return res

	def stopOrder(self,
		product, lotsize, direction, accounts,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if direction == tl.LONG:
			res = self.buy(
				product, lotsize, accounts=accounts,
				order_type=tl.STOP_ORDER,
				entry_range=entry_range, entry_price=entry_price,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)
		else:
			res = self.sell(
				product, lotsize, accounts=accounts,
				order_type=tl.STOP_ORDER,
				entry_range=entry_range, entry_price=entry_price,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		return res

	def limitOrder(self,
		product, lotsize, direction, accounts,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if direction == tl.LONG:
			res = self.buy(
				product, lotsize, accounts=accounts,
				order_type=tl.LIMIT_ORDER,
				entry_range=entry_range, entry_price=entry_price,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)
		else:
			res = self.sell(
				product, lotsize, accounts=accounts,
				order_type=tl.LIMIT_ORDER,
				entry_range=entry_range, entry_price=entry_price,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		return res

	def closeAllPositions(self, positions=None):
		if positions is None:
			positions = self.getAllPositions()
		positions = copy(positions)

		result = []

		for pos in positions:
			pos.close()
			result.append(pos)
		
		return result


	def cancelAllOrders(orders=None):
		if not orders:
			orders = self.getAllOrders()
		orders = copy(orders)

		result = []
		for order in orders:
			order.cancel()
			result.append(order)

		return result

	'''
	Data Utilities
	'''

	def _download_historical_prices(self, product, period, start, end, count):

		if tl.utils.isOffsetAware(start):
			start = tl.utils.convertTimezone(start, 'UTC')
		else:
			start = tl.utils.setTimezone(start, 'UTC')
		if tl.utils.isOffsetAware(end):
			end = tl.utils.convertTimezone(end, 'UTC')
		else:
			end = tl.utils.setTimezone(end, 'UTC')

		broker = self.api.ctrl.brokers.getBroker(self.name)
		last_date = start
		data = None
		while not self._is_last_candle_found(period, last_date, end, 1):
			result = broker._download_historical_data(
				product, period, start=last_date, end=end, count=count
			)
			if result is not None:
				data = pd.concat((data, result))
				last_date = tl.utils.convertTimestampToTime(result.index.values[-1])
			else:
				return data[~data.index.duplicated(keep='first')]

		return data[~data.index.duplicated(keep='first')]

	def _is_last_candle_found(self, period, start_dt, end_dt, count):
		utcnow = tl.utils.setTimezone(datetime.datetime.utcnow(), 'UTC')
		if tl.utils.isWeekend(utcnow):
			utcnow = tl.utils.getWeekendDate(utcnow)
		else:
			utcnow -= datetime.timedelta(seconds=tl.period.getPeriodOffsetSeconds(period))

		if tl.utils.isWeekend(end_dt):
			end_dt = tl.utils.getWeekendDate(end_dt)

		if period == tl.period.ONE_MINUTE:
			new_dt = start_dt + datetime.timedelta(minutes=count)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.TWO_MINUTES:
			new_dt = start_dt + datetime.timedelta(minutes=count*2)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.THREE_MINUTES:
			new_dt = start_dt + datetime.timedelta(minutes=count*3)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FIVE_MINUTES:
			new_dt = start_dt + datetime.timedelta(minutes=count*5)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.TEN_MINUTES:
			new_dt = start_dt + datetime.timedelta(minutes=count*10)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FIFTEEN_MINUTES:
			new_dt = start_dt + datetime.timedelta(minutes=count*15)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.THIRTY_MINUTES:
			new_dt = start_dt + datetime.timedelta(minutes=count*30)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.ONE_HOUR:
			new_dt = start_dt + datetime.timedelta(hours=count)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FOUR_HOURS:
			new_dt = start_dt + datetime.timedelta(hours=count*4)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.DAILY:
			new_dt = start_dt + datetime.timedelta(hours=count*24)
			return new_dt >= end_dt or new_dt >= utcnow
		else:
			raise Exception('Period not found.')

	'''
	Streaming Utilities
	'''

	def _stream_connect(self):
		print('Connected.')
		self.sio.emit(
			'subscribe',
			{
				'broker_id': self.brokerId,
				'field': 'ontrade'
			},
			namespace='/user'
		)

	def _stream_disconnect(self):
		print('Disconnected, retrying connection...')


	def _stream_ontick(self, item):
		try:
			self.getChart(item['product'])._on_tick(item)
		except Exception as e:
			print(traceback.format_exc())
			self.stop()


	'''
	On Trade Utilities
	'''

	def _wait(self, ref, func=None, res=None, polling=0.1, timeout=5):
		start = time.time()
		while not ref in self.handled:
			if time.time() - start >= timeout: 
				if func and res: return func(*res)
				else: return None
			time.sleep(polling)
		item = self.handled[ref]
		del self.handled[ref]
		return item


	def _stream_ontrade(self, items):
		try:
			for ref_id, item in items.items():
				result = self.onTradeHandler(ref_id, item)

				# Handle result
				if len(result):
					for func in self.ontrade_subs:
						func(
							BrokerItem({
								'reference_id': ref_id,
								'type': item.get('type'),
								'item': result
							})
						)
		except Exception as e:
			print(traceback.format_exc())
			self.stop()


	def _get_trade_handler(self, order_type):
		if order_type == tl.MARKET_ENTRY or order_type == tl.LIMIT_ENTRY or order_type == tl.STOP_ENTRY:
			return self.handlePositionEntry
		elif order_type == tl.LIMIT_ORDER or order_type == tl.STOP_ORDER:
			return self.handleOrderPlacement
		elif order_type == tl.MODIFY:
			self.handleModify
		elif (
			order_type == tl.POSITION_CLOSE
			or order_type == tl.STOP_LOSS
			or order_type == tl.TAKE_PROFIT
		):
			return self.handlePositionClose
		elif order_type == tl.ORDER_CANCEL:
			return self.handleOrderClose


	def subscribeOnTrade(self, func):
		self.ontrade_subs.append(func)

	
	def unsubscribeOnTrade(self, func):
		if func in self.ontrade_subs:
			del self.ontrade_subs[self.ontrade_subs.index(func)]


	def handlePositionEntry(self, ref_id, item):
		# Handle
		pos = item.get('item')
		new_pos = tl.position.Position.fromDict(self, pos)
		self.positions.append(new_pos)

		# Add to handled
		self.handled[ref_id] = new_pos

		return new_pos


	def handleOrderPlacement(self, ref_id, item):
		# Handle
		order = item.get('item')
		new_order = tl.order.Order.fromDict(self, order)
		self.orders.append(new_order)

		# Add to handled
		self.handled[ref_id] = new_order

		return new_order


	def handleModify(self, ref_id, item):
		# Handle
		order = item.get('item')
		result = None

		if order.get('type') == tl.STOP_ORDER or order.get('type') == tl.LIMIT_ORDER:
			for match_order in self.getAllOrders():
				if match_order.order_id == order['order_id']:
					match_order.entry_price = order['entry_price']
					match_order.sl = order['sl']
					match_order.tp = order['tp']
					result = match_order
		else:
			for match_pos in self.getAllPositions():
				if match_pos.order_id == order['order_id']:
					match_pos.sl = order['sl']
					match_pos.tp = order['tp']
					result = match_pos

		# Add to handled
		self.handled[ref_id] = result

		return result

	def handlePositionClose(self, ref_id, item):
		# Handle
		positions = copy(self.getAllPositions())
		pos = item.get('item')
		result = None

		for j in range(len(positions)):
			match_pos = positions[j]
			if match_pos.order_id == pos['order_id']:
				# Handle partial position close
				if match_pos.lotsize != pos['lotsize']:
					cpy = tl.position.Position.fromDict(self, pos)
					match_pos.lotsize = match_pos.lotsize - cpy.lotsize
					result = cpy
				# Handle full position close
				else:
					match_pos.close_price = pos['close_price']
					match_pos.close_time = pos['close_time']
					result = match_pos
					del self.positions[self.positions.index(match_pos)]

				break

		# Add to handled
		self.handled[ref_id] = result

		return result


	def handleOrderClose(self, ref_id, item):
		# Handle
		orders = copy(self.getAllOrders())
		order = item.get('item')
		result = None

		for j in range(len(orders)):
			match_order = orders[j]
			if match_order.order_id == order['order_id']:
				# Handle partial position close
				if match_order.lotsize != order['lotsize']:
					cpy = tl.order.Order.fromDict(self, order)
					match_order.lotsize = match_order.lotsize - cpy.lotsize
					result = cpy
				# Handle full position close
				else:
					match_order.close_price = order['close_price']
					match_order.close_time = order['close_time']
					result = match_order
					del self.orders[self.orders.index(match_order)]

				break

		# Add to handled
		self.handled[ref_id] = result

		return result


	def handleUpdate(self, ref_id, item):
		self.positions = [
			tl.position.Position.fromDict(self, pos) 
			for pos in item.get('positions') 
			if pos['account_id'] in self.accounts
		]

		self.orders = [
			tl.order.Order.fromDict(self, order) 
			for order in item.get('orders') 
			if order['account_id'] in self.accounts
		]


	def onTradeHandler(self, ref_id, item):
		result = []
		order_type = item.get('type')

		# Position Entry
		if order_type == tl.MARKET_ENTRY or order_type == tl.LIMIT_ENTRY or order_type == tl.STOP_ENTRY:
			result = self.handlePositionEntry(ref_id, item)

		# Order Placement
		elif order_type == tl.LIMIT_ORDER or order_type == tl.STOP_ORDER:
			result = self.handleOrderPlacement(ref_id, item)

		# Trade Modification
		elif order_type == tl.MODIFY:
			result = self.handleModify(ref_id, item)

		# Position Close
		elif (
			order_type == tl.POSITION_CLOSE
			or order_type == tl.STOP_LOSS
			or order_type == tl.TAKE_PROFIT
		):
			result = self.handlePositionClose(ref_id, item)

		# Order Cancel
		elif order_type == tl.ORDER_CANCEL:
			result = self.handleOrderClose(ref_id, item)

		elif order_type == tl.UPDATE:
			self.handleUpdate(ref_id, item)

		return result

