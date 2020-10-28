import numpy as np
import pandas as pd
import datetime
import shortuuid
import time
import math
from app import tradelib as tl
from threading import Thread
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException
from enum import Enum
from copy import copy

'''
Broker Names
'''

BACKTEST_NAME = 'backtest'
IG_NAME = 'ig'
OANDA_NAME = 'oanda'
PAPERTRADER_NAME = 'papertrader'

'''
Pending OTC Types
'''
OPEN = 'OPEN'
MODIFY = 'MODIFY'
DELETE = 'DELETE'
REJECT = 'REJECT'

def get_list():
	return [
		OANDA_NAME,
		IG_NAME,
	]

class BrokerStatus(Enum):
	OFFLINE = 'offline'
	LIVE = 'live'

'''
Parent Broker Class
'''
class Broker(object):

	__slots__ = (
		'ctrl', 'userAccount', 'brokerId', 'name', 'backtester', 'acceptLive', 
		'accounts', 'charts', 'positions', 'orders', 'is_running', '_handled', 'transactions',
		'ontrade_subs', 'display_name'
	)
	def __init__(self, ctrl, user_account, broker_id, name, accounts, display_name):
		self.ctrl = ctrl
		self.userAccount = user_account
		self.brokerId = broker_id
		self.name = name
		self.display_name = display_name

		if self.name == OANDA_NAME:
			self.backtester = tl.OandaBacktester(self)
		else:
			self.backtester = tl.IGBacktester(self)

		self.acceptLive = False

		# Containers
		self.accounts = accounts

		self.charts = []
		self.positions = []
		self.orders = []
		self.ontrade_subs = {}
		self.transactions = self._create_empty_transaction_df()
		self._handled = {}

		self.is_running = True


		# Handle mandatory strategy startup functions
		if self.userAccount:
			self._handle_papertrader_setup()


	'''
	Utilities
	'''

	# Private Functions
	def _handle_papertrader_setup(self):

		if tl.broker.PAPERTRADER_NAME in self.accounts:
			# Get transaction history
			transactions = self.ctrl.getDb().getStrategyTransactions(self.userAccount.userId, self.brokerId)
			from_ts = None
			if transactions.size > 0:
				from_ts = transactions[['timestamp']].values[-1][0]
			# Handle saved strategy positions
			earliest_trade_ts = self._retrieve_strategy_trades()
			if from_ts is None:
				from_ts = earliest_trade_ts

			# Do backtest from last transaction timestamp
			if from_ts is not None:
				self._run_backtest(from_ts)

			self.acceptLive = True
			Thread(target=self.saveTransactions).start()


	def _retrieve_strategy_trades(self):
		trades = self.ctrl.getDb().getStrategyTrades(self.userAccount.userId, self.brokerId)
		positions = trades.get('positions')
		orders = trades.get('orders')
		earliest_trade_ts = None

		for pos in positions:
			if earliest_trade_ts is None or pos['open_time'] < earliest_trade_ts:
				earliest_trade_ts = pos['open_time']
			if pos.get('account_id') in self.getAccounts():
				self.positions.append(tl.Position.fromDict(self, pos))

		for order in orders:
			if earliest_trade_ts is None or order['open_time'] < earliest_trade_ts:
				earliest_trade_ts = order['open_time']
			if order.get('account_id') in self.getAccounts():
				self.orders.append(tl.Order.fromDict(self, order))

		return earliest_trade_ts


	def _handle_live_strategy_setup(self):
		# Handle saved IG strategy positions
		self._handle_live_strategy_positions()

		self._handle_live_strategy_orders()

	def _handle_live_strategy_positions(self):
		# Get open positions
		for acc in self.getAccounts():
			if acc != tl.broker.PAPERTRADER_NAME:
				# LIVE positions
				live_positions = self._get_all_positions(acc)[acc]
				self.positions += live_positions

	def _handle_live_strategy_orders(self):
		# Get open positions
		for acc in self.getAccounts():
			if acc != tl.broker.PAPERTRADER_NAME:
				# LIVE positions
				live_orders = self._get_all_orders(acc)[acc]
				self.orders += live_orders

	def _run_backtest(self, from_ts):
		products = []
		for i in self.positions + self.orders:
			if i.product not in products:
				products.append(i.product)

		for product in products:
			chart = self.getChart(product)

			# Get all minute prices from timestamp 
			data = chart._load_data(
				tl.period.ONE_MINUTE, 
				start=tl.convertTimestampToTime(from_ts),
				end=tl.setTimezone(datetime.datetime.utcnow(), 'UTC'),
				force_download=False
			)
			ts_data = data.index.values
			ohlc_data = data.values

			for i in range(ts_data.size):
				self.backtester.handleOrders(product, ts_data[i], ohlc_data[i], is_backtest=True)
				self.backtester.handleStopLoss(product, ts_data[i], ohlc_data[i], is_backtest=True)
				self.backtester.handleTakeProfit(product, ts_data[i], ohlc_data[i], is_backtest=True)

	def _wait(self, ref, func=None, res=None, polling=0.1, timeout=5):
		start = time.time()
		while not ref in self._handled:
			if time.time() - start >= timeout: 
				if func and res: return func(res)
				else: return None
			time.sleep(polling)
		item = self._handled[ref]
		del self._handled[ref]
		return item


	def _create_empty_transaction_df(self):
		df = pd.DataFrame(columns=[
			'reference_id', 'timestamp', 'type', 'accepted',
			'order_id', 'account_id', 'product', 'order_type',
			'direction', 'lotsize', 'entry_price', 'close_price', 'sl', 'tp',
			'open_time', 'close_time'
		])
		return df.set_index('reference_id')
	

	def save_data(self, df, product, period):
		return

	def handle_live_data_save(self, res):
		return

	# Public Functions
	def generateReference(self):
		return shortuuid.uuid()

	def stop(self):
		self.is_running = False

	def getAccounts(self):
		return self.accounts

	def createChart(self, product):
		chart = self.ctrl.charts.getChart(self.name, product)
		self.charts.append(chart)

		sub_id = self.generateReference()
		chart.subscribe(tl.period.ONE_MINUTE, self.brokerId, sub_id, self._handle_tick_checks)
		return chart

	def getAllCharts(self):
		return self.charts

	def getChart(self, product):
		for chart in self.charts:
			if chart.product == product:
				return chart

		return self.createChart(product)

	def getAsk(self, product):
		return self.getChart(product).getLatestAsk(tl.period.TICK)

	def getBid(self, product):
		return self.getChart(product).getLatestBid(tl.period.TICK)

	def getTimestamp(self, product, period):
		return self.getChart(product).getLatestTimestamp(period)

	def getLotSize(self, bank, risk, stop_range):
		return round(bank * (risk / 100) / stop_range, 2)

	def getAllPositions(self, account_id=None):
		result = []
		for pos in self.positions:
			# Return specified account positions
			if not account_id or pos.account_id == account_id:
				result.append(pos)
		return result

	def getPositionByID(self, order_id):
		for pos in self.getAllPositions():
			if pos.order_id == order_id:
				return pos
		return None

	def getAllOrders(self, account_id=None):
		result = []
		for order in self.orders:
			# Return specified account positions
			if not account_id or order.account_id == account_id:
				result.append(order)
		return result

	def getOrderByID(self, order_id):
		for order in self.getAllOrders():
			if order.order_id == order_id:
				return order
		return None

	def getUserAccount(self):
		return self.userAccount

	'''
	Account Utilities
		- All functions access brokerage direction
	'''

	# Public
	def getAccountInfo(self, accounts, override=False):
		return self._get_account_details(accounts, override=override)

	'''
	Dealing Utilities
		- All functions access brokerage directly
	'''

	# Public
	def buy(self,
		product, lotsize, account_id,
		order_type=tl.MARKET_ORDER,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None,
		override=False
	):
		# Cancel order if status set to `STOP`
		if order_type == tl.MARKET_ORDER:
			res = self.createPosition(
				product, lotsize, tl.LONG, account_id,
				entry_range, entry_price,
				sl_range, tp_range, sl_price, tp_price,
				override=override
			)

		elif order_type in (tl.STOP_ORDER, tl.LIMIT_ORDER):
			res = self.createOrder(
				product, lotsize, tl.LONG, account_id,
				order_type, entry_range, entry_price,
				sl_range, tp_range, sl_price, tp_price,
				override=override
			)

		else:
			raise OrderException('Order type not found.')

		return res

	def sell(self,
		product, lotsize, account_id,
		order_type=tl.MARKET_ORDER,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None,
		override=False
	):
		if order_type == tl.MARKET_ORDER:
			res = self.createPosition(
				product, lotsize, tl.SHORT, account_id,
				entry_range, entry_price,
				sl_range, tp_range, sl_price, tp_price,
				override=override
			)

		elif order_type in (tl.STOP_ORDER, tl.LIMIT_ORDER):
			res = self.createOrder(
				product, lotsize, tl.SHORT, account_id,
				order_type, entry_range, entry_price,
				sl_range, tp_range, sl_price, tp_price,
				override=override
			)

		else:
			raise OrderException('Order type not found.')

		return res

	def stopAndReverse(self,
		product, lotsize, account_id,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if len(self.positions) > 0:
			direction = self.positions[-1].direction
			self.closeAllPositions()
		else:
			raise OrderException('Must be in position to stop and reverse.')

		if direction == tl.LONG:
			res = self.sell(
				product, lotsize, account_id,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)
		else:
			res = self.buy(
				product, lotsize, account_id,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		return res


	def uploadTrades(self, positions, orders):
		for pos in positions:
			pos = dict(pos)
			pos['broker'] = PAPERTRADER_NAME
			self.positions.append(
				tl.Position.fromDict(self, pos)
			)

		for order in orders:
			order = dict(order)
			order['broker'] = PAPERTRADER_NAME
			self.orders.append(
				tl.Order.fromDict(self, order)
			)

		# Send Update
		res = {
			self.generateReference(): {
				'timestamp': time.time(),
				'type': UPDATE,
				'accepted': True,
				'items': {
					'positions': self.getAllPositions(),
					'orders': self.getAllOrders()
				}
			}
		}

		self.handleOnTrade(res)


	# Private

	# Update Handlers
	def handleOnTrade(self, res):
		# Handle stream subscriptions
		for func in self.ontrade_subs.values():
			func(res)

		self.ctrl.sio.emit(
			'ontrade', 
			{'broker_id': self.brokerId, 'item': res}, 
			namespace='/admin'
		)


	def handleTransaction(self, res):
		for k, v in res.items():
			if k not in self.transactions:
				v = copy(v)
				item = v.get('item')
				if item is not None:
					del v['item']
					v.update(item)
				self.transactions.loc[k] = v

		if self.acceptLive:
			Thread(target=self.saveTransactions).start()


	def saveTransactions(self):
		if tl.broker.PAPERTRADER_NAME in self.accounts:
			transactions = self.ctrl.getDb().getStrategyTransactions(self.userAccount.userId, self.brokerId)
			transactions = pd.concat((transactions, self.transactions))
			self.transactions = self._create_empty_transaction_df()
			self.ctrl.getDb().updateStrategyTransactions(self.userAccount.userId, self.brokerId, transactions)


	def orderValidation(self, order, min_dist=0):

		if order.direction == tl.LONG:
			price = self.getAsk(order.product)
		else:
			price = self.getBid(order.product)

		# Entry validation
		if order.get('type') == tl.STOP_ORDER or order.get('type') == tl.LIMIT_ORDER:
			if order.entry_price == None:
				raise OrderException('Order must contain entry price.')
			elif order_type == tl.LIMIT_ORDER:
				if direction == tl.LONG:
					if order.entry_price > price - tl.utils.convertToPrice(min_dist):
						raise OrderException('Long limit order entry must be lesser than current price.')
				else:
					if order.entry_price < price + tl.utils.convertToPrice(min_dist):
						raise OrderException('Short limit order entry must be greater than current price.')
			elif order_type == tl.STOP_ORDER:
				if order.direction == tl.LONG:
					if order.entry_price < price + tl.utils.convertToPrice(min_dist):
						raise OrderException('Long stop order entry must be greater than current price.')
				else:
					if order.entry_price > price - tl.utils.convertToPrice(min_dist):
						raise OrderException('Short stop order entry must be lesser than current price.')

		# SL/TP validation
		if order.direction == tl.LONG:
			if order.sl and order.sl > order.entry_price - tl.utils.convertToPrice(min_dist):
				raise OrderException('Stop loss price must be lesser than entry price.')
			if order.tp and order.tp < order.entry_price + tl.utils.convertToPrice(min_dist):
				raise OrderException('Take profit price must be greater than entry price.')
		else:
			if order.sl and order.sl < order.entry_price + tl.utils.convertToPrice(min_dist):
				raise OrderException('Stop loss price must be greater than entry price.')
			if order.tp and order.tp > order.entry_price - tl.utils.convertToPrice(min_dist):
				raise OrderException('Take profit price must be lesser than entry price.')


	# Order Requests
	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price,
		override=False
	):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)
		return self.backtester.createPosition(
			product, lotsize, direction,
			account_id, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)
		

	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)
		return self.backtester.modifyPosition(pos, sl_price, tp_price)


	def deletePosition(self, pos, lotsize, override=False):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)
		return self.backtester.deletePosition(pos, lotsize)


	def createOrder(self,
		product, lotsize, direction, account_id,
		order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price,
		override=False
	):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)
		return self.backtester.createOrder(
			product, lotsize, direction, account_id,
			order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)
		return self.backtester.modifyOrder(order, lotsize, entry_price, sl_price, tp_price)


	def deleteOrder(self, order, override=False):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)
		return self.backtester.deleteOrder(order)

	'''
	Paper Trader Utilities
	'''

	def _handle_tick_checks(self, item):
		product = item['product']
		timestamp = int(item['timestamp'])
		ohlc = np.array([item['item']['ask'][3]]*4 + [item['item']['bid'][3]]*4, dtype=np.float64)

		self.backtester.handleOrders(product, timestamp, ohlc)
		self.backtester.handleStopLoss(product, timestamp, ohlc)
		self.backtester.handleTakeProfit(product, timestamp, ohlc)
		

	'''
	Callback Utilities
	'''

	# Public
	def subscribeOnTrade(self, func, sub_id):
		self.ontrade_subs[sub_id] = func

	def unsubscribeOnTrade(self, sub_id):
		if sub_id in self.ontrade_subs:
			del self.ontrade_subs[sub_id]

from .brokers import (
	Oanda,
	IG
)