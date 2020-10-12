import json
from app import pythonsdk as tl
from .broker import Broker, BacktestMode, State
from threading import Thread


class Strategy(object):

	def __init__(self, api, strategy_id=None, accounts=None, data_path='data/'):
		# Retrieve broker type
		self.api = api
		self.strategyId = strategy_id
		self.broker = Broker(self, self.api, strategy_id=self.strategyId, data_path=data_path)
		self.accounts = accounts

		self.tick_queue = []
		self.lastTick = None

	def run(self, auth_key=None, strategy_id=None, accounts=[]):
		if self.strategyId is None:
			self.strategyId = strategy_id
		if self.accounts is None:
			self.accounts = accounts

		self.broker.run(self.strategyId)

	def stop(self):
		self.broker.stop()


	def __getattribute__(self, key):
		if key == 'positions':
			return self.getAllPositions()
		elif key == 'orders':
			return self.getAllOrders()
		else:
			return super().__getattribute__(key)

	'''
	Broker functions
	'''

	def backtest(self, start, end, mode=BacktestMode.RUN):
		if self.getBroker().state != State.STOPPED:
			if isinstance(mode, str):
				mode = BacktestMode(mode)
			return self.getBroker().backtest(start, end, mode=mode, quick_download=True)

		else:
			raise tl.error.BrokerlibException('Strategy has been stopped.')


	def startFrom(self, dt):
		return self.getBroker().startFrom(dt)


	# Chart functions
	def getChart(self, product, *periods):
		if self.getBroker().state != State.STOPPED:
			return self.getBroker().getChart(product, *periods)

		else:
			raise tl.error.BrokerlibException('Strategy has been stopped.')

	# Account functions

	def getCurrency(self, account_id):
		return self.getBroker().getAccountInfo([account_id])[account_id]['currency']

	def getBalance(self, account_id):
		return self.getBroker().getAccountInfo([account_id])[account_id]['balance']

	def getProfitLoss(self, account_id):
		return self.getBroker().getAccountInfo([account_id])[account_id]['pl']

	def getEquity(self, account_id):
		info = self.getBroker().getAccountInfo([account_id])[account_id]
		return info['balance'] + info['pl']

	def getMargin(self, account_id):
		return self.getBroker().getAccountInfo([account_id])[account_id]['margin']


	# Order functions

	def getAllPositions(self):
		result = []
		for account_id in self.accounts:
			result += self.getBroker().getAllPositions(account_id=account_id)
		return result


	def getAllOrders(self):
		result = []
		for account_id in self.accounts:
			result += self.getBroker().getAllOrders(account_id=account_id)
		return result


	def buy(self,
		product, lotsize,
		order_type=tl.MARKET_ORDER,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if self.getBroker().state != State.STOPPED:
			return self.getBroker().buy(
				product, lotsize, self.accounts,
				order_type=order_type,
				entry_range=entry_range, entry_price=entry_price,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		else:
			raise tl.error.BrokerlibException('Strategy has been stopped.')


	def sell(self,
		product, lotsize,
		order_type=tl.MARKET_ORDER,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		if self.getBroker().state != State.STOPPED:
			return self.getBroker().sell(
				product, lotsize, self.accounts,
				order_type=order_type,
				entry_range=entry_range, entry_price=entry_price,
				sl_range=sl_range, tp_range=tp_range,
				sl_price=sl_price, tp_price=tp_price
			)

		else:
			raise tl.error.BrokerlibException('Strategy has been stopped.')


	def closeAllPositions(self, positions=None):
		return self.getBroker().closeAllPositions(positions)


	'''
	GUI Functions
	'''

	def draw(self, draw_type, layer, product, price, timestamp, 
				color='#000000', scale=1.0, rotation=0):
		timestamp = self.lastTick.timestamp
		drawing = {
			'product': product,
			'type': draw_type,
			'timestamps': [int(timestamp)],
			'prices': [price],
			'properties': {
				'colors': [color],
				'scale': scale,
				'rotation': rotation
			}
		}
		if self.getBroker().state != State.BACKTEST:
			Thread(
				target=self.api.userAccount.createDrawings, 
				args=(self.strategyId, layer, [drawing])
			).start()

		else:
			# Handle drawings through backtester
			self.getBroker().backtester.createDrawing(timestamp, layer, drawing)


	def clearDrawingLayer(self, layer):
		timestamp = self.lastTick.timestamp

		if self.getBroker().state != State.BACKTEST:
			Thread(
				target=self.api.userAccount.deleteDrawingLayer, 
				args=(self.strategyId, layer)
			).start()

		else:
			# Handle drawings through backtester
			self.getBroker().backtester.clearDrawingLayer(timestamp, layer)


	def deleteAllDrawings(self):
		timestamp = self.lastTick.timestamp

		if self.getBroker().state != State.BACKTEST:
			Thread(
				target=self.api.userAccount.deleteAllDrawings, 
				args=(self.strategyId,)
			).start()

		else:
			# Handle drawings through backtester
			self.getBroker().backtester.deleteAllDrawings(timestamp)


	def log(self, *objects, sep=' ', end='\n', file=None, flush=None):
		print(*objects, sep=sep, end=end, file=file, flush=flush)
		msg = sep.join(map(str, objects)) + end
		timestamp = self.lastTick.timestamp

		if self.getBroker().state != State.BACKTEST:
			return

		else:
			# Handle logs through backtester
			self.getBroker().backtester.createLogItem(timestamp, msg)


	def info(self, name, value):
		timestamp = self.lastTick.timestamp

		# Check if value is json serializable
		json.dumps(value)

		item = {
			'name': str(name),
			'value': value
		}

		if self.getBroker().state != State.BACKTEST:
			return

		else:
			# Handle info through backtester
			self.getBroker().backtester.createInfoItem(timestamp, item)

	'''
	Setters
	'''

	def setApp(self, app):
		self.getBroker().setApp(app)

	def setTick(self, tick):
		self.lastTick = tick

	'''
	Getters
	'''

	def getBroker(self):
		return self.broker

