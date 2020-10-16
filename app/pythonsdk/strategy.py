import json
import time
from app import pythonsdk as tl
from .broker import Broker, BacktestMode, State
from threading import Thread


SAVE_INTERVAL = 60 # Seconds


class Strategy(object):

	def __init__(self, api, strategy_id=None, accounts=None, data_path='data/'):
		# Retrieve broker type
		self.api = api
		self.strategyId = strategy_id
		self.broker = Broker(self, self.api, strategy_id=self.strategyId, data_path=data_path)
		self.accounts = accounts

		# GUI Queues
		self.drawing_queue = []
		self.log_queue = []
		self.info_queue = []
		self.lastSave = time.time()

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
			'id': self.broker.generateReference(),
			'product': product,
			'layer': layer,
			'type': draw_type,
			'timestamps': [int(timestamp)],
			'prices': [price],
			'properties': {
				'colors': [color],
				'scale': scale,
				'rotation': rotation
			}
		}
		if self.getBroker().state == State.LIVE:
			item = {
				'timestamp': timestamp,
				'type': tl.CREATE_DRAWING,
				'item': drawing
			}

			# Send Gui Socket Message
			self.api.ctrl.sio.emit(
				'ongui', 
				{'strategy_id': self.strategyId, 'item': item}, 
				namespace='/admin'
			)

			# Save to drawing queue
			self.drawing_queue.append(item)

		elif self.getBroker().state.value <= State.BACKTEST_AND_RUN.value:
			# Handle drawings through backtester
			self.getBroker().backtester.createDrawing(timestamp, layer, drawing)


	def clearDrawingLayer(self, layer):
		timestamp = self.lastTick.timestamp

		if self.getBroker().state == State.LIVE:
			item = {
				'id': self.broker.generateReference(),
				'timestamp': timestamp,
				'type': tl.CLEAR_DRAWING_LAYER,
				'item': layer
			}

			# Send Gui Socket Message
			self.api.ctrl.sio.emit(
				'ongui', 
				{'strategy_id': self.strategyId, 'item': item}, 
				namespace='/admin'
			)

			# Handle to drawing queue
			self.drawing_queue.append(item)

		elif self.getBroker().state.value <= State.BACKTEST_AND_RUN.value:
			# Handle drawings through backtester
			self.getBroker().backtester.clearDrawingLayer(timestamp, layer)


	def clearAllDrawings(self):
		timestamp = self.lastTick.timestamp

		if self.getBroker().state == State.LIVE:
			item = {
				'id': self.broker.generateReference(),
				'timestamp': timestamp,
				'type': tl.CLEAR_ALL_DRAWINGS,
				'item': None
			}

			# Send Gui Socket Message
			self.api.ctrl.sio.emit(
				'ongui', 
				{'strategy_id': self.strategyId, 'item': item}, 
				namespace='/admin'
			)

			# Handle to drawing queue
			self.drawing_queue.append(item)

		elif self.getBroker().state.value <= State.BACKTEST_AND_RUN.value:
			# Handle drawings through backtester
			self.getBroker().backtester.deleteAllDrawings(timestamp)


	def log(self, *objects, sep=' ', end='\n', file=None, flush=None):
		if self.getBroker().state.value == 3:
			print(*objects, sep=sep, end=end, file=file, flush=flush)
		msg = sep.join(map(str, objects)) + end
		timestamp = self.lastTick.timestamp

		if self.getBroker().state == State.LIVE:
			item = {
				'timestamp': timestamp,
				'type': tl.CREATE_LOG,
				'item': msg
			}

			# Send Gui Socket Message
			self.api.ctrl.sio.emit(
				'ongui', 
				{'strategy_id': self.strategyId, 'item': item}, 
				namespace='/admin'
			)

			# Save to log queue
			self.log_queue.append(item)

		elif self.getBroker().state.value <= State.BACKTEST_AND_RUN.value:
			# Handle logs through backtester
			self.getBroker().backtester.createLogItem(timestamp, msg)


	def clearLogs(self):
		return


	def info(self, name, value):
		timestamp = self.lastTick.timestamp

		# Check if value is json serializable
		json.dumps(value)

		item = {
			'name': str(name),
			'value': value
		}

		if self.getBroker().state == State.LIVE:
			item = {
				'timestamp': timestamp,
				'type': tl.CREATE_INFO,
				'item': item
			}

			# Send Gui Socket Message
			self.api.ctrl.sio.emit(
				'ongui', 
				{'strategy_id': self.strategyId, 'item': item}, 
				namespace='/admin'
			)

			# Handle to info queue
			self.info_queue.append(item)

		elif self.getBroker().state.value <= State.BACKTEST_AND_RUN.value:
			# Handle info through backtester
			self.getBroker().backtester.createInfoItem(timestamp, item)

	'''
	Setters
	'''

	def resetGuiQueues(self):
		self.drawing_queue = []
		self.log_queue = []
		self.info_queue = []
		self.lastSave = time.time()


	def handleDrawingsSave(self, gui):
		if gui is None:
			gui = self.api.userAccount.getGui(self.strategyId)
		if 'drawings' not in gui:
			gui['drawings'] = {}

		for i in self.drawing_queue:
			if i['type'] == tl.CREATE_DRAWING:
				if i['item']['layer'] not in gui['drawings']:
					gui['drawings'][i['item']['layer']] = []
				gui['drawings'][i['item']['layer']].append(i['item'])

			elif i['type'] == tl.CLEAR_DRAWING_LAYER:
				if i['item'] in gui['drawings']:
					gui['drawings'][i['item']] = []

			elif i['type'] == tl.CLEAR_ALL_DRAWINGS:
				for layer in gui['drawings']:
					gui['drawings'][layer] = []

		return gui

	def handleLogsSave(self, gui):
		if gui is None:
			gui = self.api.userAccount.getGui(self.strategyId)
		if 'logs' not in gui:
			gui['logs'] = []

		gui['logs'] += self.log_queue

		return gui

	def handleInfoSave(self, gui):
		if gui is None:
			gui = self.api.userAccount.getGui(self.strategyId)
		if 'info' not in gui:
			gui['info'] = []

		gui['info'] += self.info_queue

		return gui


	def saveGui(self):
		if time.time() - self.lastSave > SAVE_INTERVAL:
			gui = None

			if len(self.drawing_queue) > 0:
				gui = self.handleDrawingsSave(gui)

			if len(self.log_queue) > 0:
				gui = self.handleLogsSave(gui)

			if len(self.info_queue) > 0:
				gui = self.handleInfoSave(gui)

			if gui is not None:
				Thread(target=self.api.userAccount.updateGui, args=(self.strategyId, gui)).start()
				self.resetGuiQueues()


	def setApp(self, app):
		self.getBroker().setApp(app)

	def setTick(self, tick):
		self.lastTick = tick

		# Save GUI
		if self.getBroker().state == State.LIVE:
			self.saveGui()

	'''
	Getters
	'''

	def getBroker(self):
		return self.broker

