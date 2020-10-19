import json, jwt
import math
import time
import string, random
from app import tradelib as tl
from threading import Thread
from app.strategy import Strategy
from app.error import AccountException, BrokerException, AuthorizationException
from flask import current_app

class Account(object):

	def __init__(self, ctrl, userId):
		# Check user existance
		self.ctrl = ctrl
		self.userId = userId
		self.brokers = {}
		self.strategies = {}
		self.keys = {}

		self._user_validation()

	# Public
	def getAccountDetails(self):
		user = self.ctrl.getDb().getUser(self.userId)
		if not user:
			raise AccountException('User does not exist.')

		return {
			'user_id': user['user_id'],
			'username': user['username'],
			'brokers': list(user['brokers'].keys()),
			'strategies': list(user['strategies'].keys()),
			'metadata': user['metadata'],
		}

	def generateId(self):
		letters = string.ascii_uppercase + string.digits
		return ''.join(random.choice(letters) for i in range(6))


	# Strategy Functions
	def startStrategy(self, strategy_id):
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		if strategy_info is None:
			raise AccountException('Strategy not found.')

		# Handle broker info
		broker = self._set_broker(strategy_id, strategy_info)
		# Handle keys
		self.keys[strategy_id] = strategy_info.get('keys')
		# Upload any strategy info changes
		self.updateTrades(
			strategy_id,
			broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
			broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
		)

		# Init strategy handler
		strategy = self._set_strategy(strategy_id, broker, strategy_info.get('package'))


	def getStrategyInfo(self, strategy_id):
		# while self.strategies.get(strategy_id) == 'working':
		# 	pass
		
		strategy = self.strategies.get(strategy_id)
		if strategy is None:
			self.startStrategy(strategy_id)
			strategy = self.strategies.get(strategy_id)

		return strategy


	def getStrategy(self, strategy_id):
		broker = self.brokers.get(strategy_id)
		if broker is None:
			self.startStrategy(strategy_id)
			broker = self.brokers.get(strategy_id)
			
		return {
			'strategy_id': strategy_id,
			'broker': broker.name,
			'broker_id': broker.brokerId,
			'accounts': {
				acc: { 
					'strategy_status': (
						self.strategies.get(strategy_id) is not None and 
						self.strategies[strategy_id].isRunning(acc)
					)
				} 
				for acc in broker.getAccounts()
			},
			'positions': broker.getAllPositions(),
			'orders': broker.getAllOrders()
		}


	def createStrategy(self, name, broker, accounts):
		strategy = {
			'name': name,
			'accounts': accounts + [tl.broker.PAPERTRADER_NAME],
			'broker': broker,
			'keys': [],
			'package': ''
		}
		strategy_id = self.ctrl.getDb().createStrategy(self.userId, strategy)
		return strategy_id


	def updateStrategy(self, strategy_id, update):
		return self.ctrl.getDb().updateStrategy(self.userId, strategy_id, update)


	def deleteStrategy(self, strategy_id):
		self.ctrl.getDb().deleteStrategy(self.userId, strategy_id)
		broker = self.brokers.get(strategy_id)
		if broker is not None:
			del self.brokers[strategy_id]


	def strategyExists(self, strategy_id):
		return strategy_id in self.brokers


	def updateStrategyStatus(self, strategy_id, accounts):
		# Retrieve strategy
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		strategy_info['accounts'] = accounts

		# Perform update
		self.ctrl.getDb().updateStrategy(self.userId, strategy_id, strategy_info)


	def runStrategyScript(self, strategy_id, accounts):
		strategy = self.getStrategyInfo(strategy_id)

		# Retrieve Input Variables
		gui = self.getGui(strategy_id)
		input_variables = gui.get('input_variables')

		strategy.run(accounts, input_variables=input_variables)
		return strategy.package


	def stopStrategyScript(self, strategy_id, accounts):
		strategy = self.strategies.get(strategy_id)
		if strategy is not None:
			strategy.stop(accounts)
		return strategy.package


	def updateStrategyPackage(self, strategy_id, new_package):
		# Retrieve strategy
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		strategy_info['package'] = new_package

		# Perform update
		self.ctrl.getDb().updateStrategy(self.userId, strategy_id, strategy_info)


	def _set_strategy(self, strategy_id, api, package):
		strategy = Strategy(strategy_id, api, package)
		if self.strategies.get(strategy_id) is None:
			self.strategies[strategy_id] = strategy
		return self.strategies[strategy_id]


	def getStrategyBroker(self, strategy_id):
		# while self.strategies.get(strategy_id) == 'working':
		# 	pass

		broker = self.brokers.get(strategy_id)
		if broker is None:
			self.startStrategy(strategy_id)
			broker = self.brokers.get(strategy_id)
		return broker

	# Broker Functions
	def getAllBrokers(self):
		result = self.ctrl.getDb().getUser(self.userId).get('brokers')
		if result is None:
			return None

		for k, v in result.items():
			result[k] = jwt.decode(
				v, self.ctrl.app.config['SECRET_KEY'], 
				algorithms=['HS256']
			)
		return result

	def getBroker(self, name):
		result = self.ctrl.getDb().getBroker(self.userId, name)
		return result

	def createBroker(self, name, broker_name, **props):
		result = self.ctrl.getDb().createBroker(self.userId, name, broker_name, **props)
		return result

	def updateBrokerName(self, old_name, new_name):
		result = self.ctrl.getDb().updateBrokerName(self.userId, old_name, new_name)
		return result

	def deleteBroker(self, name):
		result = self.ctrl.getDb().deleteBroker(self.userId, name)
		return result


	# API Key Functions
	def getKeys(self, strategy_id):
		keys = self.keys.get(strategy_id)
		if keys is None:
			keys = self.ctrl.getDb().getKeys(self.userId, strategy_id)
		return keys

	def addKey(self, strategy_id, key):
		keys = self.getKeys(strategy_id)
		if keys is not None:
			keys.append(key)

	def checkKey(self, strategy_id, key):
		keys = self.getKeys(strategy_id)
		if keys is not None:
			return key in keys
		return False

	def deleteKey(self, strategy_id, key):
		keys = self.getKeys(strategy_id)
		if keys is not None:
			if key in keys:
				del keys[keys.index(key)]
				return True
		return False

	# Login Token Functions
	def generateToken(self):
		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time()), 
		}
		return jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')

	def checkToken(self, token):
		try:
			payload = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
			return payload.get('sub') == self.userId

		except jwt.ExpiredSignatureError:
			raise AuthorizationException('Expired signature.')
		except jwt.InvalidTokenError:
			raise AuthorizationException('Invalid token.')
		except Exception:
			raise AuthorizationException('Invalid token.')

	# Storage Functions
	def updateTrades(self, strategy_id, positions, orders):
		trades = {
			'positions': positions,
			'orders': orders
		}
		Thread(
			target=self.ctrl.getDb().updateStrategyTrades, 
			args=(self.userId, strategy_id, trades)
		).start()
		

	def getGui(self, strategy_id):
		return self.ctrl.getDb().getStrategyGui(self.userId, strategy_id)

	def updateGui(self, strategy_id, new_gui):
		self.ctrl.getDb().updateStrategyGui(self.userId, strategy_id, new_gui)

	def updateGuiItems(self, strategy_id, items):
		gui = self.getGui(strategy_id)
		item_ids = [i['id'] for i in gui['windows']]
		result = []

		if items.get('windows') != None and isinstance(items.get('windows'), list):
			for i in items.get('windows'):
				if i.get('id') in item_ids:
					gui['windows'][item_ids.index(i.get('id'))] = i
				else:
					gui['windows'].append(i)

				result.append(i.get('id'))

		if items.get('account') != None:
			gui['account'] = items.get('account')

		self.updateGui(strategy_id, gui)
		return result

	def createGuiItem(self, strategy_id, item):
		gui = self.getGui(strategy_id)
		gui_ids = [i['id'] for i in gui['windows']]

		# Make sure id is unique
		item['id'] = self.generateId()
		while item['id'] in gui_ids:
			item['id'] = self.generateId()

		gui['windows'].push(item)
		self.updateGui(strategy_id, gui)
		return item['id']

	def deleteGuiItems(self, strategy_id, items):
		gui = self.getGui(strategy_id)
		item_ids = [i['id'] for i in gui['windows']]
		result = []

		if items.get('windows') != None and isinstance(items.get('windows'), list):
			for i in items.get('windows'):
				if i in item_ids:
					del gui['windows'][item_ids.index(i)]
					result.append(i)

		self.updateGui(strategy_id, gui)
		return result


	def getBacktestInfo(self, strategy_id, backtest_id):
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		broker_info = self.ctrl.getDb().getBroker(self.userId, strategy_info.get('broker'))
		gui = self.getBacktestGui(strategy_id, backtest_id)
		return {
			**{ 
				'broker_id': strategy_info.get('broker'),
				'broker': broker_info.get('broker')
			},
			**gui
		}


	def getBacktestGui(self, strategy_id, backtest_id):
		return self.ctrl.getDb().getStrategyBacktestGui(self.userId, strategy_id, backtest_id)


	def updateBacktestGui(self, strategy_id, backtest_id, new_gui):
		self.ctrl.getDb().updateStrategyBacktestGui(self.userId, strategy_id, backtest_id, new_gui)


	def updateBacktestGuiItems(self, strategy_id, backtest_id, items):
		gui = self.getBacktestGui(strategy_id, backtest_id)
		item_ids = [i['id'] for i in gui['windows']]
		result = []

		if items.get('items') != None and isinstance(items.get('items'), list):
			for i in items.get('items'):
				if i.get('id') in item_ids:
					gui['windows'][item_ids.index(i.get('id'))] = i
				else:
					gui['windows'].append(i)

				result.append(i.get('id'))

			self.updateBacktestGui(strategy_id, backtest_id, gui)
			return result
		else:
			raise AccountException('Unrecognisable body format.')


	# Private
	def _user_validation(self):
		user = self.ctrl.getDb().getUser(self.userId)
		if not user:
			raise AccountException('User does not exist.')


	def _set_broker(self, strategy_id, strategy_info):
		broker_id = strategy_info['broker']

		broker_args = {
			'ctrl': self.ctrl,
			'user_account': self,
			'strategy_id': strategy_id,
			'broker_id': broker_id,
			'accounts': strategy_info['accounts']
		}

		if broker_id != tl.broker.PAPERTRADER_NAME:
			# Update relevant broker info items
			broker_info = self.ctrl.getDb().getBroker(self.userId, broker_id)
			if broker_info:
				broker_name = broker_info.pop('broker')
				broker_args.update(broker_info)
				return self._init_broker(broker_name, broker_args)
			else:
				raise BrokerException('Broker does not exist')

		else:
			# Update relevant broker info items
			broker_args['name'] = tl.broker.IG_NAME
			return self._init_broker(tl.broker.PAPERTRADER_NAME, broker_args)

	def _init_broker(self, broker_name, broker_args):
		# Check if broker isn't already initialized
		if broker_args['strategy_id'] not in self.brokers:
			# Initialize broker
			if broker_name == tl.broker.PAPERTRADER_NAME:
				self.brokers[broker_args['strategy_id']] = tl.broker.Broker(**broker_args)
			elif broker_name == tl.broker.IG_NAME:
				self.brokers[broker_args['strategy_id']] = tl.broker.IG(**broker_args)
			elif broker_name == tl.broker.OANDA_NAME:
				self.brokers[broker_args['strategy_id']] = tl.broker.Oanda(**broker_args)

		return self.brokers[broker_args['strategy_id']]

	# Drawing Functions
	def createDrawings(self, strategy_id, layer, drawings):
		gui = self.getGui(strategy_id)
		if gui is not None and layer in gui['drawings']:
			# Validation

			# Set IDs
			existing_ids = [i.get('id') for l in gui['drawings'] for i in gui['drawings'][l]]
			new_ids = []
			for d in drawings:
				new_id = self.generateId()
				while new_id in existing_ids:
					new_id = self.generateId()
				new_ids.append(new_id)
				d['id'] = new_id

			# Update gui
			gui['drawings'][layer] += drawings

			# Send Message to web clients
			self.ctrl.sio.emit(
				'ongui', 
				{
					'type': 'create_drawings',
					'layer': layer,
					'items': drawings
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, gui)
			return new_ids

		return None


	def deleteDrawingsById(self, strategy_id, layer, drawing_ids):
		gui = self.getGui(strategy_id)
		if gui is not None and layer in gui['drawings']:
			# Validation

			# Update gui
			deleted = []
			for i in range(len(gui['drawings'][layer])-1,-1,-1):
				d = gui['drawings'][layer][i]
				if d.get('id') in drawing_ids:
					deleted.append(d['id'])
					del gui['drawings'][layer][i]

			# Send Message to web clients
			self.ctrl.sio.emit(
				'ongui', 
				{
					'type': 'delete_drawings',
					'layer': layer,
					'items': deleted
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, gui)
			return deleted

		return []


	def deleteDrawingLayer(self, strategy_id, layer):
		gui = self.getGui(strategy_id)
		if gui is not None and layer in gui['drawings']:
			# Validation

			# Update gui
			del gui['drawings'][layer]

			# Send Message to web clients
			self.ctrl.sio.emit(
				'ongui', 
				{
					'type': 'delete_drawing_layer',
					'layer': layer
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, gui)
			return layer

		return None

	def deleteAllDrawings(self, strategy_id):
		gui = self.getGui(strategy_id)
		if gui is not None:
			# Validation

			# Update gui
			deleted_layers = list(gui['drawings'].keys())
			gui['drawings'] = {}

			# Send Message to web clients
			self.ctrl.sio.emit(
				'ongui', 
				{
					'type': 'delete_all_drawings'
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, gui)
			return deleted_layers

		return None

	# Backtest Functions
	def getBacktestGui(self, strategy_id, backtest_id):
		return self.ctrl.getDb().getStrategyBacktestGui(self.userId, strategy_id, backtest_id)


	def getBacktestTransactions(self, strategy_id, backtest_id):
		return self.ctrl.getDb().getStrategyBacktestTransactions(self.userId, strategy_id, backtest_id)


	def uploadBacktest(self, strategy_id, backtest):
		return self.ctrl.getDb().createStrategyBacktest(self.userId, strategy_id, backtest)


	def performBacktest(self, strategy_id, start, end, mode, input_variables={}):
		strategy = self.getStrategyInfo(strategy_id)

		backtest_id = strategy.backtest(start, end, mode, input_variables=input_variables)
		return backtest_id


	def replaceInputVariables(self, strategy_id, input_variables):
		gui = self.getGui(strategy_id)
		gui['input_variables'] = input_variables
		self.updateGui(strategy_id, gui)
		return input_variables


	def updateInputVariables(self, strategy_id, input_variables):
		gui = self.getGui(strategy_id)

		# Process input variable changes
		if 'input_variables' in gui:
			for name in gui['input_variables']:
				if name in input_variables:
					if (
						input_variables[name]['type'] != 'header' and
						input_variables[name]['type'] == gui['input_variables'][name]['type'] and
						gui['input_variables'][name]['value'] is not None
					):
						input_variables[name]['value'] = gui['input_variables'][name]['value']

		gui['input_variables'] = input_variables
		self.updateGui(strategy_id, gui)
		return input_variables


	def compileStrategy(self, strategy_id):
		strategy = self.getStrategyInfo(strategy_id)

		properties = strategy.compile()
		self.updateInputVariables(strategy_id, properties['input_variables'])

		return properties


	# Log Functions
	def getLogs(self):
		return

	def createLog(self):
		return

	# Alert Functions
	def getAlerts(self):
		return

	def createAlert(alert):
		return

	

