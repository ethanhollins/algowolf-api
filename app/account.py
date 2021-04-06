import json, jwt
import math
import time
import string, random
import requests
from app.controller import DictQueue
from app import tradelib as tl
from threading import Thread
from app.strategy import Strategy
from app.error import AccountException, BrokerException, AuthorizationException
from copy import copy
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

		# Queues
		self._set_broker_queue = DictQueue()

	# Public
	def getAccountDetails(self):
		user = self.ctrl.getDb().getUser(self.userId)
		if not user:
			raise AccountException('User does not exist.')

		return {
			'user_id': user.get('user_id'),
			'email': user.get('email'),
			'first_name': user.get('first_name'),
			'last_name': user.get('last_name'),
			'beta_access': user.get('beta_access'),
			'notify_me': user.get('notify_me'),
			'email_confirmed': user.get('email_confirmed'),
			'brokers': list(user.get('brokers').keys()),
			'strategies': list(user.get('strategies').keys()),
			'metadata': user.get('metadata'),
			'admin': user.get('admin')
		}

	def generateId(self):
		letters = string.ascii_uppercase + string.digits
		return ''.join(random.choice(letters) for i in range(6))


	# Strategy Functions
	def startStrategy(self, strategy_id):
		if strategy_id in self.brokers: return

		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		if strategy_info is None:
			raise AccountException('Strategy not found.')

		# Handle broker info
		brokers = self._set_brokers(strategy_id, strategy_info)


	def getStrategyInfo(self, broker_id):		
		strategy = self.strategies.get(broker_id)
		if strategy is None:
			self.startStrategy(broker_id)
			strategy = self.strategies.get(broker_id)

		return strategy


	def getStrategy(self, strategy_id):
		if strategy_id not in self.brokers:
			self.startStrategy(strategy_id)
			
		# brokers = list(self.ctrl.getDb().getStrategy(self.userId, strategy_id)['brokers'].keys())

		brokers = self._set_brokers(strategy_id, None)

		broker_info = {}

		for broker_id in brokers:
			if broker_id in self.brokers:
				if self.brokers.get(broker_id).is_auth:
					broker_info[broker_id] = {
						'name': brokers[broker_id]['name'],
						'broker': brokers[broker_id]['broker'],
						'is_auth': True,
						'accounts': {
							acc: { 
								'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
								'balance': self.brokers.get(broker_id).getAccountInfo(acc)[acc].get('balance'),
								**brokers.get(broker_id)['accounts'][acc]
							}
							for acc in brokers.get(broker_id)['accounts']
						},
						'positions': self.brokers.get(broker_id).getAllPositions(),
						'orders': self.brokers.get(broker_id).getAllOrders()
					}

				else:
					broker_info[broker_id] = {
						'name': brokers[broker_id]['name'],
						'broker': brokers[broker_id]['broker'],
						'is_auth': False,
						'accounts': {
							acc: { 
								'strategy_status': False,
								'balance': 0,
								**brokers.get(broker_id)['accounts'][acc]
							}
							for acc in brokers.get(broker_id)['accounts']
						},
						'positions': [],
						'orders': []
					}

		return {
			'strategy_id': strategy_id,
			'brokers': broker_info
		}


	def createStrategy(self, info):
		strategy = {
			'name': info.get('name'),
			'brokers': info.get('brokers'),
			'keys': [],
			'package': info.get('package')
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


	def _set_running(self, strategy_id, broker_id, account_id, script_id, input_variables):
		if self.ctrl.app.config['RESTART_SCRIPTS_ON_STARTUP']:
			user = self.ctrl.getDb().getUser(self.userId)
			user_brokers = user.get('brokers')

			if not isinstance(user['strategies'][strategy_id].get('running'), dict):
				user['strategies'][strategy_id]['running'] = {}
			if not isinstance(user['strategies'][strategy_id]['running'].get(broker_id), dict):
				user['strategies'][strategy_id]['running'][broker_id] = {}
			if not isinstance(user['strategies'][strategy_id]['running'][broker_id].get(account_id), dict):
				user['strategies'][strategy_id]['running'][broker_id][account_id] = {}

			user['strategies'][strategy_id]['running'][broker_id][account_id]['script_id'] = script_id
			user['strategies'][strategy_id]['running'][broker_id][account_id]['input_variables'] = input_variables

			# Clean user running
			for broker_id in copy(list(user['strategies'][strategy_id]['running'].keys())):
				if broker_id != strategy_id and broker_id not in user_brokers:
					del user['strategies'][strategy_id]['running'][broker_id]

			# Update user running
			self.ctrl.getDb().updateUser(self.userId, { 'strategies': user['strategies'] })


	def isScriptRunning(self, strategy_id, broker_id, account_id):
		user = self.ctrl.getDb().getUser(self.userId)
		user_running_dict = user['strategies'][strategy_id].get('running')

		is_user_running = None
		if user_running_dict is not None and broker_id in user_running_dict:
			if account_id in user_running_dict[broker_id]:
				if isinstance(user_running_dict[broker_id][account_id], dict) and 'script_id' in user_running_dict[broker_id][account_id]:
					is_user_running = user_running_dict[broker_id][account_id]['script_id']


		payload = {
			'user_id': self.userId,
			'broker_id': broker_id,
			'account_id': account_id
		}

		url = self.ctrl.app.config.get('LOADER_URL')
		endpoint = '/running'
		res = requests.get(
			url + endpoint,
			data=json.dumps(payload)
		)

		is_process_running = res.json().get('running')
		input_variables = res.json().get('input_variables')

		if is_user_running != is_process_running:
			self._set_running(
				strategy_id, broker_id, account_id, is_process_running, input_variables
			)

		return is_process_running


	def isAnyScriptRunning(self):
		user = self.ctrl.getDb().getUser(self.userId)

		if 'strategies' in user:
			for strategy_id in user['strategies']:
				if 'running' in user['strategies'][strategy_id]:
					for broker_id in user['strategies'][strategy_id]['running']:
						for account_id in user['strategies'][strategy_id]['running'][broker_id]:
							if (
								isinstance(user['strategies'][strategy_id]['running'][broker_id][account_id], dict) and 
								user['strategies'][strategy_id]['running'][broker_id][account_id].get('script_id')
							):
								return True

		return False



	def runStrategyScript(self, strategy_id, broker_id, accounts, input_variables):
		strategy = self.getStrategyInfo(broker_id)

		Thread(target=strategy.run, args=(accounts, input_variables)).start()
		return strategy.package


	def _runStrategyScript(self, strategy_id, broker_id, accounts, input_variables):
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)

		package = strategy_info['package']
		script_id = package.split('.')[0]
		version = package.split('.')[1]

		if input_variables is None:
			input_variables = {}

		session_key = self.generateSessionToken()

		payload = {
			'user_id': self.userId,
			'strategy_id': strategy_id,
			'broker_id': broker_id,
			'accounts': accounts,
			'auth_key': session_key,
			'input_variables': input_variables,
			'script_id': script_id,
			'version': version
		}

		# self.ctrl.emit(
		# 	'start', 
		# 	payload,
		# 	namespace='/admin'
		# )


		url = self.ctrl.app.config.get('LOADER_URL')
		endpoint = '/start'
		res = requests.post(
			url + endpoint,
			data=json.dumps(payload)
		)

		if res.status_code == 200:
			for account_id in accounts:
				self._set_running(strategy_id, broker_id, account_id, script_id, input_variables)
			return True
		else:
			return False


	def stopStrategyScript(self, broker_id, accounts):
		strategy = self.strategies.get(broker_id)
		if strategy is not None:
			strategy.stop(accounts)
			return strategy.package
		else:
			return None


	def _stopStrategyScript(self, strategy_id, broker_id, accounts):

		payload = {
			'user_id': self.userId,
			'broker_id': broker_id,
			'accounts': accounts
		}

		# self.ctrl.emit(
		# 	'stop', 
		# 	payload,
		# 	namespace='/admin'
		# )

		url = self.ctrl.app.config.get('LOADER_URL')
		endpoint = '/stop'
		res = requests.post(
			url + endpoint,
			data=json.dumps(payload)
		)

		if res.status_code == 200:
			for account_id in accounts:
				self._set_running(strategy_id, broker_id, account_id, None, None)
			return True
		else:
			return False


	def updateStrategyPackage(self, strategy_id, new_package):
		# Retrieve strategy
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		strategy_info['package'] = new_package

		# Perform update
		self.ctrl.getDb().updateStrategy(self.userId, strategy_id, strategy_info)


	def _set_strategy(self, strategy_id, broker_id, api, package):
		strategy = self.strategies.get(broker_id)
		if strategy is None:
			strategy = Strategy(strategy_id, broker_id, api, package)
			self.strategies[broker_id] = strategy
		return self.strategies[broker_id]


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


	def createBroker(self, broker_id, name, broker_name, **props):
		return self.ctrl.getDb().createBroker(self.userId, broker_id, name, broker_name, props)


	def updateBroker(self, broker_id, props):
		return self.ctrl.getDb().updateBroker(self.userId, broker_id, props);


	def updateBrokerName(self, old_name, new_name):
		result = self.ctrl.getDb().updateBrokerName(self.userId, old_name, new_name)
		return result


	def deleteBroker(self, name):
		result = self.ctrl.getDb().deleteBroker(self.userId, name)
		return result


	def getAccountInfo(self, strategy_id, account_code):
		script_id = self.getScriptId(strategy_id)

		result = {}
		result.update(self.getAccountGui(strategy_id, account_code))
		result.update(self.getAccountTransactions(strategy_id, account_code))
		result['input_variables'] = self.getAccountInputVariables(strategy_id, account_code, script_id)
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
		EXP_TIME = 60 * 60 * 12
		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time()), 'exp': time.time() + EXP_TIME
		}
		return jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')

	def generatePermanentToken(self):
		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time())
		}
		return jwt.encode(payload, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')

	def generateSessionToken(self):
		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time())
		}
		return jwt.encode(payload, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')


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
		

	def getStrategyGui(self, strategy_id):
		return self.ctrl.getDb().getStrategyGui(self.userId, strategy_id)


	def updateStrategyGui(self, strategy_id, new_gui):
		self.ctrl.getDb().updateStrategyGui(self.userId, strategy_id, new_gui)


	def getAccountGui(self, strategy_id, account_code):
		return self.ctrl.getDb().getAccountGui(self.userId, strategy_id, account_code)


	def getAccountTransactions(self, strategy_id, account_code):
		return self.ctrl.getDb().getAccountTransactions(self.userId, strategy_id, account_code)


	def getAccountReport(self, strategy_id, account_code, name):
		return self.ctrl.getDb().getStrategyAccountReport(self.userId, strategy_id, account_code, name)


	def updateAccountGui(self, strategy_id, account_code, new_gui):
		self.ctrl.getDb().updateAccountGui(self.userId, strategy_id, account_code, new_gui)


	def appendAccountGui(self, strategy_id, account_code, new_gui):
		self.ctrl.getDb().appendAccountGui(self.userId, strategy_id, account_code, new_gui)		


	def updateStrategyAccount(self, strategy_id, account_code):
		gui = self.getStrategyGui(strategy_id)
		gui['account'] = account_code
		self.updateStrategyGui(strategy_id, gui)


	def updateStrategyGuiItems(self, strategy_id, items):
		gui = self.getStrategyGui(strategy_id)
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

		self.updateStrategyGui(strategy_id, gui)
		return result


	def createStrategyGuiItem(self, strategy_id, item):
		gui = self.getStrategyGui(strategy_id)
		gui_ids = [i['id'] for i in gui['windows']]

		# Make sure id is unique
		item['id'] = self.generateId()
		while item['id'] in gui_ids:
			item['id'] = self.generateId()

		gui['windows'].push(item)
		self.updateStrategyGui(strategy_id, gui)
		return item['id']


	def deleteStrategyGuiItems(self, strategy_id, items):
		gui = self.getStrategyGui(strategy_id)
		item_ids = [i['id'] for i in gui['windows']]
		result = []

		if items.get('windows') != None and isinstance(items.get('windows'), list):
			for i in items.get('windows'):
				if i in item_ids:
					del gui['windows'][item_ids.index(i)]
					result.append(i)

		self.updateStrategyGui(strategy_id, gui)
		return result


	def getScriptId(self, strategy_id):
		# Get Script Id
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		return strategy_info['package'].split('.')[0]


	def getStrategyInputVariables(self, strategy_id, script_id, update=True):
		default_vars = self.ctrl.getDb().getScriptInputVariables(script_id)
		strategy_vars = self.ctrl.getDb().getStrategyInputVariables(self.userId, strategy_id, script_id)

		if len(strategy_vars) == 0:
			strategy_vars['Preset 1'] = {}

		result = {}
		for preset in strategy_vars:
			result[preset] = {}
			for k in default_vars:
				if default_vars[k].get('scope') == 'global':
					result[preset][k] = default_vars[k]

				if (
					k in strategy_vars[preset] and
					default_vars[k].get('scope') == strategy_vars[preset][k].get('scope') and
					default_vars[k].get('type') == strategy_vars[preset][k].get('type')
				):
					result[preset][k]['value'] = strategy_vars[preset][k]['value']
					if 'properties' in result[preset][k] and 'enabled' in result[preset][k]['properties']:
						result[preset][k]['properties']['enabled'] = strategy_vars[preset][k]['properties']['enabled']

		if update:
			self.updateStrategyInputVariables(strategy_id, script_id, result)
		return result


	def updateStrategyInputVariables(self, strategy_id, script_id, new_vars):		
		return self.ctrl.getDb().updateStrategyInputVariables(self.userId, strategy_id, script_id, new_vars)


	def getAccountInputVariables(self, strategy_id, account_code, script_id, update=True):
		default_vars = self.ctrl.getDb().getScriptInputVariables(script_id)
		account_vars = self.ctrl.getDb().getAccountInputVariables(self.userId, strategy_id, account_code, script_id)

		if len(account_vars) == 0:
			account_vars['Preset 1'] = {}

		result = {}
		for preset in account_vars:
			result[preset] = {}
			for k in default_vars:
				if default_vars[k].get('scope') == 'local':
					result[preset][k] = default_vars[k]

				if (
					k in account_vars[preset] and
					default_vars[k].get('scope') == account_vars[preset][k].get('scope') and
					default_vars[k].get('type') == account_vars[preset][k].get('type')
				):
					result[preset][k]['value'] = account_vars[preset][k]['value']
					if 'properties' in result[preset][k] and 'enabled' in result[preset][k]['properties']:
						result[preset][k]['properties']['enabled'] = account_vars[preset][k]['properties']['enabled']

		if update:
			self.updateAccountInputVariables(strategy_id, account_code, script_id, result)
		return result


	def updateAccountInputVariables(self, strategy_id, script_id, account_code, new_vars):		
		return self.ctrl.getDb().updateAccountInputVariables(self.userId, strategy_id, script_id, account_code, new_vars)


	def getBacktestInfo(self, strategy_id, backtest_id):
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		# broker_info = self.ctrl.getDb().getBroker(self.userId, strategy_info['brokers'][strategy_id].get('broker'))
		gui = self.getBacktestGui(strategy_id, backtest_id)
		return gui


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


	def getBacktestReport(self, strategy_id, backtest_id, name):
		return self.ctrl.getDb().getStrategyBacktestReport(self.userId, strategy_id, backtest_id, name)


	def updateBacktestReport(self, strategy_id, backtest_id, name, obj):
		return self.ctrl.getDb().updateStrategyBacktestReport(self.userId, strategy_id, backtest_id, name, obj)



	# Private
	def _user_validation(self):
		user = self.ctrl.getDb().getUser(self.userId)
		if not user:
			raise AccountException('User does not exist.')


	def _set_brokers(self, strategy_id, strategy_info):
		# brokers = {
		# 	**strategy_info['brokers'],
		# 	**{ strategy_id: [tl.broker.PAPERTRADER_NAME] }
		# }

		# print(brokers)

		# brokers = { k: list(v['accounts'].keys()) for k, v in self.getAllBrokers().items()}
		brokers = self.getAllBrokers()
		brokers = {
			**brokers,
			**{ 
				strategy_id: { 
					'name': 'Paper Trader',
					'broker': 'papertrader',
					'accounts': {
						tl.broker.PAPERTRADER_NAME: {
							'active': True,
							'nickname': ''
						} 
					}
				} 
			}
		}
		for broker_id in brokers:
			self._set_broker_queue.handle(
				broker_id,
				self._perform_set_broker, 
				strategy_id, broker_id, 
				brokers[broker_id]
			)

			broker = self.brokers.get(broker_id)
			if broker is not None:
				if tl.broker.PAPERTRADER_NAME in broker.getAccounts():
					self.updateTrades(
						strategy_id,
						broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
						broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
					)

		return brokers


	def _perform_set_broker(self, strategy_id, broker_id, broker_info):
		if broker_id not in self.brokers:
			broker_args = {
				'ctrl': self.ctrl,
				'user_account': self,
				'strategy_id': strategy_id,
				'broker_id': broker_id,
				'accounts': list(broker_info['accounts'].keys())
			}

			# Update relevant broker info items
			if broker_id == strategy_id:
				broker_name = tl.broker.PAPERTRADER_NAME
				broker_args.update({
					'name': tl.broker.OANDA_NAME,
					'display_name': 'Paper Trader',
					'is_dummy': False
				})
				self._init_broker(broker_name, broker_args)

			else:
				broker_info = self.ctrl.getDb().getBroker(self.userId, broker_id)
				if broker_info:
					broker_info['display_name'] = broker_info['name']
					del broker_info['name']
					broker_name = broker_info.pop('broker')
					broker_args.update(broker_info)
					self._init_broker(broker_name, broker_args)
				else:
					raise BrokerException('Broker does not exist')
		else:
			print('ALREADY DONE')


	def _init_broker(self, broker_name, broker_args):
		# Check if broker isn't already initialized
		if broker_args['broker_id'] not in self.brokers:
			# Initialize broker
			if broker_name == tl.broker.PAPERTRADER_NAME:
				self.brokers[broker_args['broker_id']] = tl.broker.Broker(**broker_args)
			elif broker_name == tl.broker.IG_NAME:
				self.brokers[broker_args['broker_id']] = tl.broker.IG(**broker_args)
			elif broker_name == tl.broker.OANDA_NAME:
				self.brokers[broker_args['broker_id']] = tl.broker.Oanda(**broker_args)
			elif broker_name == tl.broker.SPOTWARE_NAME:
				self.brokers[broker_args['broker_id']] = tl.broker.Spotware(**broker_args)

		return self.brokers[broker_args['broker_id']]

	# Drawing Functions
	def createDrawings(self, strategy_id, layer, drawings):
		gui = self.getGui(strategy_id, account_code)
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
			self.ctrl.emit(
				'ongui', 
				{
					'type': 'create_drawings',
					'layer': layer,
					'items': drawings
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, account_code, gui)
			return new_ids

		return None


	def deleteDrawingsById(self, strategy_id, layer, drawing_ids):
		gui = self.getGui(strategy_id, account_code)
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
			self.ctrl.emit(
				'ongui', 
				{
					'type': 'delete_drawings',
					'layer': layer,
					'items': deleted
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, account_code, gui)
			return deleted

		return []


	def deleteDrawingLayer(self, strategy_id, layer):
		gui = self.getGui(strategy_id, account_code)
		if gui is not None and layer in gui['drawings']:
			# Validation

			# Update gui
			del gui['drawings'][layer]

			# Send Message to web clients
			self.ctrl.emit(
				'ongui', 
				{
					'type': 'delete_drawing_layer',
					'layer': layer
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, account_code, gui)
			return layer

		return None

	def deleteAllDrawings(self, strategy_id):
		gui = self.getGui(strategy_id, account_code)
		if gui is not None:
			# Validation

			# Update gui
			deleted_layers = list(gui['drawings'].keys())
			gui['drawings'] = {}

			# Send Message to web clients
			self.ctrl.emit(
				'ongui', 
				{
					'type': 'delete_all_drawings'
				},
				namespace='/admin'
			)

			# Update Gui Storage
			self.updateGui(strategy_id, account_code, gui)
			return deleted_layers

		return None

	# Backtest Functions
	def getBacktestGui(self, strategy_id, backtest_id):
		return self.ctrl.getDb().getStrategyBacktestGui(self.userId, strategy_id, backtest_id)

	def getBacktestChartInfo(self, strategy_id, backtest_id):
		return self.ctrl.getDb().getStrategyBacktestInfo(self.userId, strategy_id, backtest_id)

	def getBacktestTransactions(self, strategy_id, backtest_id):
		return self.ctrl.getDb().getStrategyBacktestTransactions(self.userId, strategy_id, backtest_id)


	def uploadBacktest(self, strategy_id, backtest):
		return self.ctrl.getDb().createStrategyBacktest(self.userId, strategy_id, backtest)


	def uploadLiveBacktest(self, strategy_id, broker_id, account_id, backtest):
		return self.ctrl.getDb().createAccountBacktest(self.userId, strategy_id, broker_id, account_id, backtest)


	def performBacktest(self, strategy_id, broker, start, end, auth_key, input_variables, spread):
		# strategy = self.getStrategyInfo(strategy_id)
		# backtest_id = strategy.backtest(start, end, mode, input_variables=input_variables)

		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)

		package = strategy_info['package']
		script_id = package.split('.')[0]
		version = package.split('.')[1]

		if input_variables is None:
			input_variables = {}

		payload = {
			'user_id': self.userId,
			'strategy_id': strategy_id,
			'auth_key': auth_key,
			'input_variables': input_variables,
			'script_id': script_id,
			'version': version,
			'broker': broker,
			'start': start,
			'end': end,
			'spread': spread
		}

		url = self.ctrl.app.config.get('LOADER_URL')
		endpoint = '/backtest'
		res = requests.post(
			url + endpoint,
			data=json.dumps(payload)
		)


		return


	def replaceStrategyInputVariables(self, strategy_id, input_variables):
		# Retrieve
		script_id = self.getScriptId(strategy_id)
		global_vars = self.getStrategyInputVariables(strategy_id, script_id, update=False)

		for preset in input_variables:
			global_vars[preset] = input_variables[preset]

		# Update
		self.updateStrategyInputVariables(strategy_id, script_id, global_vars)

		return input_variables


	def replaceAccountInputVariables(self, strategy_id, account_code, input_variables):
		# Retrieve
		script_id = self.getScriptId(strategy_id)
		local_vars = self.getAccountInputVariables(strategy_id, account_code, script_id, update=False)

		for preset in input_variables:
			local_vars[preset] = input_variables[preset]

		# Update
		self.updateAccountInputVariables(strategy_id, account_code, script_id, local_vars)

		return input_variables


	# def updateInputVariables(self, strategy_id, account_code, script_id, input_variables):
	# 	global_vars = self.getStrategyInputVariables(strategy_id, script_id)
	# 	local_vars = self.getAccountInputVariables(strategy_id, account_code, script_id)

	# 	# Process input variable changes
	# 	for name in global_vars:
	# 		if name in input_variables['global']:
	# 			if (
	# 				input_variables['global'][name]['type'] != 'header' and
	# 				input_variables['global'][name]['type'] == global_vars[name]['type'] and
	# 				global_vars[name]['value'] is not None
	# 			):
	# 				input_variables['global'][name]['value'] = global_vars[name]['value']

	# 	for name in local_vars:
	# 		if name in input_variables['local']:
	# 			if (
	# 				input_variables['local'][name]['type'] != 'header' and
	# 				input_variables['local'][name]['type'] == local_vars[name]['type'] and
	# 				local_vars[name]['value'] is not None
	# 			):
	# 				input_variables['local'][name]['value'] = local_vars[name]['value']

	# 	global_vars = input_variables['global']
	# 	local_vars = input_variables['local']
	# 	self.updateStrategyInputVariables(strategy_id, script_id, global_vars)
	# 	self.updateAccountInputVariables(strategy_id, account_code, script_id, local_vars)
	# 	return input_variables


	# def compileStrategy(self, strategy_id):
	# 	strategy = self.getStrategyInfo(strategy_id)

	# 	properties = strategy.compile()
	# 	self.updateInputVariables(strategy_id, account_code, script_id, properties['input_variables'])

	# 	return properties


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

	

