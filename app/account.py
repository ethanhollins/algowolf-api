import json, jwt
import math
import time
import string, random
import requests
import shortuuid
import traceback
import zmq
import stripe
from copy import deepcopy
from datetime import datetime
from app.controller import DictQueue
from app import tradelib as tl
from threading import Thread
from app.strategy import Strategy
from app.error import AccountException, BrokerException, AuthorizationException
from copy import copy
from flask import current_app

class Account(object):
	'''Stores user account information and provides account functionality.

	Stores account information required for immediate memory access to speed up
	API calls. Provides account functionality in relation to general account
	operations. These include database operations, initializing strategies, starting scripts,
	managing broker operations and website related operations.

	Attributes:
		ctrl: A reference to the Controller object.
		userId: A string containing the user's user ID.
		brokers: A dictionary storing Broker objects by their broker ID.
		strategies: A dictionary storing Strategy objects by their strategy ID (deprecated).
		keys: A dictionary storing API keys by their related strategy ID (deprecated).
		_replace_broker: A dictionary containing user brokers in the process of being replaced. 
		_queue: A list to ensure synchronous execution of certain code exerts.
		_set_broker_queue: A DictQueue to ensure synchronous execution of Broker initialization.
	'''


	def __init__(self, ctrl, userId):
		"""Inits Account object and validates the user ID exists in the database.

		Args:
			ctrl:  A reference to the Controller object.
			userId: A string container the user's user ID.
		"""
		# Check user existance
		self.ctrl = ctrl
		self.userId = userId
		self.brokers = {}
		self.strategies = {}
		self.keys = {}

		self._user_validation()

		self._replace_broker = {}

		# Queues
		self._queue = []
		self._set_broker_queue = DictQueue()


	def getAccountDetails(self):
		'''Retrieves account info from the database.

		Retrieves user database entry where required information is returned.

		Returns:
			A dict mapping chosen user entry items and their respective keys. Some
			information is modified for usefulness.
		'''

		user = self.ctrl.getDb().getUser(self.userId)
		if not user:
			raise AccountException('User does not exist.')

		return {
			'user_id': user.get('user_id'),
			'email': user.get('email'),
			'first_name': user.get('first_name'),
			'last_name': user.get('last_name'),
			'beta_access': user.get('beta_access'),
			'notify_me': not user.get('email_opt_out'),
			'email_confirmed': user.get('email_confirmed'),
			'brokers': list(user.get('brokers').keys()),
			'strategies': user.get('strategies'),
			'metadata': user.get('metadata'),
			'admin': user.get('admin')
		}


	def generateReference(self):
		'''Generates a shortened uuid reference.

		Returns:
			A shortened uuid reference string. 
		'''

		return shortuuid.uuid()


	def generateId(self):
		'''Generates a 6 character ID of capital letters and numbers.

		Returns:
			6 character ID of capital letters and numbers.
		'''
		letters = string.ascii_uppercase + string.digits
		return ''.join(random.choice(letters) for i in range(6))


	# Strategy Functions
	def startStrategy(self, strategy_id):
		'''Initializes Broker objects associated with a strategy_id.

		Initializes all Broker objects associated with a strategy_id retrieved from the
		database. Uses redis to determine if this function has already been called for a
		particular strategy_id to avoid duplicate Broker objects due to potential simultaneous
		calls to this function. Also sends a message to other running API processes to initialize
		their own Broker objects.

		Args:
			strategy_id: A string containing the ID of the user strategy.
		'''

		brokers = {
			**self.getAllBrokers(),
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

		if self.ctrl.redis_client.exists("strategies_" + str(self.ctrl.connection_id)):
			started_strategies = json.loads(self.ctrl.redis_client.get("strategies_" + str(self.ctrl.connection_id)))
			print(f"[startStrategy] {started_strategies}", flush=True)
			if strategy_id in started_strategies:
				existing_brokers = [broker_id in started_strategies[strategy_id] for broker_id in brokers]
				if all(existing_brokers):
					print(f"[startStrategy] SKIP {strategy_id}", flush=True)
					return
		else:
			started_strategies = {}

		started_strategies[strategy_id] = list(brokers.keys())
		print(f"[startStrategy] STARTED: ({strategy_id}) {started_strategies}", flush=True)
		self.ctrl.redis_client.set("strategies_" + str(self.ctrl.connection_id), json.dumps(started_strategies))

		# if strategy_id in self.brokers: return

		print(f"[startStrategy] SEND START: {self.userId}, {strategy_id}", flush=True)

		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		if strategy_info is None:
			raise AccountException('Strategy not found.')

		self.ctrl._send_queue.append({
			"type": "start_strategy", 
			"message": {
				"user_id": self.userId,
				"strategy_id": strategy_id
			}
		})

		# Handle broker info
		brokers = self._set_brokers(strategy_id, strategy_info)

	
	def startStrategyBroker(self, strategy_id, broker_id):
		'''Initializes Broker objects associated with a strategy_id if broker_id
		is not initialized.
		
		Checks if broker_id is initialized and if not calls startStategy(strategy_id)
		to initialize all associated Brokers with strategy_id.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
		'''

		if self.ctrl.redis_client.exists("strategies_" + str(self.ctrl.connection_id)):
			started_strategies = json.loads(self.ctrl.redis_client.get("strategies_" + str(self.ctrl.connection_id)))
			print(f"[startStrategyBroker] {started_strategies}", flush=True)
			if strategy_id in started_strategies:
				if broker_id in started_strategies[strategy_id]:
					print(f"[startStrategyBroker] SKIP {strategy_id}", flush=True)
					return
		else:
			started_strategies = {}

		self.startStrategy(strategy_id)

		# if not broker_id in self.brokers:
		# 	strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		# 	if strategy_info is None:
		# 		raise AccountException('Strategy not found.')

		# 	self.ctrl._send_queue.append({
		# 		"type": "start_strategy", 
		# 		"message": {
		# 			"user_id": self.userId,
		# 			"strategy_id": strategy_id
		# 		}
		# 	})

		# 	# Handle broker info
		# 	brokers = self._set_brokers(strategy_id, strategy_info)


	def getStrategyInfo(self, strategy_id):
		'''Retrieves Strategy object from strategies.

		Initializes Strategy object if not yet initialized and returns
		Strategy object.

		Args:
			strategy_id: A string containing the ID of the user strategy.

		Returns:
			Strategy object mapped to strategy_id.
		'''

		strategy = self.strategies.get(strategy_id)
		if strategy is None:
			self.startStrategy(strategy_id)
			strategy = self.strategies.get(strategy_id)

		return strategy


	def getStrategy(self, strategy_id):
		'''Generates a dictionary containing strategy information.

		Initializes strategy_id Brokers if not yet initialized and generates a dict
		containing strategy information including database items as well as live
		information (e.g. broker authorization status, positions etc.) and broker
		information.

		Args:
			strategy_id: A string containing the ID of the user strategy.

		Returns:
			A dict with strategy information, live information and broker information.
		'''

		if strategy_id not in self.brokers:
			self.startStrategy(strategy_id)
			
		# brokers = list(self.ctrl.getDb().getStrategy(self.userId, strategy_id)['brokers'].keys())

		brokers = self._set_brokers(strategy_id, None)

		broker_info = {}

		for broker_id in brokers:
			if broker_id in self.brokers:

				broker_info[broker_id] = {
					'name': brokers[broker_id]['name'],
					'broker': brokers[broker_id]['broker'],
					'is_auth': True,
					'accounts': {},
					'positions': [],
					'orders': []
				}

				try:
					self.brokers.get(broker_id).authCheck()
				except Exception:
					pass

				print(f"[getStrategy] ({broker_id}) {self.brokers.get(broker_id).is_auth}", flush=True)
				if self.brokers.get(broker_id).is_auth:
					try:
						for acc in brokers.get(broker_id)['accounts']:
							broker_info[broker_id]['accounts'][acc] = { 
								'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
								'balance': self.brokers.get(broker_id).getAccountInfo(acc)[acc].get('balance'),
								**brokers.get(broker_id)['accounts'][acc]
							}

						broker_info[broker_id]['positions'] = self.brokers.get(broker_id).getAllPositions()
						broker_info[broker_id]['orders'] = self.brokers.get(broker_id).getAllOrders()

					except Exception as e:
						print(traceback.format_exc())

						if tl.isWeekend(datetime.utcnow()):
							broker_info[broker_id]['is_auth'] = True
						else:
							broker_info[broker_id]['is_auth'] = False
						
						broker_info[broker_id]['accounts'] = {}
						broker_info[broker_id]['positions'] = []
						broker_info[broker_id]['orders'] = []
						self.brokers.get(broker_id).is_auth = False

						for acc in brokers.get(broker_id)['accounts']:
							broker_info[broker_id]['accounts'][acc] = { 
								'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
								'balance': 0,
								**brokers.get(broker_id)['accounts'][acc]
							}
							

				else:
					if tl.isWeekend(datetime.utcnow()):
						broker_info[broker_id]['is_auth'] = True
					else:
						broker_info[broker_id]['is_auth'] = False
					
					for acc in brokers.get(broker_id)['accounts']:
						broker_info[broker_id]['accounts'][acc] = { 
							'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
							'balance': 0,
							**brokers.get(broker_id)['accounts'][acc]
						}

		return {
			'strategy_id': strategy_id,
			'brokers': broker_info
		}


	def getStrategyByBrokerId(self, strategy_id, broker_id):
		'''Generates a dictionary containing strategy information related to broker_id.

		Initializes strategy_id if broker_id's associated Broker object is not yet initialized 
		and generates a dict containing strategy information including database items as well 
		as live information (e.g. broker authorization status, positions etc.) and broker
		information.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.

		Returns:
			A dict with strategy information, live information and broker information.
		'''

		print(f"[getStrategyByBrokerId] 1", flush=True)
		if broker_id not in self.brokers:
			print(f"[getStrategyByBrokerId] 2", flush=True)
			self.getStrategy(strategy_id)
		print(f"[getStrategyByBrokerId] 3", flush=True)

		broker_info = {}
		if broker_id in self.brokers:
			brokers = {
				**self.getAllBrokers(),
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

			broker_info[broker_id] = {
				'name': brokers[broker_id]['name'],
				'broker': brokers[broker_id]['broker'],
				'is_auth': True,
				'accounts': {},
				'positions': [],
				'orders': []
			}

			try:
				self.brokers.get(broker_id).authCheck()
			except Exception:
				pass

			if self.brokers.get(broker_id).is_auth:
				try:
					for acc in brokers.get(broker_id)['accounts']:
						broker_info[broker_id]['accounts'][acc] = { 
							'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
							'balance': self.brokers.get(broker_id).getAccountInfo(acc)[acc].get('balance'),
							**brokers.get(broker_id)['accounts'][acc]
						}

					broker_info[broker_id]['positions'] = self.brokers.get(broker_id).getAllPositions()
					broker_info[broker_id]['orders'] = self.brokers.get(broker_id).getAllOrders()

				except Exception as e:

					if tl.isWeekend(datetime.utcnow()):
						broker_info[broker_id]['is_auth'] = True
					else:
						broker_info[broker_id]['is_auth'] = False
					
					broker_info[broker_id]['accounts'] = {}
					broker_info[broker_id]['positions'] = []
					broker_info[broker_id]['orders'] = []
					self.brokers.get(broker_id).is_auth = False

					for acc in brokers.get(broker_id)['accounts']:
						broker_info[broker_id]['accounts'][acc] = { 
							'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
							'balance': 0,
							**brokers.get(broker_id)['accounts'][acc]
						}
						

			else:
				if tl.isWeekend(datetime.utcnow()):
					broker_info[broker_id]['is_auth'] = True
				else:
					broker_info[broker_id]['is_auth'] = False
				
				for acc in brokers.get(broker_id)['accounts']:
					broker_info[broker_id]['accounts'][acc] = { 
						'strategy_status': self.isScriptRunning(strategy_id, broker_id, acc),
						'balance': 0,
						**brokers.get(broker_id)['accounts'][acc]
					}
		print(f"[getStrategyByBrokerId] 4", flush=True)

		return {
			'strategy_id': strategy_id,
			'brokers': broker_info
		}


	def createStrategy(self, info):
		'''Initializes strategy database entry and storage.

		Operations are queued so that createStrategy can only be called
		synchronously to avoid duplication. Calls the DB createStrategy function
		which creates a strategy database entry and storage items.
		
		Args:
			info: A dict of information regarding the strategy being created.

		Returns:
			A string containing the ID of the user strategy.
		'''
		
		queue_id = self.generateReference()
		self._queue.append(queue_id)
		while self._queue.index(queue_id) > 0: pass

		try:
			# strategy = {
			# 	'name': info.get('name'),
			# 	'package': info.get('package')
			# }
			strategy_id = self.ctrl.getDb().createStrategy(self.userId, info)
		except Exception:
			pass
		finally:
			del self._queue[0]

		return strategy_id


	def updateStrategy(self, strategy_id, update):
		'''Updates strategy database entry associated with strategy_id.

		Calls DB updateStrategy to update strategy database entry associated 
		with strategy_id.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			update: A dict mapping database keys to be changed with the change.
		Returns: 
			A boolean representing success or failure of the update.
		'''

		return self.ctrl.getDb().updateStrategy(self.userId, strategy_id, update)


	def deleteStrategy(self, strategy_id):
		'''Deletes strategy database entry and strategy storage.

		Calls DB deleteStrategy to delete strategy database entry and strategy storage.
		Deletes any in-memory storage of associated Broker objects.

		Args:
			strategy_id: A string containing the ID of the user strategy.
		'''
		
		self.ctrl.getDb().deleteStrategy(self.userId, strategy_id)
		broker = self.brokers.get(strategy_id)
		if broker is not None:
			del self.brokers[strategy_id]


	def strategyExists(self, strategy_id):
		'''Checks strategy_id is in brokers dict
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
		'''

		return strategy_id in self.brokers


	def updateStrategyStatus(self, strategy_id, accounts):
		'''Updates the accounts information in strategy database entry.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			accounts: A dict containing updates to accounts information.
		'''
		
		# Retrieve strategy
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		strategy_info['accounts'] = accounts

		# Perform update
		self.ctrl.getDb().updateStrategy(self.userId, strategy_id, strategy_info)


	def _set_running(self, strategy_id, broker_id, account_id, script_id, input_variables):
		'''Sets an account_id to have running status under a strategy and broker in the database.
		
		Sets an account_id to have running status under a strategy and broker in the database. The
		associated script_id and running input_variables are also saved in this entry. This is blocked
		by a configuration setting RESTART_SCRIPTS_ON_STARTUP if False.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			account_id: A string containing the ID of the user broker account.
			script_id: A string containing the ID of the user script.
			input_variables: A dict containing the input variables configuration running on the script.
		'''
		
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
		'''Checks if script status of an account is running.

		Checks database entry to see if script status of an account is running or the
		script process is running. If the process is running and the database
		status is not running, the database is updated.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			account_id: A string containing the ID of the user broker account.
		Returns:
			A boolean representing the script running status of the account.		
		'''

		user = self.ctrl.getDb().getUser(self.userId)
		user_running_dict = user['strategies'][strategy_id].get('running')

		server = 0
		if user.get("server") is not None:
			server = user.get("server")
		
		is_user_running = None
		if user_running_dict is not None and broker_id in user_running_dict:
			if account_id in user_running_dict[broker_id]:
				if isinstance(user_running_dict[broker_id][account_id], dict) and 'script_id' in user_running_dict[broker_id][account_id]:
					is_user_running = user_running_dict[broker_id][account_id]['script_id']

		if server == self.ctrl.app.config["SERVER"]:
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

			if not is_user_running and is_user_running != is_process_running:
				self._set_running(
					strategy_id, broker_id, account_id, is_process_running, input_variables
				)

			return is_user_running or is_process_running

		else:
			return is_user_running


	def isAnyScriptRunning(self):
		'''Checks if there are any running scripts on the user account.

		Returns:
			A boolean representing if there are any running scripts.		
		'''

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

	
	def getNumScriptsRunning(self):
		'''Retrieves the number of concurrent running scripts on the user account.

		Iteratively counts the number of running scripts according the user's database entry.
		
		Returns:
			An integer representing the number of concurrent running scripts on the user account.
		'''

		user = self.ctrl.getDb().getUser(self.userId)

		count = 0
		if 'strategies' in user:
			for strategy_id in user['strategies']:
				if 'running' in user['strategies'][strategy_id]:
					for broker_id in user['strategies'][strategy_id]['running']:
						for account_id in user['strategies'][strategy_id]['running'][broker_id]:
							if (
								isinstance(user['strategies'][strategy_id]['running'][broker_id][account_id], dict) and 
								user['strategies'][strategy_id]['running'][broker_id][account_id].get('script_id')
							):
								count += 1

		return count


	def getMaximumBanks(self, strategy_id, broker_id, account_id, script_id):
		'''Gets the Allocated Bank sum of the user's accounts.

		Iteratively retrieves the user's input variables for each active account on a given
		script and sums the Allocated Bank figures. The default figure is used when a saved
		value is not present in the input variables dict.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			account_id: A string containing the ID of the user broker account.
			script_id: A string containing the ID of the user script.
		'''
		
		strategy = self.getStrategy(strategy_id)

		current_bank = 0
		for _broker_id in strategy.get("brokers", {}):
			for _account_id in strategy["brokers"][_broker_id]["accounts"]:
				if account_id != tl.broker.PAPERTRADER_NAME and (_broker_id != broker_id or _account_id != account_id):
					account_code = _broker_id + '.' + _account_id
					input_variables = self.ctrl.getDb().getAccountInputVariables(self.userId, strategy_id, account_code, script_id)

					if len(input_variables) and "Allocated Bank" in input_variables["Preset 1"]:
						if input_variables["Preset 1"]["Allocated Bank"].get("value") is not None:
							current_bank += input_variables["Preset 1"]["Allocated Bank"]["value"]
						else:
							current_bank += input_variables["Preset 1"]["Allocated Bank"]["default"]

		return current_bank


	def runStrategyScript(self, strategy_id, broker_id, accounts, input_variables):
		'''Runs a user script (deprecated).

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			accounts: A list containing user broker account_ids.
			input_variables: A dict containing the input variables configuration running on the script.
		Returns:
			A string representing the strategy package name.
		'''

		strategy = self.getStrategyInfo(strategy_id)

		Thread(target=strategy.run, args=(accounts, input_variables)).start()
		return strategy.package


	def _runStrategyScript(self, strategy_id, broker_id, accounts, input_variables):
		'''Runs a user script.
		
		Makes a request to the loader application to run a strategy script on the
		given broker accounts with the given input_variables. The database is updated
		to show the account script running status as True.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			accounts: A list containing user broker account_ids.
			input_variables: A dict containing the input variables configuration running on the script.
		Returns:
			A boolean representing the strategy was started successfully or unsuccessfully.
		'''

		user = self.ctrl.getDb().getUser(self.userId)

		server = 0
		if user.get("server") is not None:
			server = user.get("server")

		if server == self.ctrl.app.config["SERVER"]:
			strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)

			package = strategy_info['package']
			script_id = package.split('.')[0]
			version = package.split('.')[1]

			if input_variables is None:
				input_variables = {}

			session_key = self.generateSessionToken()

			if self.ctrl.app.config.get("FORCE_SCRIPT"):
				script_id = self.ctrl.app.config["FORCE_SCRIPT"].split('.')[0]
				version = self.ctrl.app.config["FORCE_SCRIPT"].split('.')[1]

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

			# print(f"SET RUNNING... ({strategy_id}) {accounts}")
			for account_id in accounts:
				self._set_running(strategy_id, broker_id, account_id, script_id, deepcopy(input_variables))

			print("START SCRIPT...")
			url = self.ctrl.app.config.get('LOADER_URL')
			endpoint = '/start'
			res = requests.post(
				url + endpoint,
				data=json.dumps(payload)
			)

			if res.status_code == 200:
				# for account_id in accounts:
				# 	self._set_running(strategy_id, broker_id, account_id, script_id, input_variables)
				return True
			else:
				# for account_id in accounts:
				# 	self._set_running(strategy_id, broker_id, account_id, None, None)
				return False
				
		else:
			return False


	def stopStrategyScript(self, broker_id, accounts):
		'''Stops a user script (deprecated).

		Args:
			broker_id: A string containing the ID of the user broker.
			input_variables: A dict containing the input variables configuration running on the script.
		Returns:
			A string representing the strategy package name.
		'''

		strategy = self.strategies.get(broker_id)
		if strategy is not None:
			strategy.stop(accounts)
			return strategy.package
		else:
			return None


	def _stopStrategyScript(self, strategy_id, broker_id, accounts):
		'''Stops a user script.

		Makes a request to the loader application to stop a strategy script on the
		given broker accounts. The database is updated to show the account script running 
		status as False.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			accounts: A list containing user broker account_ids.
		Returns:
			A boolean representing the strategy was stopped successfully or unsuccessfully.
		'''

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
		'''Updates strategy script package name.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			new_packge: A string containing the new script package name
		'''

		# Retrieve strategy
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		strategy_info['package'] = new_package

		# Perform update
		self.ctrl.getDb().updateStrategy(self.userId, strategy_id, strategy_info)


	def _set_strategy(self, strategy_id, broker_id, api, package):
		'''Maps broker_id with Strategy object (deprecated).

		Maps broker_id with Strategy object. A new Strategy object is
		created if it does not exists.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			api: Socket IO client.
			package: Strategy script package name.
		Returns:
			The Strategy object associated with the broker_id		
		'''

		strategy = self.strategies.get(broker_id)
		if strategy is None:
			strategy = Strategy(strategy_id, broker_id, api, package)
			self.strategies[broker_id] = strategy
		return self.strategies[broker_id]


	def getStrategyBroker(self, strategy_id):
		'''Gets Broker object associated with strategy_id.
		
		Gets Broker object associated with strategy_id. If Broker object
		is not found, the strategy is started.

		Returns:
			Broker object associated with strategy_id.
		'''

		# while self.strategies.get(strategy_id) == 'working':
		# 	pass

		broker = self.brokers.get(strategy_id)
		if broker is None:
			self.startStrategy(strategy_id)
			broker = self.brokers.get(strategy_id)
		return broker

	# Broker Functions
	def getAllBrokers(self):
		'''Retrieves and decodes JWT token data of brokers saved to user's database entry.

		Retrieves the user's brokers saved in their database entry and decodes the broker JWT
		tokens to get the broker information.

		Returns:
			A list of dictionarys containing user's broker's information.
		'''

		result = self.ctrl.getDb().getUser(self.userId).get('brokers')
		if result is None:
			return None

		for k, v in result.items():
			info = jwt.decode(
				v, self.ctrl.app.config['SECRET_KEY'], 
				algorithms=['HS256']
			)
			if 'accounts' in info:
				result[k] = info
		return result


	def getBroker(self, name):
		'''Retrieves broker information from user database entry by broker_id (name).
		
		Args:
			name: A string containing the ID of the user broker.
		Returns:
			A dictionary of user's broker information.
		'''

		result = self.ctrl.getDb().getBroker(self.userId, name)
		return result


	def createBroker(self, broker_id, name, broker_name, **props):
		'''Creates a new user broker.

		Creates a new broker using user provided broker authentication information.
		After a validation check, if successful, a new broker entry as added to the
		user's database info. A _queue is used to for synchronous use of this function
		and avoid duplicates. _replace_broker is used to replace an existing broker account
		in the event that a user attempts to reconnect a broker.

		Args:
			broker_id: A string containing the ID of the user broker.
			name: A string containing the display name of the broker.
			broker_name: A string containing the name of the broker being connected to.
			props: A dict mapping relevant keys and values to be stored in the broker's
				   information dict.
		Returns:
			A string containing the ID of the user broker. None if returned if connection
			is unsuccessful.
		'''

		queue_id = self.generateReference()
		self._queue.append(queue_id)
		while self._queue.index(queue_id) > 0: pass

		print(f'[account.createBroker] {broker_id}, {broker_name}, {props}, {self._replace_broker}')

		result = None
		try:
			# if not broker_id in self.brokers:
			if broker_name in self._replace_broker:
				broker_id = self._replace_broker[broker_name]
				print(f'[account.createBroker] found replacement {broker_id}')
				del self._replace_broker[broker_name]

			if broker_id in self.brokers:
				print(f'[account.createBroker] deleting old broker: {broker_id}')
				try:
					self.brokers[broker_id].deleteChild()
				except Exception:
					pass
				del self.brokers[broker_id]

			result = self.ctrl.getDb().createBroker(self.userId, broker_id, name, broker_name, props)
		except Exception:
			pass
		finally:
			del self._queue[0]

		return result


	def updateBroker(self, broker_id, props):
		'''Updates broker information in database.

		Args:
			broker_id: A string containing the ID of the user broker.
			props: A dict containing key/values to be changed or added.
		Returns:
			props dict containing key/values that were changed or added.
		'''

		return self.ctrl.getDb().updateBroker(self.userId, broker_id, props);


	def updateBrokerName(self, old_name, new_name):
		'''Changes the key mapped to the broker JWT token in the database.
		
		Args:
			old_name: Old broker ID name.
			new_name: New broker ID name to be changed to.
		Returns:
			new_name, broker ID name that was changed to.
		'''

		result = self.ctrl.getDb().updateBrokerName(self.userId, old_name, new_name)
		return result


	def deleteBroker(self, name):
		'''Deletes a broker from database and in memory.

		Args:
			name: A string containing the ID of the user broker.
		Returns:
			name, a string containing the ID of the deleted user broker.
		'''

		if name in self.brokers:
			try:
				self.brokers[name].deleteChild()
			except Exception:
				pass
			finally:
				del self.brokers[name]

		result = self.ctrl.getDb().deleteBroker(self.userId, name)
		return result


	def getAccountInfo(self, strategy_id, account_code):
		'''Retrieves a dict of relevant account info.

		Retrieves the account GUI config, transactions and script input_variables and
		maps them in a dict.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a broker ID and broker account ID
						  concatenated with a '.'.
		Returns:
			A dict of the account GUI config, transactions and script input_variables.
		'''

		script_id = self.getScriptId(strategy_id)

		result = {}
		result.update(self.getAccountGui(strategy_id, account_code))
		result.update(self.getAccountTransactions(strategy_id, account_code))
		result['input_variables'] = self.getAccountInputVariables(strategy_id, account_code, script_id)
		return result


	# API Key Functions
	def getKeys(self, strategy_id):
		'''Retrieves keys from user database.

		Args:
			strategy_id: A string containing the ID of the user strategy.
		Returns:
			A list of strings which are keys with API access privileges associated
			to strategy_id.
		'''

		keys = self.keys.get(strategy_id)
		if keys is None:
			keys = self.ctrl.getDb().getKeys(self.userId, strategy_id)
		return keys

	def addKey(self, strategy_id, key):
		'''Adds a key to user database key list associated with strategy_id.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			key: A string containing new key to be added.
		'''

		keys = self.getKeys(strategy_id)
		if keys is not None:
			keys.append(key)

	def checkKey(self, strategy_id, key):
		'''Checks if an access key is valid for strategy.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			key: A string containing key to be validated.
		Returns:
			Boolean representing if the key meets access privileges.
		'''

		keys = self.getKeys(strategy_id)
		if keys is not None:
			return key in keys
		return False

	def deleteKey(self, strategy_id, key):
		'''Deletes a key from user database key list associated with strategy_id.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			key: A string containing key to be deleted.
		Returns:
			Boolean representing deletion was successful or unsuccessful.
		'''

		keys = self.getKeys(strategy_id)
		if keys is not None:
			if key in keys:
				del keys[keys.index(key)]
				return True
		return False

	# Login Token Functions
	def generateToken(self):
		'''Generates JWT session token for login access with expiry time.

		Returns:
			A string containing JWT session token.		
		'''

		EXP_TIME = 60 * 60 * 12
		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time()), 'exp': time.time() + EXP_TIME
		}
		return jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')

	def generatePermanentToken(self):
		'''Generates JWT session token for login access with no expiry time.

		Returns:
			A string containing JWT session token.		
		'''

		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time())
		}
		return jwt.encode(payload, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')

	def generateSessionToken(self):
		'''Generates JWT session token for login access with no expiry time.

		Returns:
			A string containing JWT session token.		
		'''

		payload = { 
			'sub': self.userId, 'iat': math.floor(time.time())
		}
		return jwt.encode(payload, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')


	def checkToken(self, token):
		'''Checks if token is valid for this user.

		Args:
			token: A string containing JWT session token.
		Returns:
			A boolean representing access granted or denied.
		'''

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
		'''Updates PaperTrader positions and orders to cloud storage.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			positions: A list containing Position dictionaries.
			orders: A list containing Order dictionaries.
		'''

		trades = {
			'positions': positions,
			'orders': orders
		}
		Thread(
			target=self.ctrl.getDb().updateStrategyTrades, 
			args=(self.userId, strategy_id, trades)
		).start()
		

	def getStrategyGui(self, strategy_id):
		'''Retrieves main strategy GUI from cloud storage.

		Args:
			strategy_id: A string containing the ID of the user strategy.
		Returns:
			A dict containing strategy GUI configuration.		
		'''

		return self.ctrl.getDb().getStrategyGui(self.userId, strategy_id)


	def updateStrategyGui(self, strategy_id, new_gui):
		'''Updates main strategy GUI in cloud storage.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			new_gui: A dict containing new strategy GUI configuration.
		'''

		self.ctrl.getDb().updateStrategyGui(self.userId, strategy_id, new_gui)


	def getAccountGui(self, strategy_id, account_code):
		'''Retrieves account GUI configuration in cloud storage.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a user broker ID and broker's account ID
						  concatenated by a '.'.
		Returns:
			A dict containing account GUI configuration.
		'''

		return self.ctrl.getDb().getAccountGui(self.userId, strategy_id, account_code)


	def getAccountTransactions(self, strategy_id, account_code):
		'''Retrieves account position and orders from cloud storage.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a user broker ID and broker's account ID
						  concatenated by a '.'.
		Returns:
			A dict containing account positions and orders.
		'''

		return self.ctrl.getDb().getAccountTransactions(self.userId, strategy_id, account_code)


	def getAccountReport(self, strategy_id, account_code, name):
		'''Retrieves an account report from cloud storage.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a user broker ID and broker's account ID
						  concatenated by a '.'.
			name: A string containing the name of the report.
		Returns:
			A dict containing account report.
		'''

		return self.ctrl.getDb().getStrategyAccountReport(self.userId, strategy_id, account_code, name)


	def updateAccountGui(self, strategy_id, account_code, new_gui):
		'''Updates account GUI configuration in cloud storage.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a user broker ID and broker's account ID
						  concatenated by a '.'.
			new_gui: A dict containing new strategy GUI configuration.
		'''

		self.ctrl.getDb().updateAccountGui(self.userId, strategy_id, account_code, new_gui)


	def appendAccountGui(self, strategy_id, account_code, new_gui):
		'''Updates account GUI configuration in cloud storage.

		Information in new_gui is added to existing GUI config rather than replacing it.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a user broker ID and broker's account ID
						  concatenated by a '.'.
			new_gui: A dict containing new strategy GUI configuration.
		'''
		
		self.ctrl.getDb().appendAccountGui(self.userId, strategy_id, account_code, new_gui)		


	def updateStrategyAccount(self, strategy_id, account_code):
		'''Updates currently active account in strategy GUI.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a user broker ID and broker's account ID
						  concatenated by a '.'.
		'''

		gui = self.getStrategyGui(strategy_id)
		gui['account'] = account_code
		self.updateStrategyGui(strategy_id, gui)


	def updateStrategyGuiItems(self, strategy_id, items):
		'''Updates strategy GUI items.

		Only items that are in the items dict are updated.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			items: A dict containing items that need to be updated.
		Returns:
			A list of the window IDs that were updated.
		'''

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
		if items.get('settings') != None:
			gui['settings'] = items.get('settings')

		self.updateStrategyGui(strategy_id, gui)
		return result


	def createStrategyGuiItem(self, strategy_id, item):
		'''Creates a new GUI window from item var.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			item: A dict containing the new GUI item.
		Returns:
			A string containing the new item ID.
		'''

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
		'''Deletes GUI window items.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			items: A dict containing the GUI items to be deleted.
		Returns:
			A list of strings containing the deleted item IDs.
		'''

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
		'''Retrieves the active strategy script ID from database.

		Args:
			strategy_id: A string containing the ID of the user strategy.
		Returns:
			A string containing the active strategy script ID.
		'''

		# Get Script Id
		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		return strategy_info['package'].split('.')[0]


	def getStrategyInputVariables(self, strategy_id, script_id, update=True):
		'''Retrieves strategy input variables.

		Default and saved variables are retrieved and combined where saved variables
		override default variables. This also accounts for new updated variables.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			script_id: A string containing the script ID.
			update: A boolean allowing (if True) or disallowing an update to cloud storage.
		Returns:
			A dict of the strategy script input variables.
		'''
		
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
		'''Updates strategy input variables in cloud storage.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			script_id: A string containing the script ID.
			new_vars: A dict containing the new input variables.
		Returns:
			A boolean denoting whether the update was or was not successful.
		'''	
		
		return self.ctrl.getDb().updateStrategyInputVariables(self.userId, strategy_id, script_id, new_vars)


	def getAccountInputVariables(self, strategy_id, account_code, script_id, update=True):
		'''Retrieves account input variables.

		Default and saved variables are retrieved and combined where saved variables
		override default variables. This also accounts for new updated variables.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a broker ID and broker account ID
						  concatenated with a '.'.
			script_id: A string containing the script ID.
			update: A boolean allowing (if True) or disallowing an update to cloud storage.
		Returns:
			A dict of the account script input variables.
		'''

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
		'''Updates account input variables in cloud storage.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			script_id: A string containing the script ID.
			account_code: A string containing a broker ID and broker account ID
						  concatenated with a '.'.
			new_vars: A dict containing the new input variables.
		Returns:
			A boolean denoting whether the update was or was not successful.
		'''	

		return self.ctrl.getDb().updateAccountInputVariables(self.userId, strategy_id, script_id, account_code, new_vars)


	def getBacktestInfo(self, strategy_id, backtest_id):
		'''Retrieves backtest GUI config JSON.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
		Returns:
			A dict of backtest GUI configuration.
		'''

		strategy_info = self.ctrl.getDb().getStrategy(self.userId, strategy_id)
		# broker_info = self.ctrl.getDb().getBroker(self.userId, strategy_info['brokers'][strategy_id].get('broker'))
		gui = self.getBacktestGui(strategy_id, backtest_id)
		return gui


	def getBacktestGui(self, strategy_id, backtest_id):
		'''Retrieves backtest GUI config JSON.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
		Returns:
			A dict of backtest GUI configuration.
		'''

		return self.ctrl.getDb().getStrategyBacktestGui(self.userId, strategy_id, backtest_id)


	def updateBacktestGui(self, strategy_id, backtest_id, new_gui):
		'''Updates backtest GUI config JSON.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
			new_gui: A dict containing the updated GUI JSON data.
		'''

		self.ctrl.getDb().updateStrategyBacktestGui(self.userId, strategy_id, backtest_id, new_gui)


	def updateBacktestGuiItems(self, strategy_id, backtest_id, items):
		'''Update specified backtest GUI window items.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
			items: A dict containing a list of the window items to be updated.
		Returns:
			A dict containing updated GUI JSON data.
		'''

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
		'''Retrieves backtest report data by name.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
			name: A string containing the name of the report.
		Returns:
			A dict containing the report's JSON data.
		'''

		return self.ctrl.getDb().getStrategyBacktestReport(self.userId, strategy_id, backtest_id, name)


	def updateBacktestReport(self, strategy_id, backtest_id, name, obj):
		'''Updates backtest report data by name.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
			name: A string containing the name of the report.
			obj: A dict containing the new report JSON data.
		Returns:
			A boolean denoting a successfuly or unsuccessful update of the report.
		'''

		return self.ctrl.getDb().updateStrategyBacktestReport(self.userId, strategy_id, backtest_id, name, obj)



	# Private
	def _user_validation(self):
		'''Checks the user ID exists in the database.

		Raises an AccountException if the user does not exist.
		'''

		user = self.ctrl.getDb().getUser(self.userId)
		if not user:
			raise AccountException('User does not exist.')


	def _set_brokers(self, strategy_id, strategy_info):
		'''Queues all user brokers to be initialized.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			strategy_info: A dict containing the info of a given strategy in
						   the user database.
		Returns:
			A dict containing the user's broker information.
		'''

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
		delete = []
		for broker_id in brokers:
			self._set_broker_queue.handle(
				broker_id,
				self._perform_set_broker, 
				strategy_id, broker_id, 
				brokers[broker_id]
			)

			broker = self.brokers.get(broker_id)

			if brokers[broker_id].get('broker') == tl.broker.DUKASCOPY_NAME and not brokers[broker_id].get('complete'):
				delete.append(broker_id)
			elif broker is not None:
				if tl.broker.PAPERTRADER_NAME in broker.getAccounts():
					self.updateTrades(
						strategy_id,
						broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
						broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
					)
			else:
				delete.append(broker_id)

		print(f'[_set_brokers] {delete}')
		for i in delete:
			del brokers[i]

		return brokers


	def _perform_set_broker(self, strategy_id, broker_id, broker_info):
		'''Initializes a Broker object if it has not already been initialized.

		This function is called synchronously per user to avoid double ups of
		brokers.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			broker_info: A dict containing the info of a given broker in
						 the user database.
		'''

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
					'name': tl.broker.FXCM_NAME,
					'display_name': 'Paper Trader',
					'is_dummy': False,
					'is_auth': True
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


	def setTestBroker(self, broker_id, broker_name):
		'''Initializes a test Broker object.

		Args:
			broker_id: A string containing the ID of the user broker.
			broker_name: A string containing the name of the broker provider.
		'''

		brokers = self.getAllBrokers()
		if broker_id not in self.brokers:
			if broker_name == tl.broker.PAPERTRADER_NAME:
				pass
			elif broker_name == tl.broker.OANDA_NAME:
				pass
			elif broker_name == tl.broker.SPOTWARE_NAME:
				self.brokers[broker_id] = tl.broker.TestSpotware(self.ctrl, self, brokers[broker_name])
			elif broker_name == tl.broker.FXOPEN_NAME:
				pass


	def _init_broker(self, broker_name, broker_args):
		'''Initializes a Broker object with provided args.

		Args:
			broker_name: A string containing the name of the broker provider.
			broker_args: A dict containing keyword arguments of Broker object.
		Returns:
			The initialized Broker object.		
		'''

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
			elif broker_name == tl.broker.IB_NAME:
				self.brokers[broker_args['broker_id']] = tl.broker.IB(**broker_args)
			elif broker_name == tl.broker.DUKASCOPY_NAME:
				print(f'[_init_broker] {broker_args}')
				if broker_args.get('complete'):
					self.brokers[broker_args['broker_id']] = tl.broker.Dukascopy(**broker_args)
				else:
					self.ctrl.getDb().deleteBroker(self.userId, broker_args['broker_id'])
					return None
			elif broker_name == tl.broker.FXOPEN_NAME:
				self.brokers[broker_args['broker_id']] = tl.broker.FXOpen(**broker_args)
			elif broker_name == "loadtest":
				print(f"[loadtest] -> {broker_args}")
				self.brokers[broker_args['broker_id']] = tl.broker.LoadTest(**broker_args)

		return self.brokers[broker_args['broker_id']]

	# Drawing Functions
	def createDrawings(self, strategy_id, layer, drawings):
		'''Saves/sends drawing items for given strategy.

		Drawing items provided in the drawings list have an ID generated and
		are saved to the strategy GUI config and sent to any concurrent user
		browser's via socket message.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			layer: A string containing the name of the drawing layer.
			drawings: A list of dicts containing the drawing configurations.
		Returns:
			A list of the generated drawing IDs.
		'''

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
		'''Deletes a strategy drawing by ID and sends deletion message.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			layer: A string containing the name of the drawing layer.
			drawing_ids: A list of strings containing the IDs of the drawings
						 to be deleted.
		Returns:
			A list of the deleted drawing's IDs.
		'''

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
		'''Deletes a strategy drawing layer and sends deletion message.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			layer: A string containing the name of the drawing layer.
		Returns:
			A string containing the deleted layer name.
		'''

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
		'''Deletes all strategy drawings and layers and sends deletion message.

		Args:
			strategy_id: A string containing the ID of the user strategy.
		Returns:
			A list of strings containing the deleted layer names.
		'''

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
		'''Retrieves backtest GUI config JSON.
		
		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
		Returns:
			A dict of backtest GUI configuration.
		'''

		return self.ctrl.getDb().getStrategyBacktestGui(self.userId, strategy_id, backtest_id)

	def getBacktestChartInfo(self, strategy_id, backtest_id):
		'''Retrieves backtest Chart Info JSON data.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
		Returns:
			A dict of backtest Chart Info JSON data.
		'''

		return self.ctrl.getDb().getStrategyBacktestInfo(self.userId, strategy_id, backtest_id)

	def getBacktestTransactions(self, strategy_id, backtest_id):
		'''Retrieves backtest Transaction JSON data.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest_id: A string containing the ID of the backtest.
		Returns:
			A dict of backtest Transaction JSON data.
		'''

		return self.ctrl.getDb().getStrategyBacktestTransactions(self.userId, strategy_id, backtest_id)


	def uploadBacktest(self, strategy_id, backtest):
		'''Saves backtest data to user storage accessible for browser viewing.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			backtest: A dict containing the backtest data and configuration info.
		Returns:
			A string containing the generated Backtest ID.
		'''

		return self.ctrl.getDb().createStrategyBacktest(self.userId, strategy_id, backtest)


	def uploadLiveBacktest(self, strategy_id, broker_id, account_id, backtest):
		'''Saves backtest data to user storage for live charts.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker_id: A string containing the ID of the user broker.
			account_id: A string containing the ID of the user broker account.
			backtest: A dict containing the backtest data and configuration info.
		Returns:
			None
		'''

		return self.ctrl.getDb().createAccountBacktest(self.userId, strategy_id, broker_id, account_id, backtest)


	def performBacktest(self, strategy_id, broker, start, end, auth_key, input_variables, spread, process_mode):
		'''Sends request to Loader application to start a backtest with given params.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			broker: A string containing the name of the broker provider.
			start: An integer containing the start UTC timestamp in seconds.
			end: An integer containing the end UTC timestamp in seconds.
			auth_key: A string containing a user JWT session token.
			input_variables: A dict containing the script user variables configuration.
			spread: A float containing the artificial price spread to be used.
			process_mode: A string containing the data processing mode to be used.
		'''

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
			'spread': spread,
			'process_mode': process_mode
		}

		url = self.ctrl.app.config.get('LOADER_URL')
		endpoint = '/backtest'
		res = requests.post(
			url + endpoint,
			data=json.dumps(payload)
		)


		return


	def replaceStrategyInputVariables(self, strategy_id, input_variables):
		'''Replaces strategy input variables for each preset provided in input_variables.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			input_variables: A dict containing the script user variables configuration.
		Returns:
			A dict containing the updated input_variables.
		'''

		# Retrieve
		script_id = self.getScriptId(strategy_id)
		global_vars = self.getStrategyInputVariables(strategy_id, script_id, update=False)

		for preset in input_variables:
			global_vars[preset] = input_variables[preset]

		# Update
		self.updateStrategyInputVariables(strategy_id, script_id, global_vars)

		return input_variables


	def replaceAccountInputVariables(self, strategy_id, account_code, input_variables):
		'''Replaces account input variables for each preset provided in input_variables.

		Args:
			strategy_id: A string containing the ID of the user strategy.
			account_code: A string containing a broker ID and broker account ID
						  concatenated with a '.'.
			input_variables: A dict containing the script user variables configuration.
		Returns:
			A dict containing the updated input_variables.
		'''

		# Retrieve
		script_id = self.getScriptId(strategy_id)
		local_vars = self.getAccountInputVariables(strategy_id, account_code, script_id, update=False)

		for preset in input_variables:
			local_vars[preset] = input_variables[preset]

		# Update
		self.updateAccountInputVariables(strategy_id, account_code, script_id, local_vars)

		return input_variables


	def setBrokerReplacement(self, broker_name, broker_id):
		'''Adds pending broker_id to be replaced on user activated broker reconnect.
		
		Args:
			broker_name: A string containing the name of the broker provider.
			broker_id: A string containing the ID of the user broker.
		Returns:
			Boolean True.
		'''

		self._replace_broker[broker_name] = broker_id
		return True


	def getStripeCustomerDetails(self):
		'''Retrieves Stripe customer details from Stripe API.
		
		Returns:
			A dict containing Stripe customer details.
		'''

		starting_after = None
		while True:
			res = stripe.Customer.list(
				api_key=self.ctrl.app.config['STRIPE_API_KEY'],
				starting_after=starting_after
			)
			for i in res["data"]:
				if "user_id" in i["metadata"] and i["metadata"]["user_id"] == self.userId:
					return i

			if not res["has_more"]:
				break
			else:
				starting_after = res["data"][-1]
		
		return None

	
	def getStripePaymentDetails(self):
		'''Retrieves Stripe default payment details from Stripe API.

		Returns:
			A dict containing Stripe payment card information.
		'''

		customer = self.getStripeCustomerDetails()
		if customer is not None:
			payment_method_id = customer["invoice_settings"]["default_payment_method"]
			if "invoice_settings" in customer and "default_payment_method" in customer["invoice_settings"]:
				payment_method = stripe.PaymentMethod.retrieve(
					payment_method_id,
					api_key=self.ctrl.app.config['STRIPE_API_KEY']
				)
				if payment_method is not None:
					return {
						"last4": payment_method["card"]["last4"]
					}

		return None

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

	

