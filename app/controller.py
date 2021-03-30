import os
import json
import socketio
import requests
import shortuuid
import time
import traceback
from copy import copy
from urllib.request import urlopen
from flask import abort
from threading import Thread
from xecd_rates_client import XecdClient


STREAM_URL = 'http://nginx:3001'


def initController(app):
	global ctrl
	ctrl = Controller(app)


class DictQueue(dict):

	def generateReference(self):
		return shortuuid.uuid()

	def handle(self, key, func, *args, **kwargs):
		_id = self.generateReference()

		if key not in self:
			self[key] = []
		self[key].append(_id)
		while self[key].index(_id) != 0:
			time.sleep(0.1)

		result = func(*args, **kwargs)
		del self[key][0]
		return result


class Controller(object):

	def __init__(self, app):
		self.app = app
		self._msg_queue = {}
		self._listeners = {}

		self.sio = self.setupSio()
		self.sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		self.accounts = Accounts(self)
		self.db = Database(self, app.config['ENV'])
		self.charts = Charts(self)
		self.brokers = Brokers(self)

		self.spots = Spots(self, [
			'USD', 'EUR', 'AUD', 'CAD', 'CHF', 'GBP',
			'JPY', 'MXN', 'NOK', 'NZD', 'SEK',
			'RUB', 'CNY', 'TRY', 'ZAR', 'PLN',
			'HUF', 'CZK', 'SGD', 'HKD', 'DKK'
		])

		# Thread(target=self.restartScripts).start()
		

	def closeApp(self):
		# Discontinue any threads
		print('Closing app... (This may take a few seconds)')

		self.sio.disconnect()
		for broker in self.brokers.values():
			broker.stop()

		for account in self.accounts.values():
			for broker in account.brokers.values():
				broker.stop()

		return

	def setupSio(self):
		while True:
			try:
				sio = socketio.Client()
				sio.connect(self.app.config['STREAM_URL'], namespaces=['/admin'])
				break
			except socketio.exceptions.ConnectionError as e:
				print(e)
				time.sleep(1)

		return sio

	def emit(self, event, data=None, namespace=None, callback=None):
		try:
			self.sio.emit(event, data=data, namespace=namespace, callback=callback)
		except Exception:
			print(traceback.format_exc())


	def onCommand(self, data):
		if 'msg_id' in data:
			if data['msg_id'] in self._listeners:
				result = data['result']
				self._listeners[data['msg_id']](*result.get('args'), **result.get('kwargs'))
			else:
				self._msg_queue[data['msg_id']] = data


	def _wait_broker_response(self, msg_id, timeout=60):
		start = time.time()

		while time.time() - start < timeout:
			if msg_id in copy(list(self._msg_queue.keys())):
				res = self._msg_queue[msg_id]
				del self._msg_queue[msg_id]
				print('WAIT RECV', flush=True)
				return res.get('result')
			time.sleep(0.1)

		return {
			'error': 'No response.'
		}


	def brokerRequest(self, broker, broker_id, func, *args, **kwargs):
		msg_id = shortuuid.uuid()

		data = {
			'msg_id': msg_id,
			'broker': broker,
			'broker_id': broker_id,
			'cmd': func,
			'args': list(args),
			'kwargs': kwargs
		}
		try:
			self.sio.emit('broker_cmd', data=data, namespace='/admin')
			return self._wait_broker_response(msg_id)
		except Exception:
			print(traceback.format_exc())
			return {
				'error': 'No response.'
			}


	def addBrokerListener(self, msg_id, listener):
		self._listeners[msg_id] = listener


	def restartScripts(self):
		all_users = self.getDb().getAllUsers()

		for user in all_users:
			user_id = user.get('user_id')
			if 'strategies' in user:
				for strategy_id in user['strategies']:
					if 'running' in user['strategies'][strategy_id]:
						for broker_id in user['strategies'][strategy_id]['running']:
							for account_id in user['strategies'][strategy_id]['running'][broker_id]:
								if (
									isinstance(user['strategies'][strategy_id]['running'][broker_id][account_id], dict) and 
									user['strategies'][strategy_id]['running'][broker_id][account_id].get('script_id')
								):
									# Get package name by last run
									script_id = user['strategies'][strategy_id]['running'][broker_id][account_id]['script_id']
									input_variables = user['strategies'][strategy_id]['running'][broker_id][account_id]['input_variables']

									# Restart strategy
									account = self.accounts.getAccount(user_id)
									account.startStrategy(strategy_id)

									# Get Auth Key
									# key = account.generateSessionToken()

									# Run Script
									print(f'STARTING {strategy_id}, {broker_id}, {account_id}')

									account._runStrategyScript(strategy_id, broker_id, [account_id], input_variables)




	def getAccounts(self):
		return self.accounts

	def getBrokers(self):
		return self.brokers

	def getCharts(self):
		return self.charts

	def getDb(self):
		return self.db


class Brokers(dict):

	def __init__(self, ctrl):
		self.ctrl = ctrl
		options = self._get_options()

		for k, v in options.items():
			self.ctrl.charts._init_broker_charts(k)
			self[k] = self._init_broker(k, v)

	def _get_options(self):
		path = self.ctrl.app.config['BROKERS']
		if os.path.exists(path):
			with open(path, 'r') as f:
				return json.load(f)
		else:
			raise Exception('Broker options file does not exist.')

	def _get_spotware_items(self):
		assets_path = self.ctrl.app.config['SPOTWARE_ASSETS']
		symbols_path = self.ctrl.app.config['SPOTWARE_SYMBOLS']
		if os.path.exists(assets_path) and os.path.exists(symbols_path):
			assets = None
			symbols = None
			with open(assets_path, 'r') as f:
				assets = json.load(f)
			with open(symbols_path, 'r') as f:
				symbols = json.load(f)

			if all((assets, symbols)):
				return assets, symbols
		
		raise Exception('Spotware files do not exist.')

	def _init_broker(self, name, options):
		key = options.get('key')
		is_demo = options.get('is_demo')
		if name == tl.broker.OANDA_NAME:
			accounts = options.get('accounts')
			return tl.broker.Oanda(self.ctrl, key, is_demo, accounts=accounts, is_parent=True)
		elif name == tl.broker.FXCM_NAME:
			username = options.get('username')
			password = options.get('password')
			return tl.broker.FXCM(self.ctrl, username, password, is_demo, is_parent=True)
		elif name == tl.broker.IG_NAME:
			username = options.get('username')
			password = options.get('password')
			return tl.broker.IG(self.ctrl, username, password, key, is_demo)
		elif name == tl.broker.SPOTWARE_NAME:
			accounts = options.get('accounts')
			assets, symbols = self._get_spotware_items()
			return tl.broker.Spotware(self.ctrl, is_demo, accounts=accounts, is_parent=True, assets=assets, symbols=symbols)

	def getBroker(self, name):
		return self.get(name)

	def setBroker(self, name, obj):
		options = self._get_options()
		options[name] = obj

		path = self.ctrl.app.config['BROKERS']
		with open(path, 'w') as f:
			f.write(json.dumps(f, indent=2))


class Charts(dict):

	def __init__(self, ctrl):
		self.ctrl = ctrl
		self.queue = DictQueue()
		# self._generate_broker_keys()


	# def _generate_broker_keys(self):
	# 	for k in self.ctrl.brokers:
	# 		self[k] = {}

	def _init_broker_charts(self, broker):
		self[broker] = {}


	def createChart(self, broker, product, await_completion):
		if 'brokers' in dir(self.ctrl):
			broker = self.ctrl.brokers[broker.name]
		print(f'CREATE CTRL: {broker} {broker.name}')

		if product not in self[broker.name]:
			chart = tl.Chart(self.ctrl, broker, product, await_completion=await_completion)
			self[broker.name][product] = chart
			return chart
		else:
			return self[broker.name][product]


	def getChart(self, broker, product, await_completion):
		print(self)
		if broker.name in self:
			if product in self[broker.name]:
				return self[broker.name][product]
			else:
				return self.queue.handle(
					f'{broker.name}:{product}',
					self.createChart,
					broker, product,
					await_completion
				)

		raise abort(404, 'Broker does not exist.')


	def deleteChart(self, broker_name, product):
		try:
			del broker[broker_name][product]
		except:
			pass


class Accounts(dict):

	def __init__(self, ctrl):
		self.ctrl = ctrl
		self.queue = DictQueue()

	def initAccount(self, user_id):
		if user_id not in self:
			try:
				acc = Account(self.ctrl, user_id)
				self[user_id] = acc
					
			except AccountException:
				return None
		
		return self.get(user_id)


	def addAccount(self, account):
		self[account.user_id] = account

	def getAccount(self, user_id):
		acc = self.get(user_id)
		if acc is None:
			return self.queue.handle(user_id, self.initAccount, user_id)
		else:
			return acc

	def deleteAccount(self, user_id):
		if user_id in self:
			del self[user_id]


class Spots(dict):

	def __init__(self, ctrl, spots):
		self.ctrl = ctrl
		self._init_spots(spots)


	def _init_spots(self, spots):
		for i in spots:
			self[i] = tl.Spot(self.ctrl, i)
			self[i].getRate()


	def _update_spots(self):
		for i in self:
			self[i].getRate()



from app import tradelib as tl
from app.account import Account
from app.db import Database
from app.error import AccountException
