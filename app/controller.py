import os
import json
import socketio
import requests
import shortuuid
import time
import traceback
import zmq
import jwt
from copy import copy
from urllib.request import urlopen
from flask import abort
from threading import Thread
from redis import Redis


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
		self._emit_queue = []
		self._send_queue = []

		self.redis_client = Redis(host='redis', port=6379, password="dev")
		self.redis_client.set("workers_complete", 0)

		# self.sio = self.setupSio(self.app.config['STREAM_URL'])
		# self.sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		# if not self.app.config['IS_MAIN_STREAM']:
		# 	self.main_sio = self.setupSio(self.app.config['MAIN_STREAM_URL'])
		# 	self.main_sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		# self.accounts = Accounts(self)
		# self.db = Database(self, app.config['ENV'])
		# self.charts = Charts(self)
		# self.brokers = Brokers(self)

		self.spots = Spots(self, [
			'USD', 'EUR', 'AUD', 'CAD', 'CHF', 'GBP',
			'JPY', 'MXN', 'NOK', 'NZD', 'SEK',
			'RUB', 'CNY', 'TRY', 'ZAR', 'PLN',
			'HUF', 'CZK', 'SGD', 'HKD', 'DKK'
		])

		# print(f"RESTART SCRIPTS? {self.app.config['RESTART_SCRIPTS_ON_STARTUP']}")
		# if self.app.config['RESTART_SCRIPTS_ON_STARTUP']:
		# 	Thread(target=self.restartScripts).start()
		
	def _setup_zmq_connections(self):
		self.zmq_context = zmq.Context()

		# self.zmq_pull_socket = self.zmq_context.socket(zmq.PULL)
		# self.zmq_pull_socket.connect("tcp://zmq_broker:5555")

		# self.zmq_sub_socket = self.zmq_context.socket(zmq.SUB)
		# self.zmq_sub_socket.connect("tcp://zmq_broker:5556")
		# self.zmq_sub_socket.setsockopt(zmq.SUBSCRIBE, b'')

		# self.zmq_dealer_socket = self.zmq_context.socket(zmq.DEALER)
		# self.zmq_dealer_socket.connect("tcp://zmq_broker:5557")


		# self.zmq_poller = zmq.Poller()
		# self.zmq_poller.register(self.zmq_pull_socket, zmq.POLLIN)
		# self.zmq_poller.register(self.zmq_sub_socket, zmq.POLLIN)
		
		self.zmq_req_socket = self.zmq_context.socket(zmq.REQ)
		self.zmq_req_socket.connect("tcp://zmq_broker:5563")
		self.zmq_req_socket.send_json({"type": "connection_id"})

		message = self.zmq_req_socket.recv_json()
		self.connection_id = message["connection_id"]
		print(f"CONNECTION ID: {self.connection_id}", flush=True)

		Thread(target=self.zmq_message_loop).start()
		Thread(target=self.zmq_send_loop).start()

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

	def setupSio(self, url):
		while True:
			try:
				sio = socketio.Client()
				sio.connect(url, namespaces=['/admin'])
				break
			except socketio.exceptions.ConnectionError as e:
				print(e)
				time.sleep(1)

		return sio

	def emit(self, event, data=None, namespace=None, callback=None):
		# _id = shortuuid.uuid()
		# print(f"[emit] ({_id}) {event}, {data}, {namespace}, {callback}")
		try:
			# self._emit_queue.append(_id)
			# while self._emit_queue[0] != _id:
			# 	time.sleep(0.001)
			
			# time.sleep(0.001)
			self.sio.emit(event, data=data, namespace=namespace, callback=callback)
		except Exception:
			print(f"[emit] {traceback.format_exc()}")
		# finally:
		# 	del self._emit_queue[0]


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
			time.sleep(0.01)

		return {
			'error': 'No response.'
		}


	# def brokerRequest(self, broker, broker_id, func, *args, **kwargs):
	# 	msg_id = shortuuid.uuid()

	# 	try:
	# 		# self._emit_queue.append(msg_id)
	# 		# while self._emit_queue[0] != msg_id:
	# 		# 	time.sleep(0.001)
			
	# 		time.sleep(0.001)

	# 		data = {
	# 			'msg_id': msg_id,
	# 			'broker': broker,
	# 			'broker_id': broker_id,
	# 			'cmd': func,
	# 			'args': list(args),
	# 			'kwargs': kwargs
	# 		}
	# 		print(f"Emit: {data}")
	# 		self.sio.emit('broker_cmd', data=data, namespace='/admin')
	# 		result = self._wait_broker_response(msg_id)
	# 	except Exception:
	# 		print(f"[brokerRequest] {traceback.format_exc()}")
	# 		result = {
	# 			'error': 'No response.'
	# 		}
	# 	finally:
	# 		# del self._emit_queue[0]
	# 		return result

	
	def brokerRequest(self, broker, broker_id, func, *args, **kwargs):
		msg_id = shortuuid.uuid()

		try:
			data = {
				"type": broker,
				"message": {
					'msg_id': msg_id,
					'broker': broker,
					'broker_id': broker_id,
					'cmd': func,
					'args': list(args),
					'kwargs': kwargs
				}
			}
			print(f"[brokerRequest] Emit: {data}")
			# self.sio.emit('broker_cmd', data=data, namespace='/admin')
			# result = self._wait_broker_response(msg_id)

			# self.zmq_dealer_socket.send_json(data, zmq.NOBLOCK)
			self._send_queue.append(data)
			result = self._wait_broker_response(msg_id)
			
			# result = self.zmq_dealer_socket.recv_json()
			print(f"[brokerRequest] Result: {result}")
			
		except Exception:
			print(f"[brokerRequest] {traceback.format_exc()}")
			result = {
				'error': 'No response.'
			}
		finally:
			return result
			


	def mainBrokerRequest(self, broker, broker_id, func, *args, **kwargs):
		msg_id = shortuuid.uuid()

		try:
			# self._emit_queue.append(msg_id)
			# while self._emit_queue[0] != msg_id:
			# 	time.sleep(0.001)
			
			data = {
				'msg_id': msg_id,
				'broker': broker,
				'broker_id': broker_id,
				'cmd': func,
				'args': list(args),
				'kwargs': kwargs
			}
			self.main_sio.emit('broker_cmd', data=data, namespace='/admin')
			result = self._wait_broker_response(msg_id)
		except Exception:
			print(f"[mainBrokerRequest] {traceback.format_exc()}")
			result = {
				'error': 'No response.'
			}
		finally:
			# del self._emit_queue[0]
			return result


	def addBrokerListener(self, msg_id, listener):
		self._listeners[msg_id] = listener


	def restartScripts(self):
		print(f"[restartScripts] WORKERS: {int(self.redis_client.get('workers_complete').decode())}", flush=True)
		while int(self.redis_client.get("workers_complete").decode()) != 5:
			time.sleep(1)
		time.sleep(1)

		print("RESTARTING SCRIPTS...")
		all_users = self.getDb().getAllUsers()

		server_number = self.app.config["SERVER"]
		print(f"SERVER NUMBER: {server_number}")
		for user in all_users:
			user_id = user.get('user_id')
			user_server = user.get("server")
			if user_server is None:
				user_server = 0
			else:
				user_server = int(user_server)
				
			print(f"USER SERVER NUMBER: {user_server}, {server_number == user_server}")

			if server_number == user_server and 'strategies' in user:
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
									print(f"START 1: {strategy_id}, {script_id}")

									# Restart strategy
									account = self.accounts.getAccount(user_id)
									account.startStrategy(strategy_id)
									print(f"START 2")

									# Get Auth Key
									# key = account.generateSessionToken()

									# Run Script
									print(f'STARTING {strategy_id}, {broker_id}, {account_id}')

									account._runStrategyScript(strategy_id, broker_id, [account_id], input_variables)
									# Thread(target=account._runStrategyScript, args=(strategy_id, broker_id, [account_id], input_variables)).start()


	def handleListenerMessage(self, message):
		if "msg_id" in message:
			if message["msg_id"] in self._listeners:
				result = message['result']
				Thread(target=self._listeners[message['msg_id']], args=result.get('args'), kwargs=result.get('kwargs')).start()
			else:
				self._msg_queue[message['msg_id']] = message

	
	def handleRequestMessage(self, message):
		if message.get("ept") == "init_strategy_by_broker_id_ept":
			print(f"[handleRequestMessage] RECEIVED init_strategy_by_broker_id_ept {message}")
			msg_id = message["msg_id"]
			key = message.get("Authorization")
			strategy_id = message["args"][0]
			broker_id = message["args"][1]

			print(f"[handleRequestMessage] {key}, {strategy_id}", flush=True)
			user_id, _ = self.check_auth_key(key, strategy_id)
			print(f"[handleRequestMessage] {user_id}", flush=True)

			if isinstance(user_id, str):
				account = self.accounts.getAccount(user_id)
				print(f"[handleRequestMessage] getStrategyByBrokerId", flush=True)
				strategy = account.getStrategyByBrokerId(strategy_id, broker_id)
				print(f"[handleRequestMessage] {strategy}", flush=True)

				self._send_queue.append({
					"type": "response",
					"message": {
						"msg_id": msg_id,
						"result": strategy
					}
				})
			
			else:
				print(f"[handleRequestMessage] NOPE", flush=True)
				self._send_queue.append({
					"type": "response",
					"message": {
						"msg_id": msg_id,
						"result": user_id
					}
				})


	def zmq_message_loop(self):
		self.zmq_pull_socket = self.zmq_context.socket(zmq.PULL)
		self.zmq_pull_socket.connect("tcp://zmq_broker:5555")

		self.zmq_sub_socket = self.zmq_context.socket(zmq.SUB)
		self.zmq_sub_socket.connect("tcp://zmq_broker:5556")
		self.zmq_sub_socket.setsockopt(zmq.SUBSCRIBE, b'')

		self.zmq_poller = zmq.Poller()
		self.zmq_poller.register(self.zmq_pull_socket, zmq.POLLIN)
		self.zmq_poller.register(self.zmq_sub_socket, zmq.POLLIN)

		while True:
			try:
				socks = dict(self.zmq_poller.poll())

				if self.zmq_pull_socket in socks:
					message = self.zmq_pull_socket.recv_json()
					print(f"[zmq_message_loop] {message}", flush=True)

					if message.get("type") == "request":
						self.handleRequestMessage(message["message"])
					else:
						print(f"[handleListenerMessage] {'msg_id' in message}, {message}")
						self.handleListenerMessage(message)

				if self.zmq_sub_socket in socks:
					message = self.zmq_sub_socket.recv_json()
					if message.get("type") == "price":
						if self.connection_id == 0:
							self.handleListenerMessage(message["message"])
					elif message.get("type") == "start_strategy":
						print("[zmq_message_loop] START STRATEGY", flush=True)
						user_id = message["message"]["user_id"]
						strategy_id = message["message"]["strategy_id"]
						account = ctrl.accounts.getAccount(user_id)
						print(f"[zmq_message_loop] {account}")
						Thread(target=account.startStrategy, args=(strategy_id,)).start()
					else:
						self.handleListenerMessage(message)

			except Exception:
				print(traceback.format_exc())
	
	def zmq_send_loop(self):
		self.zmq_dealer_socket = self.zmq_context.socket(zmq.DEALER)
		self.zmq_dealer_socket.connect("tcp://zmq_broker:5557")
		
		while True:
			try:
				if len(self._send_queue):
					item = self._send_queue[0]
					del self._send_queue[0]

					self.zmq_dealer_socket.send_json(item, zmq.NOBLOCK)

			except Exception:
				print(traceback.format_exc())
			
			time.sleep(0.01)


	def startModules(self):
		
		self.sio = self.setupSio(self.app.config['STREAM_URL'])
		self.sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		if not self.app.config['IS_MAIN_STREAM']:
			self.main_sio = self.setupSio(self.app.config['MAIN_STREAM_URL'])
			self.main_sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		self._setup_zmq_connections()
		self.redis_client.set("strategies_" + str(self.connection_id), json.dumps({}))
		
		self.accounts = Accounts(self)
		self.db = Database(self, self.app.config['ENV'])
		self.charts = Charts(self)
		self.brokers = Brokers(self)


	def performRestartScripts(self):
		print(f"RESTART SCRIPTS? {self.app.config['RESTART_SCRIPTS_ON_STARTUP']}")
		if self.app.config['RESTART_SCRIPTS_ON_STARTUP']:
			Thread(target=self.restartScripts).start()


	def check_auth_key(self, key, strategy_id):
		key = key.split(' ')
		if len(key) == 2:
			if key[0] == 'Bearer':
				# Decode JWT API key
				try:
					payload = jwt.decode(key[1], self.app.config['SECRET_KEY'], algorithms=['HS256'])
				except jwt.exceptions.DecodeError:
					error = {
						'error': 'AuthorizationException',
						'message': 'Invalid authorization key.'
					}
					return error, 403
				except jwt.exceptions.ExpiredSignatureError:
					error = {
						'error': 'AuthorizationException',
						'message': 'Authorization key expired.'
					}
					return error, 403

				return payload.get('sub'), 200

		error = {
			'error': 'ValueError',
			'message': 'Unrecognizable authorization key.'
		}
		return error, 400


	def getAccounts(self):
		return self.accounts

	def getBrokers(self):
		return self.brokers

	def setBrokers(self):
		self.brokers = Brokers(self)

	def getCharts(self):
		return self.charts

	def getDb(self):
		return self.db


class Brokers(dict):

	def __init__(self, ctrl):
		self.ctrl = ctrl
		self.ib_port_sessions = {}

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
			return tl.broker.FXCM(self.ctrl, username, password, is_demo, broker_id="PARENT", is_parent=True)
		elif name == tl.broker.IG_NAME:
			username = options.get('username')
			password = options.get('password')
			return tl.broker.IG(self.ctrl, username, password, key, is_demo)
		elif name == tl.broker.SPOTWARE_NAME:
			accounts = options.get('accounts')
			return tl.broker.Spotware(self.ctrl, is_demo, broker_id="PARENT", accounts=accounts, is_parent=True)
		elif name == tl.broker.IB_NAME:
			PARENT_PORT = 5000
			tl.broker.IB(self.ctrl, port=PARENT_PORT, is_parent=True)
		elif name == tl.broker.DUKASCOPY_NAME:
			username = options.get('username')
			password = options.get('password')
			return tl.broker.Dukascopy(self.ctrl, username=username, password=password, is_demo=is_demo, broker_id='PARENT', is_parent=True)
		elif name == tl.broker.FXOPEN_NAME:
			web_api_id = options.get('web_api_id')
			web_api_secret = options.get('web_api_secret')
			return tl.broker.FXOpen(self.ctrl, key, web_api_id, web_api_secret, is_demo, broker_id='PARENT', is_parent=True)

	def getBroker(self, name):
		return self.get(name)

	def setBroker(self, name, obj):
		options = self._get_options()
		options[name] = obj

		path = self.ctrl.app.config['BROKERS']
		with open(path, 'w') as f:
			f.write(json.dumps(f, indent=2))

	def getUsedPorts(self):
		used_ports = []
		for port in self.ib_port_sessions:
			port = str(port)
			if self.ib_port_sessions[port]['client'].is_auth:
				used_ports.append(port)
			
			elif time.time() < self.ib_port_sessions[port]['expiry']:
				used_ports.append(port)

			else:
				self.ib_port_sessions[port]['client'].setPort(None)


		return used_ports

	def assignPort(self, client, port):
		port = str(port)
		if port in self.ib_port_sessions:
			self.ib_port_sessions[port]['expiry'] = time.time() + (60*10)
			self.ib_port_sessions[port]['client'] = client
		else:
			self.ib_port_sessions[port] = { 'client': client, 'expiry': time.time() + (60*10), 'ips': [] }



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
		self.prices = self._get_prices()

		if self.prices is None:
			self._init_spots_backup(spots)
		else:
			self._init_spots()


	def _get_prices(self):
		uri = "http://data.fixer.io/api/latest"
		res = requests.get(uri + f"?access_key={self.ctrl.app.config['FIXER_IO_ACCESS_KEY']}&base=USD")
		if res.status_code == 200:
			data = res.json()
			if data.get("success"):
				return data.get("rates")
		
		return None


	def _init_spots(self):
		for i in self.prices:
			self[i] = tl.Spot(self.ctrl, i, rate=1/self.prices[i])


	def _init_spots_backup(self, spots):
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
