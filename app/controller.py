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
	'''Central handler containing containers for Accounts, Brokers, Charts and Database objects.

	One Controller object exists per worker and is used throughout the program as a central
	handler of Account, Broker, Chart and Database objects. Controller also handles tcp/socket
	connections between other programs that this API communicates with.
	
	Attributes:
		app: Flask app.
		sio: Socket IO connection to local stream server.
		main_sio: Socket IO connection to main stream server.
		accounts: A dict that maps Account objects to their user ID
		db: Central Database object.
		charts: A dict that maps an instrument and Chart object key pair to
				a broker provider name string.
		brokers: A dict that maps parent Broker objects to a broker provider
				 string name.
		spots: A dict that maps currencies to their daily spot rate.
		zmq_context: ZMQ Context object.
		zmq_req_socket: ZMQ Request socket.
		zmq_pull_socket: ZMQ Pull socket.
		zmq_sub_socket: ZMQ Subscribe socket.
		zmq_poller: ZMQ Poller object.
		connection_id: An int denoting the worker's connection ID.
		redis_client: Redis object.
		_msg_queue: A dict that maps socket JSON messages to their msg ID string.
		_listeners: A dict that maps a function to the msg ID that triggers 
					the function call.
		_send_queue: A list that queues JSON socket messages to be handled.

	'''

	def __init__(self, app):
		'''Initializes Controller object and member variables.

		Args:
			app: Flask app.
		'''

		self.app = app
		self._msg_queue = {}
		self._listeners = {}
		self._emit_queue = []
		self._send_queue = []

		# self.sio = self.setupSio(self.app.config['STREAM_URL'])
		# self.sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		# if not self.app.config['IS_MAIN_STREAM']:
		# 	self.main_sio = self.setupSio(self.app.config['MAIN_STREAM_URL'])
		# 	self.main_sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		# self.accounts = Accounts(self)
		# self.db = Database(self, app.config['ENV'])
		# self.charts = Charts(self)
		# self.brokers = Brokers(self)

		# self.spots = Spots(self, [
		# 	'USD', 'EUR', 'AUD', 'CAD', 'CHF', 'GBP',
		# 	'JPY', 'MXN', 'NOK', 'NZD', 'SEK',
		# 	'RUB', 'CNY', 'TRY', 'ZAR', 'PLN',
		# 	'HUF', 'CZK', 'SGD', 'HKD', 'DKK'
		# ])

		# print(f"RESTART SCRIPTS? {self.app.config['RESTART_SCRIPTS_ON_STARTUP']}")
		# if self.app.config['RESTART_SCRIPTS_ON_STARTUP']:
		# 	Thread(target=self.restartScripts).start()
		
	def _setup_zmq_connections(self):
		'''Initializes ZMQ sockets and starts message/send loop'''

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
		'''Shuts down Socket IO connection and stop all Brokers'''

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
		'''Starts Socket IO connection.

		Connection is retried until successful.

		Args:
			url: A string containing the URL the socket connects to.
		Returns:
			A connected socketio Client object.
		'''

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
		'''Initiates a Socket IO emit.

		Args:
			event: A string containing the name of the event.
			data: A dict containing the JSON payload.
			namespace: A string containing the namespace.
			callback: A function containing the emit callback function.
		'''

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
		'''Handles incoming socket messages.

		The msg_id is first checked if its contained in the _listeners dict whos
		function is scalled if True. Else the message is added to the _msg_queue
		to be processed.

		Args:
			data: A JSON dict containing the received socket data package.
		'''

		if 'msg_id' in data:
			if data['msg_id'] in self._listeners:
				result = data['result']
				self._listeners[data['msg_id']](*result.get('args'), **result.get('kwargs'))
			else:
				self._msg_queue[data['msg_id']] = (data, time.time())


	def _wait_broker_response(self, msg_id, timeout=60):
		'''Waits for expected incoming socket message by msg_id

		Args:
			msg_id: A string containing a message ID.
			timeout: An integer used as the timeout period.
		'''

		start = time.time()

		while time.time() - start < timeout:
			if msg_id in copy(list(self._msg_queue.keys())):
				res = self._msg_queue[msg_id][0]
				del self._msg_queue[msg_id]
				print('WAIT RECV', flush=True)
				return res.get('result')
			time.sleep(0.01)

		return {
			'error': 'No response.'
		}

	
	def _clean_msg_queue(self):
		''' Removes messages from _msg_queue older than 120 seconds. '''

		try:
			for msg_id in copy(list(self._msg_queue.keys())):
				if time.time() - self._msg_queue[msg_id][1] > 120:
					del self._msg_queue[msg_id]
		except Exception:
			print(traceback.format_exc())



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
		'''Sends socket message to broker and waits for response.

		Args:
			broker: A string containing the name of the broker, recognised
					by the broker application the message is sent to.
			broker_id: A string containing the ID of the user broker.
			func: A string containing the command to be performed on the
				  broker application.
		Returns:
			A dict containing the resulting message response or error response.
		'''

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
			print(f"[brokerRequest] Send: ({msg_id}) {time.time()}, {data}")
			# self.sio.emit('broker_cmd', data=data, namespace='/admin')
			# result = self._wait_broker_response(msg_id)

			# self.zmq_dealer_socket.send_json(data, zmq.NOBLOCK)
			self._send_queue.append(data)
			result = self._wait_broker_response(msg_id)
			
			# result = self.zmq_dealer_socket.recv_json()
			print(f"[brokerRequest] Result: ({msg_id}) {time.time()}, {result} | MSGs {len(self._msg_queue)}")
			
		except Exception:
			print(f"[brokerRequest] {traceback.format_exc()}")
			result = {
				'error': 'No response.'
			}
		finally:
			return result
			


	def mainBrokerRequest(self, broker, broker_id, func, *args, **kwargs):
		'''Sends socket message to broker and waits for response.

		This message is sent through the main_sio socket which is connected
		to the main server.

		Args:
			broker: A string containing the name of the broker, recognised
					by the broker application the message is sent to.
			broker_id: A string containing the ID of the user broker.
			func: A string containing the command to be performed on the
				  broker application.
		Returns:
			A dict containing the resulting message response or error response.
		'''
		
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
		'''Maps msg_id to a listener function.

		Everytime a socket message with this msg_id is received
		this listener function is called.

		Args:
			msg_id: A string containing a message ID.
			listener: A function to be called when receiving the
					  appropriate socket message.
		'''

		self._listeners[msg_id] = listener


	def restartScripts(self):
		'''Restarts all previously running scripts on startup.

		Checks user database to see if their script was previously running
		to continue that state on startup. A check is done to make sure 
		the user is using this server.
		'''

		print(f"[restartScripts] WORKERS: {int(self.redis_client.get('workers_complete').decode())}", flush=True)
		while int(self.redis_client.get("workers_complete").decode()) != 5:
			time.sleep(1)
		time.sleep(1)

		print("RESTARTING SCRIPTS...")
		all_users = self.getDb().getAllUsers()

		server_number = self.app.config["SERVER"]
		script_count = 0
		start_time = time.time()
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
									script_count += 1
									print(f"SCRIPT COUNT: {script_count}")
									# if script_count % 5 == 0:
									# 	account._runStrategyScript(strategy_id, broker_id, [account_id], input_variables)
									# else:
									# 	Thread(target=account._runStrategyScript, args=(strategy_id, broker_id, [account_id], input_variables)).start()

		print("RESTART COMPLETE ({:.2f}s)".format(time.time() - start_time))


	def handleListenerMessage(self, message):
		'''Checks if message msg_id is handled by a listener.

		If the msg_id is not handled by a listener it is added to the _msg_queue.
		Listener functions are threaded to avoid blocking.

		Args:
			message: A dict containing a received socket message.
		'''

		if "msg_id" in message:
			if message["msg_id"] in self._listeners:
				result = message['result']
				Thread(target=self._listeners[message['msg_id']], args=result.get('args'), kwargs=result.get('kwargs')).start()
			else:
				self._msg_queue[message['msg_id']] = (message, time.time())

	
	def handleRequestMessage(self, message):
		'''Handles received socket message commands.

		A response message is sent once the command has been handled.

		Args:
			message: A dict containing a received socket message.
		'''

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
		''' Message loop for connected ZMQ sockets.	'''

		self.zmq_pull_socket = self.zmq_context.socket(zmq.PULL)
		self.zmq_pull_socket.connect("tcp://zmq_broker:5555")

		self.zmq_sub_socket = self.zmq_context.socket(zmq.SUB)
		self.zmq_sub_socket.connect("tcp://zmq_broker:5556")
		self.zmq_sub_socket.setsockopt(zmq.SUBSCRIBE, b'')

		self.zmq_poller = zmq.Poller()
		self.zmq_poller.register(self.zmq_pull_socket, zmq.POLLIN)
		self.zmq_poller.register(self.zmq_sub_socket, zmq.POLLIN)

		clean_check = time.time()
		while True:
			try:
				socks = dict(self.zmq_poller.poll())

				if self.zmq_pull_socket in socks:
					message = self.zmq_pull_socket.recv_json()
					print(f"[zmq_message_loop] {message}", flush=True)

					if message.get("type") == "request":
						Thread(target=self.handleRequestMessage, args=(message["message"],)).start()
					else:
						print(f"[handleListenerMessage] {time.time()} {message}", flush=True)
						self.handleListenerMessage(message)

				if self.zmq_sub_socket in socks:
					message = self.zmq_sub_socket.recv_json()
					if message.get("type") == "price":
						# if self.connection_id == 0:
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

				if time.time() - clean_check > 30:
					clean_check = time.time()
					self._clean_msg_queue()

			except Exception:
				print(traceback.format_exc())
	
	def zmq_send_loop(self):
		''' Loop handling messages stored in _send_queue. '''

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
		'''Initializes main member variables of Controller object.

		Initializes Redis, Socket IO, ZMQ, Accounts, Database, Charts, Brokers
		Spots.
		'''

		self.redis_client = Redis(host='redis', port=6379, password="dev")
		
		self.sio = self.setupSio(self.app.config['STREAM_URL'])
		self.sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		if not self.app.config['IS_MAIN_STREAM']:
			self.main_sio = self.setupSio(self.app.config['MAIN_STREAM_URL'])
			self.main_sio.on('broker_res', handler=self.onCommand, namespace='/admin')

		self._setup_zmq_connections()

		if self.connection_id == 0:
			self.redis_client.set("workers_complete", 0)

		self.redis_client.set("strategies_" + str(self.connection_id), json.dumps({}))
		all_handled_keys = list(self.redis_client.hgetall("handled").keys())
		if len(all_handled_keys):
			self.redis_client.hdel("handled", *all_handled_keys)

		self.accounts = Accounts(self)
		self.db = Database(self, self.app.config['ENV'])
		self.charts = Charts(self)
		self.brokers = Brokers(self)

		self.spots = Spots(self, [
			'USD', 'EUR', 'AUD', 'CAD', 'CHF', 'GBP',
			'JPY', 'MXN', 'NOK', 'NZD', 'SEK',
			'RUB', 'CNY', 'TRY', 'ZAR', 'PLN',
			'HUF', 'CZK', 'SGD', 'HKD', 'DKK'
		])


	def performRestartScripts(self):
		''' Calls restartScripts if enabled in config. '''
		
		print(f"RESTART SCRIPTS? {self.app.config['RESTART_SCRIPTS_ON_STARTUP']}")
		if self.app.config['RESTART_SCRIPTS_ON_STARTUP']:
			Thread(target=self.restartScripts).start()


	def check_auth_key(self, key, strategy_id):
		'''Checks JWT auth key validity.
		
		Args:
			key: A string containing a JWT token.
			strategy_id: A string containing the ID of the user strategy.
		Returns:
			A tuple of a string containing user_id or a dict containing an error message
			and an integer containing the status code. 
		'''

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
	'''A dict mapping broker name strings to the parent Broker object for that provider.
	
	Attributes:
		ctrl: A reference to the Controller object.
		ib_port_sessions: A dict mapping active port numbers to their session info. (Deprecated)
	'''

	def __init__(self, ctrl):
		'''Initializes member variables and parent Broker objects.

		Args:
			ctrl: A reference to the Controller object.
		'''

		self.ctrl = ctrl
		self.ib_port_sessions = {}

		options = self._get_options()

		for k, v in options.items():
			self.ctrl.charts._init_broker_charts(k)
			self[k] = self._init_broker(k, v)

	def _get_options(self):
		'''Reads broker JSON config file.

		Retuns:
			A dict containing broker configuration information. 
		'''

		path = self.ctrl.app.config['BROKERS']
		if os.path.exists(path):
			with open(path, 'r') as f:
				return json.load(f)
		else:
			raise Exception('Broker options file does not exist.')

	def _get_spotware_items(self):
		'''Loads Spotware asset and symbol data from file.

		Returns:
			A dict containing Spotware asset and symbol data. 
		'''

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
		'''Initializes parent Broker object from broker config file.

		Args:
			name: A string containing the name of the broker provider.
			options: A dict containing the broker config file information.
		'''

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
		'''Writes broker configuration to file.
		
		Args:
			name: A string containing the name of the broker provider.
			obj: A dict containing new config options.
		'''
		options = self._get_options()
		options[name] = obj

		path = self.ctrl.app.config['BROKERS']
		with open(path, 'w') as f:
			f.write(json.dumps(f, indent=2))

	def getUsedPorts(self):
		'''Generates a list of current actively used IB ports.

		Returns:
			A list containing currently active IB ports.
		'''

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
		'''Maps a port number to IB client.

		Args:
			client: A user IB object.
			port: An integer representing the active port number.
		'''

		port = str(port)
		if port in self.ib_port_sessions:
			self.ib_port_sessions[port]['expiry'] = time.time() + (60*10)
			self.ib_port_sessions[port]['client'] = client
		else:
			self.ib_port_sessions[port] = { 'client': client, 'expiry': time.time() + (60*10), 'ips': [] }



class Charts(dict):
	'''A dict mapping brokers and instruments to a Chart object.

	Attributes:
		ctrl: A reference to the Controller object.
		queue: A queue to enforce synchronous Chart initialization.
	'''

	def __init__(self, ctrl):
		self.ctrl = ctrl
		self.queue = DictQueue()
		# self._generate_broker_keys()


	# def _generate_broker_keys(self):
	# 	for k in self.ctrl.brokers:
	# 		self[k] = {}

	def _init_broker_charts(self, broker):
		'''Maps broker name to an empty dict.

		Args:
			broker: A string containing the name of the broker provider.
		'''

		self[broker] = {}


	def createChart(self, broker, product, await_completion):
		'''Initializes Chart object and maps to broker name and instrument (product).

		If the Chart already exists, the existing Chart is returned.

		Args:
			broker: A string containing the name of the broker provider.
			product: A string containing the financial instrument name.
			await_completion: A bool passed to the Chart object on initialization.
		Returns:
			A Chart object mapped to the broker and instrument (product)
		'''

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
		'''Retrieves the Chart object mapped to broker name and instrument (product).

		If the Chart object does not exists it is initialized.

		Args:
			broker: A string containing the name of the broker provider.
			product: A string containing the financial instrument name.
			await_completion: A bool passed to the Chart object on initialization.
		Returns:
			A Chart object mapped to the broker and instrument (product)
		'''

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
		'''Deletes Chart object mapping.

		Args:
			broker_name: A string containing the name of the broker provider.
			product: A string containing the financial instrument name.
		'''

		try:
			del broker[broker_name][product]
		except:
			pass


class Accounts(dict):
	'''A dict mapping user ID to Account object.
	
	Attributes:
		ctrl: A reference to the Controller object.
		queue: A queue to enforce synchronous Chart initialization.
	'''

	def __init__(self, ctrl):
		self.ctrl = ctrl
		self.queue = DictQueue()

	def initAccount(self, user_id):
		'''Initializes Account object and maps to user_id.

		A check is done to see if Account object already exists.

		Args:
			user_id: A string containing user ID.
		Returns:
			An Account object mapped to the user_id.
		'''

		if user_id not in self:
			try:
				acc = Account(self.ctrl, user_id)
				self[user_id] = acc
					
			except AccountException:
				return None
		
		return self.get(user_id)


	def addAccount(self, account):
		'''Maps user ID to Account object.

		Args:
			account: An Account object.
		'''

		self[account.user_id] = account

	def getAccount(self, user_id):
		'''Retrieves Account object mapped to user_id.

		If mapping does not exists, Account object is initialized.

		Args:
			user_id: A string containing user ID.
		'''

		acc = self.get(user_id)
		if acc is None:
			return self.queue.handle(user_id, self.initAccount, user_id)
		else:
			return acc

	def deleteAccount(self, user_id):
		'''Deletes user_id mapping.

		Args:
			user_id: A string containing user ID.
		'''

		if user_id in self:
			del self[user_id]


class Spots(dict):
	'''A dict mapping currencies to their base USD spot rate wrapped by Spot object.

	Attributes:
		ctrl: A reference to the Controller object.
		prices: A dict containing the retrieved currency prices from the Fixer API.
	'''

	def __init__(self, ctrl, spots):
		'''Initializes member variables and retrieves curreny spot prices.
		
		Args:
			ctrl: A reference to the Controller object.
			spots: A dict containing old currency spot rates.
		'''

		self.ctrl = ctrl
		self.prices = self._get_prices()

		if self.prices is None:
			self._init_spots_backup(spots)
		else:
			self._init_spots()


	def _get_prices(self):
		'''Retrieves spot prices from Fixer API.

		Returns:
			A dict containing curreny data retrieved from Fixer API.
		'''

		uri = "http://data.fixer.io/api/latest"
		res = requests.get(uri + f"?access_key={self.ctrl.app.config['FIXER_IO_ACCESS_KEY']}&base=USD")
		if res.status_code == 200:
			data = res.json()
			if data.get("success"):
				return data.get("rates")
		
		return None


	def _init_spots(self):
		''' Maps currency to Spot object containing currency rate. '''

		for i in self.prices:
			self[i] = tl.Spot(self.ctrl, i, rate=1/self.prices[i])


	def _init_spots_backup(self, spots):
		''' Maps currency to Spot object containing old currency rate. '''
		for i in spots:
			self[i] = tl.Spot(self.ctrl, i)
			self[i].getRate()


	def _update_spots(self):
		''' Updates currency spot rate. '''

		for i in self:
			self[i].getRate()



from app import tradelib as tl
from app.account import Account
from app.db import Database
from app.error import AccountException
