import os
import json
import socketio
import requests
import shortuuid
import time
from copy import copy
from urllib.request import urlopen
from flask import abort
from threading import Thread


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


class ContinuousThreadHandler(object):

	def __init__(self):
		self.jobs = {}
		self._to_add = []
		self._to_stop = []
		self._running = True
		self.thread = Thread(target=self.run)
		self.thread.start()

	def generateReference(self):
		return shortuuid.uuid()


	def addJob(self, func, *args, **kwargs):
		ref = self.generateReference()
		self._to_add.append((ref, func, args, kwargs))
		return ref


	def stopJob(self, ref):
		self._to_stop.append(ref)


	def jobsHandler(self):
		for j in self._to_add:
			self.jobs[j[0]] = j[1:]

		for j in self._to_stop:
			if ref in self.jobs:
				del self.jobs[ref]


	def run(self):
		while self._running:
			for j in self.jobs.values():
				j[0](*j[1], **j[2])
			self.jobsHandler()

	def stop(self):
		self._running = False
		self.thread.join()


class Controller(object):

	def __init__(self, app):
		self.app = app
		self.continuousThreadHandler = ContinuousThreadHandler()
		self.sio = self.setupSio()
		self.accounts = Accounts(self)
		self.brokers = Brokers(self)
		self.charts = Charts(self)
		self.db = Database(self, app.config['DATABASE'])

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
			except socketio.exceptions.ConnectionError:
				time.sleep(1)

		return sio

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
			self[k] = self._init_broker(k, v)

	def _get_options(self):
		path = self.ctrl.app.config['BROKERS']
		if os.path.exists(path):
			with open(path, 'r') as f:
				return json.load(f)
		else:
			raise Exception('Broker options file does not exist.')

	def _init_broker(self, name, options):
		key = options.get('key')
		is_demo = options.get('is_demo')
		if name == tl.broker.OANDA_NAME:
			accounts = options.get('accounts')
			return tl.broker.Oanda(self.ctrl, key, is_demo, accounts=accounts)
		elif name == tl.broker.IG_NAME:
			username = options.get('username')
			password = options.get('password')
			return tl.broker.IG(self.ctrl, username, password, key, is_demo)

	def getBroker(self, name):
		return self.get(name)


class Charts(dict):

	def __init__(self, ctrl):
		self.ctrl = ctrl
		self.queue = DictQueue()
		self._generate_broker_keys()


	def _generate_broker_keys(self):
		for k in self.ctrl.brokers:
			self[k] = {}


	def createChart(self, broker, product):
		if product not in self[broker.name]:
			chart = tl.Chart(self.ctrl, broker, product)
			self[broker.name][product] = chart
			return chart
		else:
			return self[broker.name][product]


	def getChart(self, broker_name, product):
		if broker_name in self:
			if product in self[broker_name]:
				return self[broker_name][product]
			else:
				return self.queue.handle(
					f'{broker_name}:{product}',
					self.createChart,
					self.ctrl.brokers.get(broker_name),
					product
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


from app import tradelib as tl
from app.account import Account
from app.db import Database
from app.error import AccountException
