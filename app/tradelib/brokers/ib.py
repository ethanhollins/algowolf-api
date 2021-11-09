import time
import traceback
import numpy as np
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from copy import copy
from threading import Thread
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException


class IB(Broker):

	def __init__(self,
		ctrl, username, password, port=None, user_account=None, strategy_id=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False, is_parent=False
	):
		print('IB INIT')

		super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.IB_NAME, accounts, display_name, is_dummy, False)

		self.username = username
		self.password = password
		if port is not None:
			self.port = str(port)
		else:
			self.port = None
		# else:
			# self.port = self.findUnusedPort()

		# if not is_parent:
		# 	self.assignPort()

		self.ips = []
		self.is_parent = is_parent

		self._gateway_loaded = False
		self._queue = []

		# if self.port is not None:
		# 	self._add_user()
		# 	# if not self._gateway_loaded:
		# 	# 	self._start_gateway()

		# elif not is_parent:
		self._add_user()


	def _add_user(self):
		print('Add User IB')

		if self.userAccount is not None:
			user_id = self.userAccount.userId
		else:
			user_id = None

		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'add_user',
			user_id, self.strategyId, self.brokerId, self.username, 
			self.password, is_parent=self.is_parent
		)

		if 'error' in res:
			return self._add_user()
		else:
			if res.get('_gateway_loaded'):
				self._gateway_loaded = True
			return res


	def isLoggedIn(self):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'isLoggedIn',
			self.port
		)
		print(f'[isLoggedIn] {res}')

		if not 'error' in res:
			self.is_auth = res.get('result')
			
		return self.is_auth


	def findUnusedPort(self):

		queue_id = self.generateReference()
		self._queue.append(queue_id)
		print(f'[findUnusedPort] {self._queue}')
		while self._queue.index(queue_id) > 0:
			pass
		print(f'[findUnusedPort] {queue_id}, {self.port}, {self.brokerId}')

		if self.port is None:
			used_ports = self.ctrl.brokers.getUsedPorts()

			res = self.ctrl.brokerRequest(
				self.name, self.brokerId, 'findUnusedPort',
				used_ports
			)

			print(f'[findUnusedPort] {res}')

			if 'error' in res:
				del self._queue[0]
				return None
			else:
				port = res.get('result')
				self.setPort(port)
				print(f'[findUnusedPort] Port Set: {port}')
				del self._queue[0]
				return port

		else:
			del self._queue[0]
			return self.port


	def setPort(self, port):
		self.port = port

		if self.port is not None:
			self.port = str(port)
			self.assignPort()
			self._add_user()
			self._subscribe_gui_updates()
			self._start_gateway()


	def findUser(self):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'find_user',
			self.userAccount.userId, self.strategyId, self.brokerId
		)

		if res.get('port') != -1:
			self.port = res.get('port')
			self._subscribe_gui_updates()
			self.is_auth = True
			self._gateway_loaded = True



	def assignPort(self):
		self.ctrl.brokers.assignPort(self, self.port)


	def _download_historical_data_broker(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		return


	def _get_all_positions(self, account_id):
		return


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		return


	def modifyPosition(self, pos, sl_price, tp_price):
		return


	def deletePosition(self, pos, lotsize):
		return


	def _get_all_orders(self, account_id):
		return


	def getAllAccounts(self):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAllAccounts', self.port
		)

		if self.userAccount is not None:
			broker = self.userAccount.getBroker(self.brokerId)
			accounts = {}

			for acc in res['accounts']:
				if acc in broker['accounts']:
					accounts[acc] = broker['accounts'][acc]
				else:
					accounts[acc] = {
						"active": True,
						"nickname": "",
						"is_demo": False,
					}

				if acc in self.accounts:
					self.accounts[acc].update(res['accounts'][acc])
					self.accounts[acc].update(accounts[acc])
				else:
					self.accounts[acc] = res['accounts'][acc]
					self.accounts[acc].update(accounts[acc])

			self.userAccount.updateBroker(self.brokerId, { 'accounts': accounts })


		return res


	def getAccountInfo(self, account_id):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAccountInfo', self.port, account_id
		)
		print(f'[getAccountInfo] {res}')

		if not 'error' in res:
			if account_id in self.accounts:
				self.accounts[account_id].update(res[account_id])
			else:
				self.accounts[account_id] = res[account_id]
			
			print(f'[getAccountInfo] {self.accounts}')

			return { account_id: self.accounts[account_id] }

		else:
			if account_id in self.accounts:
				return {  account_id: self.accounts[account_id] }
			else:
				return {  account_id: {} }


	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		return


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price):
		return


	def deleteOrder(self, order):
		return


	def _start_gateway(self):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_start_gateway', self.port
		)

		if not 'error' in res:
			return True
		else:
			return False


	def _subscribe_gui_updates(self):
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'ib', self.brokerId, '_subscribe_gui_updates', self.port, stream_id
		)
		self.ctrl.addBrokerListener(stream_id, self.onGuiUpdate)


	def onGuiUpdate(self, *args, **kwargs):
		print(f'[onGuiUpdate] {args}')

		message = args[0]
		if message == 'gateway_loaded':
			self._gateway_loaded = True


		self.handleOnGui(None, message)
