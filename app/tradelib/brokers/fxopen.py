import requests
import asyncio
import pandas as pd
import numpy as np
import json
import time
import math
import traceback
import dateutil.parser
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from copy import copy
from threading import Thread
from datetime import datetime, timedelta
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException


class FXOpen(Broker):

	def __init__(self, 
		ctrl, key, web_api_id, web_api_secret, is_demo, 
		user_account=None, strategy_id=None, broker_id=None, 
		accounts={}, display_name=None, is_dummy=False, is_parent=False
	):
		super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.FXOPEN_NAME, accounts, display_name, is_dummy, True)
		
		print(f'FXOPEN INIT 1, {key}, {web_api_id}, {web_api_secret}')

		self.dl = tl.DataLoader(broker=self)
		self.data_saver = tl.DataSaver(broker=self)

		self._key = key
		self._web_api_id = web_api_id
		self._web_api_secret = web_api_secret
		self._is_demo = is_demo
		self.is_dummy = is_dummy
		self.is_parent = is_parent

		self._last_update = time.time()
		self._subscriptions = []
		self._account_update_queue = []
		self._is_connected = False

		self.is_auth = self._add_user()

		# Thread(target=self._handle_account_updates).start()

		# if not is_dummy:
		# 	for account_id in self.getAccounts():
		# 		if account_id != tl.broker.PAPERTRADER_NAME:
		# 			self._subscribe_account_updates(account_id)

		# 	# Handle strategy
		# 	if self.userAccount and self.brokerId:
		# 		self._handle_live_strategy_setup()

		# print('FXOPEN INIT 1')
		# if is_parent:
		# 	# Load Charts
		# 	CHARTS = ['EUR_USD']
		# 	PERIODS = [tl.period.FIVE_SECONDS, tl.period.ONE_MINUTE]
		# 	for instrument in CHARTS:
		# 		chart = self.createChart(instrument, await_completion=True)
		# 		# self.data_saver.subscribe(chart, PERIODS)


	def _periodic_check(self):
		TWENTY_SECONDS = 20
		self._last_update = time.time()
		# Check most recent Oanda `HEARTBEAT` was received or reconnect
		while self.is_running:
			if time.time() - self._last_update > TWENTY_SECONDS:
				print('RECONNECT')
				if self._is_connected:
					self._is_connected = False
					# Run disconnected callback
					self.handleOnSessionStatus({
						'broker': self.name,
						'timestamp': math.floor(time.time()),
						'type': 'disconnected',
						'message': 'The session has been disconnected.'
					})

					# Perform periodic refresh
					self._reconnect()
			time.sleep(5)

		for sub in self._subscriptions:
			for i in sub.res:
				i.close()


	def _add_user(self):
		print('Add User')

		if self.userAccount is not None:
			user_id = self.userAccount.userId
		else:
			user_id = None

		res = self.ctrl.brokerRequest(
			'fxopen', self.brokerId, 'add_user',
			user_id, self.brokerId, self._key, self._web_api_id, self._web_api_secret, self._is_demo, self.accounts,
			is_parent=self.is_parent, is_dummy=self.is_dummy
		)

		if 'error' in res:
			if res['error'] == 'No response.':
				return self._add_user()
			elif res['error'] == 'Not Authorised':
				return False
			else:
				return False

		else:
			return True


	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		
		dl_start = None
		dl_end = None
		if start:
			dl_start = tl.utils.convertTimeToTimestamp(start)
		if end:
			dl_end = tl.utils.convertTimeToTimestamp(end)

		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_download_historical_data_broker',
			product, period, tz=tz, start=dl_start, end=dl_end,
			count=count, force_download=force_download
		)

		if 'error' in res:
			result = self._create_empty_df(period)
		else:
			for i in res:
				res[i] = { float(k):v for k,v in res[i].items() }
			result = pd.DataFrame.from_dict(res, dtype=float)
			result.index = result.index.astype(int)

		return result


	# Order Requests


	def _handle_order_create(self, res):
		oanda_id = res.get('id')
		result = {}

		if res.get('type') == 'LIMIT_ORDER':
			order_type = tl.LIMIT_ORDER
		elif res.get('type') == 'STOP_ORDER':
			order_type = tl.STOP_ORDER
		else:
			return result

		order_id = res.get('id')
		account_id = res.get('accountID')
		product = res.get('instrument')
		direction = tl.LONG if float(res.get('units')) > 0 else tl.SHORT
		lotsize = self.convertToLotsize(abs(float(res.get('units'))))
		entry_price = float(res.get('price'))

		sl = None
		if res.get('stopLossOnFill'):
			if res['stopLossOnFill'].get('price'):
				sl = float(res['stopLossOnFill'].get('price'))
			elif res['stopLossOnFill'].get('distance'):
				if direction == tl.LONG:
					sl = entry_price + float(res['stopLossOnFill'].get('distance'))
				else:
					sl = entry_price - float(res['stopLossOnFill'].get('distance'))
			
		tp = None
		if res.get('takeProfitOnFill'):
			tp = float(res['takeProfitOnFill'].get('price'))

		ts = tl.convertTimeToTimestamp(datetime.strptime(
			res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
		))

		order = tl.Order(
			self,
			order_id, account_id, product,
			order_type, direction, lotsize,
			entry_price, sl, tp, ts
		)
		self.orders.append(order)

		result[self.generateReference()] = {
			'timestamp': ts,
			'type': order_type,
			'accepted': True,
			'item': order
		}

		# Add update to handled
		self._handled[oanda_id] = result

		return result


	def _handle_order_fill(self, account_id, res):
		# Retrieve position information
		oanda_id = res.get('id')
		result = {}

		ts = tl.convertTimeToTimestamp(datetime.strptime(
			res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
		))

		from_order = self.getOrderByID(res.get('orderID'))
		if from_order is not None:
			del self.orders[self.orders.index(from_order)]

			self.handleOnTrade(account_id, {
				self.generateReference(): {
					'timestamp': from_order.close_time,
					'type': tl.ORDER_CANCEL,
					'accepted': True,
					'item': from_order
				}
			})

		if res.get('tradeOpened'):
			order_id = res['tradeOpened'].get('tradeID')

			account_id = res['accountID']
			product = res.get('instrument')
			direction = tl.LONG if float(res['tradeOpened'].get('units')) > 0 else tl.SHORT
			lotsize = self.convertToLotsize(abs(float(res['tradeOpened'].get('units'))))
			entry_price = float(res.get('price'))
			
			if res.get('reason') == 'LIMIT_ORDER':
				order_type = tl.LIMIT_ENTRY
			elif res.get('reason') == 'STOP_ORDER':
				order_type = tl.STOP_ENTRY
			else:
				order_type = tl.MARKET_ENTRY


			pos = tl.Position(
				self,
				order_id, account_id, product,
				order_type, direction, lotsize,
				entry_price, None, None, ts
			)
			self.positions.append(pos)

			result[self.generateReference()] = {
				'timestamp': ts,
				'type': order_type,
				'accepted': True,
				'item': pos
			}

		if res.get('tradeReduced'):
			order_id = res['tradeReduced'].get('tradeID')
			pos = self.getPositionByID(order_id)

			if pos is not None:
				cpy = tl.Position.fromDict(self, pos)
				cpy.lotsize = self.convertToLotsize(abs(float(res['tradeReduced'].get('units'))))
				cpy.close_price = float(res['tradeReduced'].get('price'))
				cpy.close_time = tl.convertTimeToTimestamp(datetime.strptime(
					res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
				))

				# Modify open position
				pos.lotsize -= self.convertToLotsize(abs(float(res['tradeReduced'].get('units'))))

				result[self.generateReference()] = {
					'timestamp': ts,
					'type': tl.POSITION_CLOSE,
					'accepted': True,
					'item': cpy
				}

		if res.get('tradesClosed'):
			if res.get('reason') == 'STOP_LOSS_ORDER':
				order_type = tl.STOP_LOSS
			elif res.get('reason') == 'TAKE_PROFIT_ORDER':
				order_type = tl.TAKE_PROFIT
			else:
				order_type = tl.POSITION_CLOSE

			for i in range(len(res['tradesClosed'])):
				trade = res['tradesClosed'][i]
				order_id = trade.get('tradeID')
				pos = self.getPositionByID(order_id)
				if pos is not None:
					pos.close_price = float(trade.get('price'))
					pos.close_time = tl.convertTimeToTimestamp(datetime.strptime(
						res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
					))

					result[self.generateReference()] = {
						'timestamp': ts,
						'type': order_type,
						'accepted': True,
						'item': pos
					}
					del self.positions[self.positions.index(pos)]



		# Add update to handled
		self._handled[oanda_id] = result

		return result


	def _handle_order_cancel(self, res):
		oanda_id = res.get('id')
		result = {}

		order_id = res.get('orderID') 
		order = self.getOrderByID(order_id)

		ts = tl.convertTimeToTimestamp(datetime.strptime(
			res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
		))
		if order is not None:
			order.close_time = ts
			del self.orders[self.orders.index(order)]
			result[self.generateReference()] = {
				'timestamp': ts,
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': order
			}

			# Add update to handled
			self._handled[oanda_id] = result

		else:
			for trade in self.positions:
				if trade.sl_id == order_id:
					trade.sl = None
					trade.sl_id = None

					result[self.generateReference()] = {
						'timestamp': ts,
						'type': tl.MODIFY,
						'accepted': True,
						'item': trade
					}

				elif trade.tp_id == order_id:
					trade.tp = None
					trade.tp_id = None

					result[self.generateReference()] = {
						'timestamp': ts,
						'type': tl.MODIFY,
						'accepted': True,
						'item': trade
					}

			if result:
				# Add update to handled
				self._handled[oanda_id] = result

		return result



	def _handle_stop_loss_order(self, res):
		oanda_id = res.get('id')
		order_id = res.get('tradeID')
		pos = self.getPositionByID(order_id)

		result = {}
		if pos is not None:
			ts = tl.convertTimeToTimestamp(datetime.strptime(
				res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
			))

			pos.sl = float(res.get('price'))
			pos.sl_id = oanda_id

			result[self.generateReference()] = {
				'timestamp': ts,
				'type': tl.MODIFY,
				'accepted': True,
				'item': pos
			}

			# Add update to handled
			self._handled[oanda_id] = result

		return result


	def _handle_take_profit_order(self, res):
		oanda_id = res.get('id')
		order_id = res.get('tradeID')
		pos = self.getPositionByID(order_id)

		result = {}
		if pos is not None:
			ts = tl.convertTimeToTimestamp(datetime.strptime(
				res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
			))

			pos.tp = float(res.get('price'))
			pos.tp_id = oanda_id

			result[self.generateReference()] = {
				'timestamp': ts,
				'type': tl.MODIFY,
				'accepted': True,
				'item': pos
			}

			# Add update to handled
			self._handled[oanda_id] = result

		return result


	def _get_all_positions(self, account_id):

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_get_all_positions',
			account_id
		)

		if result is not None and not 'error' in result:
			for account_id in result:
				for i in range(len(result[account_id])):
					result[account_id][i] = tl.Position.fromDict(self, result[account_id][i])

			return result

		else:
			self.is_auth = False
			return { account_id: [] }


	def _handle_tp_sl(self, order, sl_range, tp_range, sl_price, tp_price):
		payload = {}

		# Handle Stop Loss
		req_sl = None
		if sl_price:
			req_sl = round(sl_price, 5)
		elif sl_range:
			sl_range = tl.convertToPrice(sl_range)
			if order['direction'] == tl.LONG:
				req_sl = round(order['entry_price'] - sl_range, 5)

			else:
				req_sl = round(order['entry_price'] + sl_range, 5)

		if req_sl is not None:
			payload['stopLoss'] = {
				'timeInForce': 'GTC',
				'price': str(req_sl)
			}

		# Handle Take Profit
		req_tp = None
		if tp_price:
			req_tp = round(tp_price, 5)
		elif tp_range:
			tp_range = tl.convertToPrice(tp_range)
			if order['direction'] == tl.LONG:
				req_tp = round(order['entry_price'] + tp_range, 5)

			else:
				req_tp = round(order['entry_price'] - tp_range, 5)

		if req_tp is not None:
			payload['takeProfit'] = {
				'timeInForce': 'GTC',
				'price': str(req_tp)
			}

		result = {}
		if len(payload):
			# endpoint = f'/v3/accounts/{order["account_id"]}/trades/{order["order_id"]}/orders'
			# res = self._session.put(
			# 	self._url + endpoint,
			# 	headers=self._headers,
			# 	data=json.dumps(payload)
			# )

			broker_result = self.ctrl.brokerRequest(
				self.name, self.brokerId, 'modifyPosition',
				order, req_sl, req_tp
			)

			print(f'HANDLE TP SL: {broker_result}')

			status_code = broker_result.get('status')
			res = broker_result.get('result')

			if 200 <= status_code < 300:
				if res.get('stopLossOrderTransaction'):
					result.update(self._wait(
						res['stopLossOrderTransaction'].get('id'),
						self._handle_stop_loss_order,
						res['stopLossOrderTransaction']
					))

				if res.get('takeProfitOrderTransaction'):
					result.update(self._wait(
						res['takeProfitOrderTransaction'].get('id'),
						self._handle_take_profit_order,
						res['takeProfitOrderTransaction']
					))

		return result


	def authCheck(self):
		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'authCheck'
		)

		if result is not None and result.get('result'):
			self.is_auth = True
		else:
			self.is_auth = False



	def convertToLotsize(self, size):
		return size / 100000


	def convertToUnitSize(self, size):
		return size * 100000


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price,
		override=False
	):
		# Check auth
		if override:
			status = 200
		else:
			_, status = key_or_login_required(self.brokerId, AccessLevel.DEVELOPER, disable_abort=True)

		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createPosition',
			product, lotsize, direction,
			account_id, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		# status_code = res.status_code
		# res = res.json()
		if 200 <= status_code < 300:
			if res.get('orderFillTransaction'):
				# Process entry
				result.update(self._wait(
					res['orderFillTransaction'].get('id'),
					self._handle_order_fill, 
					res['orderFillTransaction']
				))

				# Handle stoploss and takeprofit
				for i in copy(result).values():
					result.update(
						self._handle_tp_sl(i.get('item'), sl_range, tp_range, sl_price, tp_price)
					)

			else:
				if res.get('orderCancelTransaction') is not None:
					msg = res['orderCancelTransaction'].get('reason')
				else:
					msg = 'No message available.'

				# Response error
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MARKET_ORDER,
						'accepted': False,
						'message': msg
					}
				})
		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MARKET_ORDER,
					'accepted': False,
					'message': msg
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MARKET_ORDER,
					'accepted': False,
					'message': 'Oanda internal server error.'
				}
			})
		
		return result


	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)


		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyPosition',
			pos, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		# status_code = res.status_code
		# res = res.json()
		if 200 <= status_code < 300:
			if res.get('stopLossOrderTransaction'):
				result.update(self._wait(
					res['stopLossOrderTransaction'].get('id'),
					self._handle_stop_loss_order,
					res['stopLossOrderTransaction']
				))

			if res.get('takeProfitOrderTransaction'):
				result.update(self._wait(
					res['takeProfitOrderTransaction'].get('id'),
					self._handle_take_profit_order,
					res['takeProfitOrderTransaction']
				))

		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': msg,
					'item': {
						'order_id': pos.order_id
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': 'Oanda internal server error.',
					'item': {
						'order_id': pos.order_id
					}
				}
			})

		return result


	def deletePosition(self, pos, lotsize, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)


		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deletePosition',
			pos, lotsize
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		if status_code == 200:
			if res.get('orderFillTransaction'):
				result.update(self._wait(
					res['orderFillTransaction'].get('id'),
					self._handle_order_fill,
					res['orderFillTransaction']
				))

			else:
				msg = 'No message available.'
				if res.get('orderCancelTransaction') is not None:
					msg = res['orderCancelTransaction'].get('reason')

				# Response error
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.POSITION_CLOSE,
						'accepted': False,
						'message': msg,
						'item': {
							'order_id': pos.order_id
						}
					}
				})

		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': msg,
					'item': {
						'order_id': pos.order_id
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': 'Oanda internal server error.',
					'item': {
						'order_id': pos.order_id
					}
				}
			})

		return result

	def _get_all_orders(self, account_id):
		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_get_all_orders',
			account_id
		)

		if result is not None and not 'error' in result:
			for account_id in result:
				for i in range(len(result[account_id])):
					result[account_id][i] = tl.Order.fromDict(self, result[account_id][i])

			return result
		else:
			self.is_auth = False
			return { account_id: [] }


	def getAllAccounts(self):

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAllAccounts'
		)

		print(result)

		return result


	def getAccountInfo(self, account_id, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)


		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAccountInfo',
			account_id
		)

		for account_id in result:
			result[account_id]['balance'] = self.ctrl.spots[result[account_id]['currency']].convertFrom(result[account_id]['balance'])

		print(f'ACCOUNT INFO: {result}')

		return result

	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price,
		override=False
	):
		# Check auth
		if override:
			status = 200
		else:
			_, status = key_or_login_required(self.brokerId, AccessLevel.DEVELOPER, disable_abort=True)

		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createOrder',
			product, lotsize, direction,
			account_id, order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		if 200 <= status_code < 300:
			print(json.dumps(res, indent=2))

			if res.get('orderCancelTransaction'):
				msg = res['orderCancelTransaction'].get('reason')

				# Response error
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': order_type,
						'accepted': False,
						'message': msg
					}
				})
			elif res.get('orderCreateTransaction'):
				result.update(self._wait(
					res['orderCreateTransaction'].get('id'),
					self._handle_order_create,
					res['orderCreateTransaction']
				))

		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': order_type,
					'accepted': False,
					'message': msg
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': order_type,
					'accepted': False,
					'message': 'Oanda internal server error.'
				}
			})

		return result

	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)


		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyOrder',
			order, lotsize, entry_price, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		if 200 <= status_code < 300:
			if res.get('orderCancelTransaction'):
				result.update(self._wait(
					res['orderCancelTransaction'].get('id'),
					self._handle_order_cancel,
					res['orderCancelTransaction']
				))

			if res.get('replacingOrderCancelTransaction'):
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MODIFY,
						'accepted': False,
						'message': res['replacingOrderCancelTransaction'].get('reason'),
						'item': {
							'order_id': order.order_id
						}
					}
				})

			else:
				if res.get('orderCreateTransaction'):
					result.update(self._wait(
						res['orderCreateTransaction'].get('id'),
						self._handle_order_create,
						res['orderCreateTransaction']
					))
				
		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': msg,
					'item': {
						'order_id': order.order_id
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': 'Oanda internal server error.',
					'item': {
						'order_id': order.order_id
					}
				}
			})

		return result

	def deleteOrder(self, order, override=False):
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)


		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deleteOrder',
			order
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		if 200 <= status_code < 300:
			print(json.dumps(res, indent=2))

			if res.get('orderCancelTransaction'):
				result.update(self._wait(
					res['orderCancelTransaction'].get('id'),
					self._handle_order_cancel,
					res['orderCancelTransaction']
				))

		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.ORDER_CANCEL,
					'accepted': False,
					'message': msg,
					'item': {
						'order_id': order.order_id
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.ORDER_CANCEL,
					'accepted': False,
					'message': 'Oanda internal server error.',
					'item': {
						'order_id': order.order_id
					}
				}
			})

		return result


	# Live utilities
	def _reconnect(self):
		for sub in copy(self._subscriptions):
			sub.receive = False

			# for i in copy(sub.res):
			# 	i.close()
			# 	del sub.res[sub.res.index(i)]

			# try:
			# 	if sub.sub_type == Subscription.ACCOUNT:
			# 		self._perform_account_connection(sub)
			# 	elif sub.sub_type == Subscription.CHART:
			# 		self._perform_chart_connection(sub)

			# except requests.exceptions.ConnectionError:
			# 	return


	def _encode_params(self, params):
		return urlencode(dict([(k, v) for (k, v) in iter(params.items()) if v]))


	def _subscribe_chart_updates(self, instrument, listener):
		print(f'SUBSCRIBE CHART: {instrument}')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'oanda', self.brokerId, '_subscribe_chart_updates', stream_id, instrument
		)
		self.ctrl.addBrokerListener(stream_id, listener)


	def _perform_chart_connection(self, sub):
		endpoint = f'/v3/accounts/{self.getAccounts()[0]}/pricing/stream'
		params = self._encode_params({
			'instruments': '%2C'.join(sub.args[0])
		})
		req = Request(f'{self._stream_url}{endpoint}?{params}', headers=self._headers)

		try:
			stream = urlopen(req, timeout=20)

			sub.setStream(stream)
			Thread(target=self._stream_price_updates, args=(sub,)).start()
		except Exception as e:
			time.sleep(1)
			print('[Oanda] Attempting price reconnect.')
			Thread(target=self._perform_chart_connection, args=(sub,)).start()
			return


	def _stream_price_updates(self, sub):

		while sub.receive:
			try:
				message = sub.stream.readline().decode('utf-8').rstrip()
				if not message.strip():
					sub.receive = False
				else:
					sub.listener(json.loads(message))

			except Exception as e:
				print(traceback.format_exc())
				sub.receive = False

		# Reconnect
		print('[Oanda] Price Updates Disconnected.')
		self._perform_chart_connection(sub)


	def onChartUpdate(self, chart, *args, **kwargs):
		# print(f'CHART UPDATE: {args}')

		update = args[0]
		if update.get('type') == 'HEARTBEAT':
			self._last_update = time.time()

		elif update.get('type') == 'PRICE':
			update_time = update.get('time')
			if len(update.get('asks')) == 0 or len(update.get('bids')) == 0:
				return
			ask = float(update.get('asks')[:3][-1].get('price'))
			if len(update.get('bids')) >= 4:
				bid = np.around(
					(float(update.get('bids')[:4][-1].get('price')) +
						float(update.get('bids')[:4][-2].get('price'))) / 2,
					decimals=5
				)
			else:
				bid = float(update.get('bids')[:3][-1].get('price'))

			if update_time is not None:
				# Convert time to datetime
				c_ts = tl.convertTimeToTimestamp(dateutil.parser.isoparse(update_time))
				result = []
				# Iterate periods
				for period in chart.getActivePeriods():
					if (isinstance(chart.bid.get(period), np.ndarray) and 
						isinstance(chart.ask.get(period), np.ndarray)):

						# Handle period bar end
						if period != tl.period.TICK:
							is_new_bar = chart.isNewBar(period, c_ts)
							if is_new_bar:
								bar_ts = chart.lastTs[period]
								result.append({
									'broker': self.name,
									'product': chart.product,
									'period': period,
									'bar_end': True,
									'timestamp': chart.lastTs[period],
									'item': {
										'ask': chart.ask[period].tolist(),
										'mid': chart.mid[period].tolist(),
										'bid': chart.bid[period].tolist()
									}
								})
								chart.lastTs[period] = tl.getNextTimestamp(period, chart.lastTs[period], now=c_ts - tl.period.getPeriodOffsetSeconds(period))
								print(f'[{period}] Prev: {bar_ts}, Next: {chart.lastTs[period]}')
								chart.ask[period] = np.array([chart.ask[period][3]]*4, dtype=np.float64)
								chart.bid[period] = np.array([chart.bid[period][3]]*4, dtype=np.float64)
								chart.mid[period] = np.array(
									[np.around(
										(chart.ask[period][3] + chart.bid[period][3])/2,
										decimals=5
									)]*4, 
								dtype=np.float64)

						# Ask
						if ask is not None:
							chart.ask[period][1] = ask if ask > chart.ask[period][1] else chart.ask[period][1]
							chart.ask[period][2] = ask if ask < chart.ask[period][2] else chart.ask[period][2]
							chart.ask[period][3] = ask

						# Bid
						if bid is not None:
							chart.bid[period][1] = bid if bid > chart.bid[period][1] else chart.bid[period][1]
							chart.bid[period][2] = bid if bid < chart.bid[period][2] else chart.bid[period][2]
							chart.bid[period][3] = bid

						# Mid
						if ask is not None and bid is not None:
							new_high = np.around((chart.ask[period][1] + chart.bid[period][1])/2, decimals=5)
							new_low = np.around((chart.ask[period][2] + chart.bid[period][2])/2, decimals=5)
							new_close = np.around((chart.ask[period][3] + chart.bid[period][3])/2, decimals=5)

							chart.mid[period][1] = new_high if new_high > chart.mid[period][1] else chart.mid[period][1]
							chart.mid[period][2] = new_low if new_low < chart.mid[period][2] else chart.mid[period][2]
							chart.mid[period][3] = new_close

						# Handle period bar info
						result.append({
							'broker': self.name,
							'product': chart.product,
							'period': period,
							'bar_end': False,
							'timestamp': c_ts,
							'item': {
								'ask': chart.ask[period].tolist(),
								'mid': chart.mid[period].tolist(),
								'bid': chart.bid[period].tolist()
							}
						})

					elif period == tl.period.TICK:
						if ask is not None:
							chart.ask[period] = ask
						if bid is not None:
							chart.bid[period] = bid
						if bid is not None and ask is not None:
							chart.mid[period] = np.around((ask + bid)/2, decimals=5)

						result.append({
							'broker': self.name,
							'product': chart.product,
							'period': period,
							'bar_end': False,
							'timestamp': c_ts,
							'item': {
								'ask': chart.ask[period],
								'mid': chart.mid[period],
								'bid': chart.bid[period]
							}
						})

			if len(result):
				chart.handleTick(result)


	def _subscribe_account_updates(self, account_id):
		# sub = Subscription(Subscription.ACCOUNT, self._on_account_update, account_id)
		# self._subscriptions.append(sub)
		# self._perform_account_connection(sub)

		print(f'SUBSCRIBE: {account_id}')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'oanda', self.brokerId, '_subscribe_account_updates', stream_id, account_id
		)
		self.ctrl.addBrokerListener(stream_id, self._on_account_update)


	def _perform_account_connection(self, sub):
		endpoint = f'/v3/accounts/{sub.args[0]}/transactions/stream'
		req = Request(f'{self._stream_url}{endpoint}', headers=self._headers)
		
		try:
			stream = urlopen(req, timeout=20)

			sub.setStream(stream)
			Thread(target=self._stream_account_update, args=(sub,)).start()
		except Exception as e:
			time.sleep(1)
			print('[Oanda] Attempting account reconnect.')
			Thread(target=self._perform_account_connection, args=(sub,)).start()
			return


	def _stream_account_update(self, sub):
		print(f'accounts connected. {self._is_connected}')
		if not self._is_connected:
			print('Send connected.')
			self._is_connected = True
			self._last_update = time.time()
			# Run connected callback
			# self.handleOnSessionStatus({
			# 	'timestamp': math.floor(time.time()),
			# 	'type': 'connected',
			# 	'message': 'The session connected successfully.'
			# })

		while sub.receive:
			try:
				message = sub.stream.readline().decode('utf-8').rstrip()
				if not message.strip():
					sub.receive = False
				else:
					sub.listener(sub.args[0], json.loads(message))

			except Exception as e:
				print(traceback.format_exc())
				sub.receive = False

		# Reconnect
		print('[Oanda] Account Updates Disconnected.')

		if self._is_connected:
			self._is_connected = False
			# Run disconnected callback
			# self.handleOnSessionStatus({
			# 	'timestamp': math.floor(time.time()),
			# 	'type': 'disconnected',
			# 	'message': 'The session has been disconnected.'
			# })

		self._perform_account_connection(sub)


	def _on_account_update(self, account_id, update):
		self._account_update_queue.append((account_id, update))


	def _handle_account_updates(self):

		while True:
			if len(self._account_update_queue):
				account_id, update = self._account_update_queue[0]
				del self._account_update_queue[0]

				print(f'UPDATE: {account_id}, {update}')

				res = {}
				if update.get('type') == 'HEARTBEAT':
					self._last_update = time.time()

				elif update.get('type') == 'connected':
					if not self.is_dummy and self.userAccount and self.brokerId:
						print(f'[_on_account_update] CONNECTED, Retrieving positions/orders')
						self._handle_live_strategy_setup()

						res.update({
							self.generateReference(): {
								'timestamp': time.time(),
								'type': 'update',
								'accepted': True,
								'item': {
									'positions': self.positions,
									'orders': self.orders
								}
							}
						})

				elif update.get('type') == 'ORDER_FILL':
					if self._handled.get(update.get('id')):
						res.update(self._wait(update.get('id')))

					else:
						res.update(self._handle_order_fill(account_id, update))

				elif update.get('type') == 'STOP_LOSS_ORDER':
					if self._handled.get(update.get('id')):
						res.update(self._wait(update.get('id')))

					else:
						res.update(self._handle_stop_loss_order(update))

				elif update.get('type') == 'TAKE_PROFIT_ORDER':
					if self._handled.get(update.get('id')):
						res.update(self._wait(update.get('id')))

					else:
						res.update(self._handle_take_profit_order(update))

				elif update.get('type') == 'LIMIT_ORDER' or update.get('type') == 'STOP_ORDER':
					if self._handled.get(update.get('id')):
						res.update(self._wait(update.get('id')))

					else:
						res.update(self._handle_order_create(update))

				elif update.get('type') == 'ORDER_CANCEL':
					if self._handled.get(update.get('id')):
						res.update(self._wait(update.get('id')))

					else:
						res.update(self._handle_order_cancel(update))

				if len(res):
					self.handleOnTrade(account_id, res)

			time.sleep(0.1)


	def isPeriodCompatible(self, period):
		return period in [
			tl.period.ONE_MINUTE, tl.period.TWO_MINUTES,
			tl.period.FOUR_MINUTES,
			tl.period.FIVE_MINUTES, tl.period.TEN_MINUTES,
			tl.period.FIFTEEN_MINUTES, tl.period.THIRTY_MINUTES, 
			tl.period.ONE_HOUR, tl.period.TWO_HOURS, 
			tl.period.THREE_HOURS, tl.period.FOUR_HOURS, 
			tl.period.TWELVE_HOURS, tl.period.DAILY, 
			tl.period.WEEKLY, tl.period.MONTHLY
		]
