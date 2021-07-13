import time
import math
import traceback
import numpy as np
import pandas as pd
import ntplib
import os
import sys
import base64
from datetime import datetime, timedelta
from copy import copy
from threading import Thread
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException


class Dukascopy(Broker):

	def __init__(self,
		ctrl, username=None, password=None, is_demo=None, user_account=None, strategy_id=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False, is_parent=False, complete=True
	):
		print(f'Dukascopy INIT (U) {username} (P) {password} (D) {is_demo}')

		super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.DUKASCOPY_NAME, accounts, display_name, is_dummy, False)

		self.username = username
		self.password = password
		self.isDemo = is_demo

		self.is_parent = is_parent

		self._gateway_loaded = False
		self._price_queue = []
		self._queue = []

		self._add_user()
		self._subscribe_account_updates()
		
		# self._subscribe_gui_updates()

		# elif not is_parent:
		# 	self.findUser()

		if self.username is not None:
			self.completeLogin(self.username, self.password, self.isDemo, "")

			if self.is_auth:
				# Handle strategy
				if self.is_parent:
					CHARTS = ['EUR_USD']
					for instrument in CHARTS:
						print(f'LOADING {instrument}')
						chart = self.createChart(instrument, await_completion=True)

					t = Thread(target=self._handle_chart_update)
					t.start()
				else:
					if self.userAccount and self.brokerId:
						self._handle_live_strategy_setup()



	def _add_user(self):
		print('Add User Dukascopy')

		if self.userAccount is not None:
			user_id = self.userAccount.userId
		else:
			user_id = None

		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'add_user',
			user_id, self.brokerId, 
			self.username, self.password, self.isDemo, 
			is_parent=self.is_parent
		)

		if 'error' in res:
			return self._add_user()
		else:
			# if res.get('_gateway_loaded'):
			# 	self._gateway_loaded = True
			return res


	def _set_time_off(self):
		try:
			client = ntplib.NTPClient()
			response = client.request('pool.ntp.org')
			self.time_off = response.tx_time - time.time()
		except Exception:
			pass


	def isLoggedIn(self):
		return


	def getLoginCaptchaBytes(self):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'get_login_captcha'
		)

		# print(f'[getLoginCaptcha] {res}')
		captcha_b64 = res['captcha']
		# with open('/app/img.png', 'wb') as f:
		# 	f.write(base64.b64decode(captcha_b64))

		return captcha_b64


	def completeLogin(self, username, password, is_demo, captcha_result):
		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'complete_login',
			username, password, is_demo, captcha_result
		)

		print(f'[completeLogin] {res}')

		if res.get('result'):
			self.username = username
			self.password = password
			self.isDemo = is_demo
			self.is_auth = True

			self.getAllAccounts()
			self._handle_live_strategy_setup()
			print(f'[completeLogin] {self.accounts}')

		return res.get('result')


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
			product, period, tz, dl_start, dl_end,
			count, force_download
		)

		if 'error' in res:
			result = pd.concat((
				self._create_empty_asks_df(), 
				self._create_empty_mids_df(), 
				self._create_empty_bids_df()
			))
		else:
			timestamps = np.array(res['timestamp']).reshape(-1,1)
			asks = np.array(res['asks'])
			bids = np.array(res['bids'])
			mids = np.around((asks + bids)/2, decimals=5)
			concat_arr = np.concatenate((timestamps, asks, mids, bids), axis=1)
			result = pd.DataFrame(
				data=concat_arr, columns=[
					'timestamp', 
					'ask_open', 'ask_high', 'ask_low', 'ask_close',
					'mid_open', 'mid_high', 'mid_low', 'mid_close',
					'bid_open', 'bid_high', 'bid_low', 'bid_close'
				]
			).set_index('timestamp')

		return result


	def _get_all_positions(self, account_id):
		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_get_all_positions',
			account_id
		)

		print(f'[_get_all_positions] {result}', flush=True)

		for account_id in result:
			for i in range(len(result[account_id])):
				result[account_id][i] = tl.Position.fromDict(self, result[account_id][i])

		return result


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
			sl_range, tp_range, sl_price, tp_price
		)

		print(f'[createPosition] {broker_result}')

		result = {}
		for ref_id in broker_result:
			update = broker_result[ref_id]
			if not ref_id == 'error':
				result.update(self._wait(
					ref_id, self.handleResponse, 
					{ ref_id: update }
				))
			else:
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MARKET_ORDER,
						'accepted': False,
						'message': update
					}
				})

		return result


	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyPosition',
			pos.order_id, sl_price, tp_price
		)

		print(f'[modifyPosition] {broker_result}')

		result = {}
		for ref_id in broker_result:
			update = broker_result[ref_id]
			if not ref_id == 'error':
				result.update(self._wait(
					ref_id, self.handleResponse, 
					{ ref_id: update }
				))
			else:
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MODIFY,
						'accepted': False,
						'message': update
					}
				})

		return result


	def deletePosition(self, pos, lotsize, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deletePosition',
			pos.order_id, lotsize
		)

		print(f'[deletePosition] {broker_result}')

		result = {}
		for ref_id in broker_result:
			update = broker_result[ref_id]
			if not ref_id == 'error':
				result.update(self._wait(
					ref_id, self.handleResponse, 
					{ ref_id: update }
				))
			else:
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.POSITION_CLOSE,
						'accepted': False,
						'message': update
					}
				})

		return result


	def _get_all_orders(self, account_id):
		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_get_all_orders'
		)

		print(f'[_get_all_orders] {result}', flush=True)
		for account_id in result:
			for i in range(len(result[account_id])):
				result[account_id][i] = tl.Order.fromDict(self, result[account_id][i])

		return result


	def getAllAccounts(self):
		account_res = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAllAccounts'
		)
		print(f'[getAllAccounts] {account_res}')

		if self.userAccount is not None:
			broker = self.userAccount.getBroker(self.brokerId)
			accounts = {}
			for acc in account_res:
				if acc in broker['accounts']:
					accounts[acc] = broker['accounts'][acc]
				else:
					accounts[acc] = {
						"active": True,
						"nickname": ""
					}

				if acc in self.accounts:
					self.accounts[acc].update(account_res[acc])
					self.accounts[acc].update(accounts[acc])
				else:
					self.accounts[acc] = account_res[acc]
					self.accounts[acc].update(accounts[acc])

			update = {
				"username": self.username,
				"password": self.password,
				"is_demo": self.isDemo,
				"accounts": accounts
			}

			if self.is_auth:
				update['complete'] = True

			self.userAccount.updateBroker(self.brokerId, update)

		print(f'[getAllAccounts] {self.accounts}')


	def getAccountInfo(self, account_id):
		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAccountInfo'
		)

		return result


	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price, override=False
	):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createOrder',
			product, lotsize, direction,
			order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		print(f'[createOrder] {broker_result}')

		result = {}
		for ref_id in broker_result:
			update = broker_result[ref_id]
			if not ref_id == 'error':
				result.update(self._wait(
					ref_id, self.handleResponse, 
					{ ref_id: update }
				))
			else:
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': order_type,
						'accepted': False,
						'message': update
					}
				})

		return result


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyOrder',
			order.order_id, lotsize, entry_price, sl_price, tp_price
		)

		print(f'[modifyOrder] {broker_result}')

		result = {}
		for ref_id in broker_result:
			update = broker_result[ref_id]
			if not ref_id == 'error':
				result.update(self._wait(
					ref_id, self.handleResponse, 
					{ ref_id: update }
				))
			else:
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MODIFY,
						'accepted': False,
						'message': update
					}
				})

		return result


	def deleteOrder(self, order, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deleteOrder',
			order.order_id
		)

		print(f'[deleteOrder] {broker_result}')
		result = {}
		for ref_id in broker_result:
			update = broker_result[ref_id]
			if not ref_id == 'error':
				result.update(self._wait(
					ref_id, self.handleResponse, 
					{ ref_id: update }
				))
			else:
				result.update({
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.ORDER_CANCEL,
						'accepted': False,
						'message': update
					}
				})

		return result


	def _handle_order_fill(self, ref_id, update):
		result = {}
		for i in range(len(self.orders)):
			order = self.orders[i]
			if order.order_id == update['item']['order_id']:
				del self.orders[i]

				result.update({
					self.generateReference(): {
						'timestamp': update['timestamp'],
						'type': tl.ORDER_CANCEL,
						'accepted': True,
						'item': order
					}
				})
				break

		if 'order' in update['item']:
			del update['item']['order']
		if 'handled_check' in update['item']:
			del update['item']['handled_check']

		pos = tl.Position.fromDict(self, update['item'])

		if not pos.order_id in [i.order_id for i in self.positions]:
			if pos.close_price == 0:
				pos.close_price = None
			if pos.close_time == 0:
				pos.close_time = None


			self.positions.append(pos)

			result.update({
				ref_id: {
					'timestamp': update['timestamp'],
					'type': pos.order_type,
					'accepted': True,
					'item': pos
				}
			})

		if len(result):
			self._handled[ref_id] = result
		
		print(f'[_handle_order_fill] {result}', flush=True)

		return result


	def _handle_position_modify(self, ref_id, update):
		pos = self.getPositionByID(update['item']['order_id'])

		if pos is not None:
			pos.lotsize = update['item']['lotsize']
			pos.entry_price = update['item']['entry_price']
			pos.sl = update['item']['sl']
			pos.sl_id = update['item']['sl_id']
			pos.tp = update['item']['tp']
			pos.tp_id = update['item']['tp_id']

			result = {
				ref_id: {
					'timestamp': update['timestamp'],
					'type': tl.MODIFY,
					'accepted': True,
					'item': pos
				}
			}

			self._handled[ref_id] = result

			return result

		else:
			return {}


	def _handle_position_close(self, ref_id, update):

		for i in range(len(self.positions)):
			# TODO: Partial closing

			pos = self.positions[i]
			if pos.order_id == update['item']['order_id']:

				# Full Close
				if pos.lotsize == update['item']['lotsize']:
					print(f'[_handle_order_fill] FULL', flush=True)
					del self.positions[i]

					pos.close_price = update['item']['close_price']
					pos.close_time = update['item']['close_time']

					result = {
						ref_id: {
							'timestamp': update['timestamp'],
							'type': tl.POSITION_CLOSE,
							'accepted': True,
							'item': pos
						}
					}

				# Partial Close
				else:
					print(f'[_handle_order_fill] PARTIAL', flush=True)
					cpy = tl.Position.fromDict(self, update['item'])

					cpy.close_price = update['item']['close_price']
					cpy.close_time = update['item']['close_time']
					cpy.lotsize = round(pos.lotsize - update['item']['lotsize'], 2)

					pos.lotsize = update['item']['lotsize']

					result = {
						ref_id: {
							'timestamp': update['timestamp'],
							'type': tl.POSITION_CLOSE,
							'accepted': True,
							'item': cpy
						}
					}

				self._handled[ref_id] = result

				print(f'[_handle_order_fill] {result}', flush=True)

				return result
	
		return {}


	def _handle_order_create(self, ref_id, update):
		order = tl.Order.fromDict(self, update['item'])

		print(f'[_handle_order_create] {order}', flush=True)

		if not order.order_id in [i.order_id for i in self.orders]:
			if order.close_price == 0:
				order.close_price = None
			if order.close_time == 0:
				order.close_time = None

			self.orders.append(order)

			result = {
				ref_id: {
					'timestamp': update['timestamp'],
					'type': order.order_type,
					'accepted': True,
					'item': order
				}
			}

			self._handled[ref_id] = result

			return result

		else:
			return {}


	def _handle_order_modify(self, ref_id, update):
		order = self.getOrderByID(update['item']['order_id'])

		if order is not None:
			order.lotsize = update['item']['lotsize']
			order.entry_price = update['item']['entry_price']
			order.sl = update['item']['sl']
			order.sl_id = update['item']['sl_id']
			order.tp = update['item']['tp']
			order.tp_id = update['item']['tp_id']

			result = {
				ref_id: {
					'timestamp': update['timestamp'],
					'type': tl.MODIFY,
					'accepted': True,
					'item': order
				}
			}

			self._handled[ref_id] = result

		return result


	def _handle_order_cancel(self, ref_id, update):
		for i in range(len(self.orders)):
			order = self.orders[i]
			if order.order_id == update['item']['order_id']:
				del self.orders[i]

				order.close_price = update['item']['close_price']
				order.close_time = update['item']['close_time']

				result = {
					ref_id: {
						'timestamp': update['timestamp'],
						'type': tl.ORDER_CANCEL,
						'accepted': True,
						'item': order
					}
				}

				self._handled[ref_id] = result

				return result
	
		return {}


	def _handle_rejected(self, ref_id, update):
		result = {
			ref_id: {
				'timestamp': update['timestamp'],
				'accepted': update['accepted'],
				'type': update['type'],
				'item': update['item']
			}
		}

		self._handled[ref_id] = result

		return result


	def handleResponse(self, res):
		result = {}
		for ref_id in res:
			update = res[ref_id]
			if update.get('type') == 'connected':
				print('[handleResponse] CONNECTED')
				if not self.is_parent and self.userAccount and self.brokerId:
					print('[handleResponse] Re-Handling Setup')
					self._handle_live_strategy_setup()

					result = {
						self.generateReference(): {
							'timestamp': time.time(),
							'type': 'update',
							'accepted': True,
							'item': {
								'positions': self.positions,
								'orders': self.orders
							}
						}
					}
					account_id = list(self.accounts.keys())[0]
					self.handleOnTrade(account_id, result)

			elif update.get('type') == 'disconnected':
				print('[handleResponse] DISCONNECTED')

			elif update.get('accepted'):
				func = self.getTradeHandler(update['type'])
				if func is not None:
					result.update(func(ref_id, update))
			else:
				result.update(self._handle_rejected(ref_id, update))

		return result


	def getTradeHandler(self, order_type):
		if order_type in (tl.STOP_ORDER, tl.LIMIT_ORDER):
			return self._handle_order_create
		elif order_type in (tl.MARKET_ENTRY, tl.STOP_ENTRY, tl.LIMIT_ENTRY):
			return self._handle_order_fill
		elif order_type == tl.ORDER_MODIFY:
			return self._handle_order_modify
		elif order_type == tl.POSITION_MODIFY:
			return self._handle_position_modify
		elif order_type in (tl.POSITION_CLOSE, tl.STOP_LOSS, tl.TAKE_PROFIT):
			return self._handle_position_close
		elif order_type == tl.ORDER_CANCEL:
			return self._handle_order_cancel


	def _subscribe_chart_updates(self, instrument, listener):
		print(f'[_subscribe_chart_updates]')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'dukascopy', self.brokerId, '_subscribe_chart_updates', stream_id, instrument
		)
		self.ctrl.addBrokerListener(stream_id, listener)


	def _handle_chart_update(self):
		time_off_timer = time.time()

		while True:
			result = []
			if len(self._price_queue):
				chart, update = self._price_queue[0]
				del self._price_queue[0]

				timestamp = update.get('timestamp')
				ask = update.get('ask')
				bid = update.get('bid')
				bar_end = update.get('bar_end')

				for period in chart.getActivePeriods():
					if (isinstance(chart.bid.get(period), np.ndarray) and 
						isinstance(chart.ask.get(period), np.ndarray)):

						# Handle period bar end
						if period != tl.period.TICK:
							is_new_bar = chart.isNewBar(period, timestamp)
							if is_new_bar:
								if chart.volume[period] > 0:
									chart.volume[period] = 0
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

								chart.lastTs[period] = tl.getNextTimestamp(period, chart.lastTs[period], now=timestamp - tl.period.getPeriodOffsetSeconds(period))
								print(f'[Dukascopy] ({period}) Prev: {chart.lastTs[period]}, Next: {chart.lastTs[period]}')
								chart.ask[period] = np.array([chart.ask[period][3]]*4, dtype=np.float64)
								chart.bid[period] = np.array([chart.bid[period][3]]*4, dtype=np.float64)
								chart.mid[period] = np.array(
									[np.around(
										(chart.ask[period][3] + chart.bid[period][3])/2,
										decimals=5
									)]*4, 
								dtype=np.float64)

						chart.volume[period] += 1

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
							'timestamp': max(timestamp, chart.lastTs[period]),
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
							'timestamp': timestamp,
							'item': {
								'ask': chart.ask[period],
								'mid': chart.mid[period],
								'bid': chart.bid[period]
							}
						})

				if len(result):
					chart.handleTick(result)

			# else:
			# 	for chart in self.charts:
			# 		timestamp = time.time()+self.time_off-1
			# 		for period in chart.getActivePeriods():
			# 			if period != tl.period.TICK and chart.volume[period] > 0:
			# 				# Handle period bar end
			# 				is_new_bar = chart.isNewBar(period, timestamp)
			# 				if is_new_bar:
			# 					print(f'ADD NEW BAR 2: {period}', flush=True)
			# 					chart.volume[period] = 0
			# 					result.append({
			# 						'broker': self.name,
			# 						'product': chart.product,
			# 						'period': period,
			# 						'bar_end': True,
			# 						'timestamp': chart.lastTs[period],
			# 						'item': {
			# 							'ask': chart.ask[period].tolist(),
			# 							'mid': chart.mid[period].tolist(),
			# 							'bid': chart.bid[period].tolist()
			# 						}
			# 					})
			# 					chart.lastTs[period] = tl.getNextTimestamp(period, chart.lastTs[period], now=timestamp - tl.period.getPeriodOffsetSeconds(period))
			# 					print(f'[FXCM] ({period}) Prev: {chart.lastTs[period]}, Next: {chart.lastTs[period]}')
			# 					chart.ask[period] = np.array([chart.ask[period][3]]*4, dtype=np.float64)
			# 					chart.bid[period] = np.array([chart.bid[period][3]]*4, dtype=np.float64)
			# 					chart.mid[period] = np.array(
			# 						[np.around(
			# 							(chart.ask[period][3] + chart.bid[period][3])/2,
			# 							decimals=5
			# 						)]*4, 
			# 					dtype=np.float64)

			# 		if len(result):
			# 			chart.handleTick(result)

			# if time.time() - time_off_timer > ONE_HOUR:
			# 	time_off_timer = time.time()
			# 	self._set_time_off()

			time.sleep(0.01)




	def onChartUpdate(self, *args, **kwargs):
		self._price_queue.append(args)




	def _subscribe_account_updates(self):
		if not self.is_parent:
			print(f'[_subscribe_account_updates]')
			stream_id = self.generateReference()
			res = self.ctrl.brokerRequest(
				'dukascopy', self.brokerId, '_subscribe_account_updates', stream_id
			)
			self.ctrl.addBrokerListener(stream_id, self._on_account_update)


	def _on_account_update(self, *args, **kwargs):
		update = args[0]
		print(f'[_on_account_update] {update}')

		result = self.handleResponse(update)

		print(f'[_on_account_update] RESULT: {result}')

		if len(result):
			account_id = list(self.accounts.keys())[0]
			self.handleOnTrade(account_id, result)


	def _subscribe_gui_updates(self):
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'ib', self.brokerId, '_subscribe_gui_updates', stream_id
		)
		self.ctrl.addBrokerListener(stream_id, self.onGuiUpdate)


	def onGuiUpdate(self, *args, **kwargs):
		print(f'[onGuiUpdate] {args}')

		message = args[0]
		if message == 'gateway_loaded':
			self._gateway_loaded = True


		self.handleOnGui(None, message)
