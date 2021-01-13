import time
import traceback
import numpy as np
import pandas as pd
from datetime import datetime
from copy import copy
from threading import Thread
from . import spotware_connect as sc
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException

CLIENT_ID = '2096_sEzU1jyvCjvNMo2ViU8YnZha8UQmuHokkaXJDVD7fVEoIc1wx3'
CLIENT_SECRET = '0Tl8PVbt9rek4rRelAkGx9BoYRUhbhDYTp9sQjOAMdcmo0XQ6W'

class Spotware(Broker):

	def __init__(self,
		ctrl, is_demo, access_token=None,
		user_account=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False, is_parent=False
	):
		if not is_parent:
			super().__init__(ctrl, user_account, broker_id, tl.broker.SPOTWARE_NAME, accounts, display_name)

		self.is_demo = is_demo
		self._spotware_connected = False
		self._last_update = time.time()
		self._subscriptions = {}

		self.access_token = access_token

		'''
		Setup Spotware Funcs
		'''

		if is_parent:
			self.parent = self
			self.children = []

			self.client = sc.Client()

			self.client.event(self.connect)
			self.client.event(self.disconnect)
			self.client.event(self.message)
			self.client.event('message', self.on_auth_ok, msgtype='ApplicationAuthRes')
			self.client.event('message', self.on_account_auth_ok, msgtype='AccountAuthRes')

			Thread(target=self._run).start()
			
			while not self._spotware_connected:
				time.sleep(1)

			super().__init__(ctrl, user_account, broker_id, tl.broker.SPOTWARE_NAME, accounts, display_name)

		else:
			self.parent = ctrl.brokers.getBroker(tl.broker.SPOTWARE_NAME)
			self.parent.addChild(self)
			self.client = self.parent.client
			self._authorize_accounts(accounts)

			# self._subscribe_chart_updates(2, self.onChartUpdate)

		if not is_dummy:
			# for account_id in self.getAccounts():
			# 	if account_id != tl.broker.PAPERTRADER_NAME:
			# 		self._subscribe_account_updates(account_id)

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

		# Start refresh thread
		Thread(target=self._periodic_refresh).start()

	'''
	Spotware messages
	'''

	def _periodic_refresh(self):
		TEN_SECONDS = 10
		while self.is_running:
			if time.time() - self._last_update > TEN_SECONDS:
				self.client.emit('HeartbeatEvent')
				self._last_update = time.time()

			time.sleep(1)


	def _wait(self, ref_id, polling=0.1, timeout=30):
		start = time.time()
		while not ref_id in self._handled:
			if time.time() - start >= timeout:
				return None

		item = self._handled[ref_id]
		del self._handled[ref_id]
		return item


	def _wait_for_position(self, order_id, polling=0.1, timeout=30):
		start = time.time()
		while time.time() - start < 30:
			for pos in self.positions:
				if pos.order_id == order_id:
					return pos
			time.sleep(polling)

		return None


	def _wait_for_close(self, pos, polling=0.1, timeout=30):
		start = time.time()
		while time.time() - start < 30:
			if not pos.close_price is None:
				return pos
			time.sleep(polling)

		return None
			

	def _run(self):
		self.client.start()		


	def connect(self):
		print('Spotware connected!')

		# Application Auth
		self.client.emit(
			'ApplicationAuthReq', clientId=CLIENT_ID, clientSecret=CLIENT_SECRET
		)


	def disconnect(self):
		print('Spotware disconnected')


	def message(self, msg, payload, **kwargs):
		# print(f'MSG: {payload.payloadType}')

		# Heartbeat
		if payload.payloadType == 51:
			self.client.emit('HeartbeatEvent')
			self._last_update = time.time()

		# Tick
		elif payload.payloadType == 2131:
			if str(payload.symbolId) in self._subscriptions:
				self._subscriptions[str(payload.symbolId)](payload)
		else:
			result = None
			if 'ctidTraderAccountId' in payload.DESCRIPTOR.fields_by_name.keys():
				account_id = payload.ctidTraderAccountId
				for child in self.children:
					if account_id in map(int, child.accounts.keys()):
						result = child._on_account_update(account_id, payload, kwargs.get('msgid'))
						break

			if kwargs.get('msgid'):
				if result is None:
					self._handled[kwargs['msgid']] = payload
				else:
					self._handled[kwargs['msgid']] = result



	def on_auth_ok(self, **kwargs):
		# Account Auth
		# self.client.emit(
		# 	'AccountAuthReq', 
		# 	ctidTraderAccountId=19891017, 
		# 	accessToken='UWU1cwMhRWbUhQ5gFZNImArEhMI7oPKJeV3dIizOFYY'
		# )
		print("[SW] Authorized")

		# Re-Authorize accounts
		for child in self.children:
			child._authorize_accounts(child.accounts)

		self._spotware_connected = True


	def on_account_auth_ok(self, **kwargs):
		return


	def _authorize_accounts(self, accounts):
		for account_id in accounts:
			ref_id = self.generateReference()
			self.client.emit(
				'AccountAuthReq', 
				msgid=ref_id, ctidTraderAccountId=int(account_id), 
				accessToken='UWU1cwMhRWbUhQ5gFZNImArEhMI7oPKJeV3dIizOFYY'
			)
			self.parent._wait(ref_id)


	'''
	Broker functions
	'''

	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		sw_product = self._convert_product(product)
		sw_period = self._convert_period(period)

		result = {}

		dl_start = None
		dl_end = None
		if start:
			dl_start = tl.utils.convertTimeToTimestamp(start)
		if end:
			dl_end = tl.utils.convertTimeToTimestamp(end)

		if count:
			if start:
				dl_end = tl.utils.getCountDate(period, count+1, start=start).timestamp()
			elif end:
				dl_start = tl.utils.getCountDate(period, count+1, end=end).timestamp()
			else:
				dl_start = tl.utils.getCountDate(period, count+1).timestamp()
				dl_end = datetime.utcnow().timestamp()


		ref_id = self.generateReference()
		self.client.emit(
			'GetTrendbarsReq',
			msgid=ref_id, ctidTraderAccountId=int(list(self.accounts.keys())[0]),
			fromTimestamp=int(dl_start*1000), toTimestamp=int(dl_end*1000), 
			symbolId=sw_product, period=sw_period
		)

		res = self._wait(ref_id)

		'''
		Bar Constructor
		'''

		if res.payloadType == 2138:
			mids = self._bar_data_constructor(res, self._create_empty_mids_df())
			asks = mids.copy()
			asks.columns = ['ask_open', 'ask_high', 'ask_low', 'ask_close']
			bids = mids.copy()
			bids.columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close']
			result = pd.concat((asks, mids, bids), axis=1)
		else:
			result = pd.concat((
				self._create_empty_asks_df(), 
				self._create_empty_mids_df(), 
				self._create_empty_bids_df()
			))

		'''
		Tick Constructor
		'''

		# asks_id = self.generateReference()
		# self.client.emit(
		# 	'GetTickDataReq',
		# 	msgid=asks_id, ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 	type=2, fromTimestamp=int(dl_start*1000), toTimestamp=int(dl_end*1000), 
		# 	symbolId=sw_product
		# )

		# bids_id = self.generateReference()
		# self.client.emit(
		# 	'GetTickDataReq',
		# 	msgid=bids_id, ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 	type=1, fromTimestamp=int(dl_start*1000), toTimestamp=int(dl_end*1000), 
		# 	symbolId=sw_product
		# )

		# asks = self._wait(asks_id)
		# bids = self._wait(bids_id)

		# # Asks
		# asks_df = self._tick_data_constructor(period, asks, self._create_empty_asks_df())

		# # Bids
		# bids_df = self._tick_data_constructor(period, bids, self._create_empty_bids_df())

		# # Intersect
		# asks_df_intersect = asks_df.loc[asks_df.index.intersection(bids_df.index)]
		# bids_df_intersect = bids_df.loc[bids_df.index.intersection(asks_df.index)]

		# # Mids
		# mids_df = self._create_empty_mids_df()
		# for i in range(asks_df_intersect.index.size):
		# 	idx = asks_df_intersect.index[i]
		# 	mids_df.loc[idx] = np.around(
		# 		((asks_df_intersect.loc[idx].values + bids_df_intersect.loc[idx].values)/2).tolist(), 
		# 		decimals=5
		# 	)

		# result = pd.concat((
		# 	asks_df_intersect, mids_df, bids_df_intersect
		# ), axis=1)

		return result


	def convert_sw_position(self, account_id, pos):
		order_id = pos.positionId
		product = self._convert_sw_product(pos.tradeData.symbolId)
		direction = tl.LONG if pos.tradeData.tradeSide == 1 else tl.SHORT
		lotsize = pos.tradeData.volume
		entry_price = pos.price
		sl = None if pos.stopLoss == 0 else pos.stopLoss
		tp = None if pos.takeProfit == 0 else pos.takeProfit
		open_time = pos.tradeData.openTimestamp / 1000

		return tl.Position(
			self,
			order_id, account_id, product,
			tl.MARKET_ENTRY, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def convert_sw_order(self, account_id, order):
		entry_price = None
		if order.orderType == 3:
			entry_price = order.stopPrice
			order_type = tl.STOP_ORDER
		elif order.orderType == 2:
			entry_price = order.limitPrice
			order_type = tl.LIMIT_ORDER

		order_id = order.orderId
		product = self._convert_sw_product(order.tradeData.symbolId)
		direction = tl.LONG if order.tradeData.tradeSide == 1 else tl.SHORT
		lotsize = order.tradeData.volume
		sl = None if order.stopLoss == 0 else order.stopLoss
		tp = None if order.takeProfit == 0 else order.takeProfit
		open_time = order.tradeData.openTimestamp / 1000

		return tl.Order(
			self,
			order_id, account_id, product,
			order_type, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def _get_all_positions(self, account_id):
		ref_id = self.generateReference()
		self.client.emit(
			'ReconcileReq',
			msgid=ref_id, ctidTraderAccountId=int(account_id)
		)

		res = self.parent._wait(ref_id)

		result = { account_id: [] }
		if res.payloadType == 2125:
			for pos in res.position:
				new_pos = self.convert_sw_position(account_id, pos)

				result[account_id].append(new_pos)

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
		
		if (status != 200 or account_id == tl.broker.PAPERTRADER_NAME):
			return super().createPosition(
				product, lotsize, direction,
				account_id, entry_range, entry_price,
				sl_range, tp_range, sl_price, tp_price,
				override=override
			)

		ref_id = self.generateReference()

		sl_tp_prices = {}
		sl_tp_ranges = {}
		if sl_price:
			sl_tp_prices['stopLoss'] = sl_price

			# Get range from current price for temp sl
			chart = self.getChart(product)
			if direction == tl.LONG:
				sl_tp_ranges['relativeStopLoss'] = int((chart.ask[tl.period.ONE_MINUTE][3] - sl_price) * 100000)
			else:
				sl_tp_ranges['relativeStopLoss'] = int((sl_price - chart.bid[tl.period.ONE_MINUTE][3]) * 100000)

		if sl_range:
			sl_tp_ranges['relativeStopLoss'] = int(sl_range)

		if tp_price:
			sl_tp_prices['takeProfit'] = tp_price
			# Get range from current price for temp tp
			chart = self.getChart(product)
			if direction == tl.LONG:
				sl_tp_ranges['relativeTakeProfit'] = int((tp_price - chart.ask[tl.period.ONE_MINUTE][3]) * 100000)
			else:
				sl_tp_ranges['relativeTakeProfit'] = int((chart.bid[tl.period.ONE_MINUTE][3] - tp_price) * 100000)

		if tp_range:
			sl_tp_ranges['relativeTakeProfit'] = int(tp_range)
		
		sw_product = self._convert_product(product)
		direction = 1 if direction == tl.LONG else 2

		# Execute Market Order
		self.client.emit(
			'NewOrderReq',
			msgid=ref_id, ctidTraderAccountId=int(account_id),
			symbolId=sw_product, orderType=1, tradeSide=direction,
			volume=lotsize, **sl_tp_ranges
		)

		res = self.parent._wait(ref_id)

		result = {}
		if res.payloadType == 2126:
		# 	new_pos = self.convert_sw_position(account_id, res.position)
			pos = self._wait_for_position(res.position.positionId)

			if pos is not None:
				if len(sl_tp_prices) > 0:
					ref_id = self.generateReference()

					self.client.emit(
						'AmendPositionSLTPReq',
						msgid=ref_id, ctidTraderAccountId=int(pos.account_id),
						positionId=int(pos.order_id), **sl_tp_prices
					)

					res = self.parent._wait(ref_id)

				result.update({
					ref_id: {
						'timestamp': pos.open_time,
						'type': tl.MARKET_ENTRY,
						'accepted': True,
						'item': pos
					}
				})

		elif not res is None and res.payloadType == 50:
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MARKET_ENTRY,
					'accepted': False,
					'message': res.errorCode
				}
			})

		else:
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MARKET_ENTRY,
					'accepted': False
				}
			})

		return result


	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyPosition(
				pos, sl_price, tp_price, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		ref_id = self.generateReference()

		self.client.emit(
			'AmendPositionSLTPReq',
			msgid=ref_id, ctidTraderAccountId=int(pos.account_id),
			positionId=int(pos.order_id), stopLoss=sl_price, takeProfit=tp_price
		)

		res = self.parent._wait(ref_id)

		if not isinstance(res, dict):
			if not res is None and res.payloadType == 50:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.MODIFY,
						'accepted': False,
						'message': res.errorCode
					}
				}
			else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.MODIFY,
						'accepted': False
					}
				}

		return res


	def deletePosition(self, pos, lotsize, override=False):
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deletePosition(
				pos, lotsize, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		ref_id = self.generateReference()

		self.client.emit(
			'ClosePositionReq',
			msgid=ref_id, ctidTraderAccountId=int(pos.account_id),
			positionId=int(pos.order_id), volume=lotsize
		)

		res = self.parent._wait(ref_id)

		# Handle delete result
		result = {}
		if res.payloadType == 2126:
			self._wait_for_close(pos)

			result.update({
				ref_id: {
					'timestamp': pos.close_time,
					'type': tl.POSITION_CLOSE,
					'accepted': True,
					'item': pos
				}
			})

		elif not res is None and res.payloadType == 50:
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': res.errorCode
				}
			})

		else:
			result.update({
				ref_id: {
					'timestamp': time.time(),
					'type': tl.POSITION_CLOSE,
					'accepted': False
				}
			})

		return result


	def _get_all_orders(self, account_id):
		ref_id = self.generateReference()
		self.client.emit(
			'ReconcileReq',
			msgid=ref_id, ctidTraderAccountId=int(account_id)
		)

		res = self.parent._wait(ref_id)

		result = { account_id: [] }
		if res.payloadType == 2125:
			for order in res.order:
				new_order = self.convert_sw_order(account_id, order)

				result[account_id].append(new_order)

		return result


	def getAllAccounts(self):
		ref_id = self.generateReference()
		self.client.emit(
			'GetAccountListByAccessTokenReq',
			msgid=ref_id, accessToken=self.access_token
		)

		res = self.parent._wait(ref_id)
		if res is not None:
			result = []
			for i in res.ctidTraderAccount:
				if i.isLive != self.is_demo and res.permissionScope == 1:
					result.append(i.ctidTraderAccountId)

			return result

		else:
			return None


	def getAccountInfo(self, account_id, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)

		ref_id = self.generateReference()

		self.client.emit(
			'TraderReq',
			msgid=ref_id, ctidTraderAccountId=int(account_id)
		)

		res = self.parent._wait(ref_id)

		# Handle account info result

		result = {}
		if res.payloadType == 2122:
			result[account_id] = {
				'currency': 'USD',
				'balance': res.trader.balance,
				'pl': None,
				'margin': None,
				'available': None
			}
		
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
		
		if (status != 200 or account_id == tl.broker.PAPERTRADER_NAME):
			return super().createOrder(
				product, lotsize, direction,
				account_id, order_type, entry_range, entry_price,
				sl_range, tp_range, sl_price, tp_price,
				override=override
			)

		ref_id = self.generateReference()

		# Convert symbol
		# symbol_id = 

		params = {}
		if order_type == tl.STOP_ORDER:
			params['stopPrice'] = entry_price
		elif order_type == tl.LIMIT_ORDER:
			params['limitPrice'] = entry_price

		if sl_price:
			params['stopLoss'] = sl_price
		else:
			params['relativeStopLoss'] = sl_range

		if tp_price:
			params['takeProfit'] = tp_price
		else:
			params['relativeTakeProfit'] = tp_range

		direction = 1 if direction == tl.LONG else 2
		sw_order_type = 3 if order_type == tl.STOP_ORDER else 2

		self.client.emit(
			'NewOrderReq',
			msgid=ref_id, ctidTraderAccountId=int(account_id),
			symbolId=2, orderType=sw_order_type, tradeSide=direction,
			volume=lotsize, **params
		)

		res = self.parent._wait(ref_id)

		if not isinstance(res, dict):
			if not res is None and res.payloadType == 50:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': order_type,
						'accepted': False,
						'message': res.errorCode
					}
				}
			else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': order_type,
						'accepted': False
					}
				}

		return res


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyOrder(
				order, lotsize, entry_price, sl_price, tp_price, override=override
			)

		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		ref_id = self.generateReference()

		entry_args = {}
		if order.order_type == tl.STOP_ORDER:
			entry_args['stopPrice'] = entry_price
		elif order.order_type == tl.LIMIT_ORDER:
			entry_args['limitPrice'] = entry_price

		self.client.emit(
			'AmendOrderReq',
			msgid=ref_id, ctidTraderAccountId=int(order.account_id), orderId=int(order.order_id),
			volume=lotsize, stopLoss=sl_price, takeProfit=tp_price, **entry_args
		)

		res = self.parent._wait(ref_id)

		if not isinstance(res, dict):
			if not res is None and res.payloadType == 50:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.MODIFY,
						'accepted': False,
						'message': res.errorCode
					}
				}
			else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.MODIFY,
						'accepted': False
					}
				}

		return res


	def deleteOrder(self, order, override=False):
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deleteOrder(order, override=override)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		ref_id = self.generateReference()

		self.client.emit(
			'CancelOrderReq',
			msgid=ref_id, ctidTraderAccountId=int(order.account_id),
			orderId=int(order.order_id)
		)

		res = self.parent._wait(ref_id)

		if not isinstance(res, dict):
			if not res is None and res.payloadType == 50:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.ORDER_CANCEL,
						'accepted': False,
						'message': res.errorCode
					}
				}
			else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.ORDER_CANCEL,
						'accepted': False
					}
				}

		return res


	def _on_account_update(self, account_id, update, ref_id):
		if update.payloadType == 2126:
			if not ref_id:
				ref_id = self.generateReference()

			print(f'Account Update: {update}')
			execution_type = update.executionType

			result = {}
			# ORDER_FILLED
			if execution_type == 3:
				# Check `closingOrder`
				if update.order.closingOrder:
					# Delete
					for i in range(len(self.positions)):
						pos = self.positions[i]
						if update.position.positionId == pos.order_id:
							# Fully Closed
							if update.position.tradeData.volume == 0:
								pos.close_price = update.order.executionPrice
								pos.close_time = update.order.utcLastUpdateTimestamp / 1000

								del self.positions[i]

								result.update({
									ref_id: {
										'timestamp': pos.close_time,
										'type': tl.POSITION_CLOSE,
										'accepted': True,
										'item': pos
									}
								})

							# Partially Closed
							else:
								pos.lotsize -= update.order.executedVolume

								del_pos = tl.Position.fromDict(self, pos)
								del_pos.lotsize = update.order.executedVolume
								del_pos.close_price = update.order.executionPrice
								del_pos.close_time = update.order.utcLastUpdateTimestamp / 1000

								result.update({
									ref_id: {
										'timestamp': del_pos.close_time,
										'type': tl.POSITION_CLOSE,
										'accepted': True,
										'item': del_pos
									}
								})

							break
				else:
					# Create
					new_pos = self.convert_sw_position(account_id, update.position)
					self.positions.append(new_pos)

					result.update({
						ref_id: {
							'timestamp': new_pos.open_time,
							'type': tl.POSITION_CLOSE,
							'accepted': True,
							'item': new_pos
						}
					})

			# ORDER_ACCEPTED
			elif execution_type == 2:
				# Check if `STOP` or `LIMIT`
				if update.order.orderType in (2,3):
					new_order = self.convert_sw_order(account_id, update.order)
					self.orders.append(new_order)

					result.update({
						ref_id: {
							'timestamp': update.order.utcLastUpdateTimestamp/1000,
							'type': new_order.order_type,
							'accepted': True,
							'item': new_order
						}
					})

				# Check if `STOP_LOSS_TAKE_PROFIT`
				elif update.order.orderType == 4:
					for pos in self.positions:
						if update.position.positionId == pos.order_id:
							new_sl = None if update.position.stopLoss == 0 else update.position.stopLoss
							pos.sl = new_sl
							new_tp = None if update.position.takeProfit == 0 else update.position.takeProfit
							pos.tp = new_tp

							result.update({
								ref_id: {
									'timestamp': update.order.utcLastUpdateTimestamp/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': pos
								}
							})

							break

			# ORDER_CANCELLED
			elif execution_type == 5:
				# Check if `STOP` or `LIMIT`
				if update.order.orderType in (2,3):
					# Update current order
					new_order = self.convert_sw_order(account_id, update.order)
					for i in range(len(self.orders)):
						order = self.orders[i]
						if update.order.orderId == order.order_id:
							order.close_time = update.order.utcLastUpdateTimestamp / 1000

							del self.orders[i]

							result.update({
								ref_id: {
									'timestamp': order.close_time,
									'type': tl.ORDER_CANCEL,
									'accepted': True,
									'item': order
								}
							})

							break

				# Check if `STOP_LOSS_TAKE_PROFIT`
				elif update.order.orderType == 4:
					for pos in self.positions:
						if update.position.positionId == pos.order_id:
							new_sl = None if update.position.stopLoss == 0 else update.position.stopLoss
							pos.sl = new_sl
							new_tp = None if update.position.takeProfit == 0 else update.position.takeProfit
							pos.tp = new_tp

							result.update({
								ref_id: {
									'timestamp': update.order.utcLastUpdateTimestamp/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': pos
								}
							})

							break

			# ORDER_REPLACED
			elif execution_type == 4:
				# Check if `STOP` or `LIMIT`
				if update.order.orderType in (2,3):
					# Update current order
					new_order = self.convert_sw_order(account_id, update.order)
					for order in self.orders:
						if update.order.orderId == order.order_id:
							order.update(new_order)

							result.update({
								ref_id: {
									'timestamp': update.order.utcLastUpdateTimestamp/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': order
								}
							})

				# Check if `STOP_LOSS_TAKE_PROFIT`
				elif update.order.orderType == 4:
					# Update current position
					for pos in self.positions:
						if update.position.positionId == pos.order_id:
							new_sl = None if update.position.stopLoss == 0 else update.position.stopLoss
							pos.sl = new_sl
							new_tp = None if update.position.takeProfit == 0 else update.position.takeProfit
							pos.tp = new_tp

							result.update({
								ref_id: {
									'timestamp': update.order.utcLastUpdateTimestamp/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': pos
								}
							})

			if len(result):
				self.handleOnTrade(result)
				return result
			else:
				return None


	def _subscribe_chart_updates(self, product, listener):
		ref_id = self.generateReference()

		product = self._convert_product(product)
		self.parent._subscriptions[str(product)] = listener

		self.client.emit(
			'SubscribeSpotsReq',
			msgid=ref_id, ctidTraderAccountId=int(list(self.accounts.keys())[0]),
			symbolId=[product]
		)
		self.parent._wait(ref_id)

		for i in range(14):
			if i % 5 == 0:
				time.sleep(1)

			self.client.emit(
				'SubscribeLiveTrendbarReq',
				ctidTraderAccountId=int(list(self.accounts.keys())[0]),
				symbolId=product, period=i+1
			)



	def onChartUpdate(self, chart, payload):
		result = []
		if len(payload.trendbar) > 0:
			for i in payload.trendbar:
				period = self._convert_sw_period(i.period)
				if period in chart.getActivePeriods():
					if (isinstance(chart.bid.get(period), np.ndarray) and 
						isinstance(chart.ask.get(period), np.ndarray)):

						bar_ts = i.utcTimestampInMinutes*60
						# Handle period bar end
						if chart.lastTs[period] is None:
							chart.lastTs[period] = bar_ts
						elif bar_ts > chart.lastTs[period]:
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

							chart.lastTs[period] = bar_ts

							chart.ask[period] = np.array([chart.ask[period][3]]*4, dtype=np.float64)
							chart.bid[period] = np.array([chart.bid[period][3]]*4, dtype=np.float64)
							chart.mid[period] = np.array(
								[np.around(
									(chart.ask[period][3] + chart.bid[period][3])/2,
									decimals=5
								)]*4, 
							dtype=np.float64)

		c_ts = time.time()
		ask = payload.ask / 100000
		bid = payload.bid / 100000
		for period in chart.getActivePeriods():
			if (isinstance(chart.bid.get(period), np.ndarray) and 
				isinstance(chart.ask.get(period), np.ndarray)):
				if ask:
					chart.ask[period][1] = ask if ask > chart.ask[period][1] else chart.ask[period][1]
					chart.ask[period][2] = ask if ask < chart.ask[period][2] else chart.ask[period][2]
					chart.ask[period][3] = ask
				
				# Bid
				if bid:
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
					'timestamp': c_ts,
					'item': {
						'ask': chart.ask[period].tolist(),
						'mid': chart.mid[period].tolist(),
						'bid': chart.bid[period].tolist()
					}
				})

			elif period == tl.period.TICK:
				if ask:
					chart.ask[period] = ask
				if bid:
					chart.bid[period] = bid
				
				if chart.ask[period] and chart.bid[period]:
					chart.mid[period] = np.around((chart.ask[period] + chart.bid[period])/2, decimals=5)

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


	def _convert_product(self, product):
		if product == tl.product.GBPUSD:
			return 2
		elif product == tl.product.EURUSD:
			return 1


	def _convert_sw_product(self, product):
		if product == 2:
			return tl.product.GBPUSD
		elif product == 1:
			return tl.product.EURUSD


	def isPeriodCompatible(self, period):
		return period in [
			tl.period.ONE_MINUTE, tl.period.TWO_MINUTES,
			tl.period.THREE_MINUTES, tl.period.FOUR_MINUTES,
			tl.period.FIVE_MINUTES, tl.period.TEN_MINUTES,
			tl.period.FIFTEEN_MINUTES, tl.period.THIRTY_MINUTES, 
			tl.period.ONE_HOUR, tl.period.FOUR_HOURS, 
			tl.period.TWELVE_HOURS, tl.period.DAILY, 
			tl.period.WEEKLY, tl.period.MONTHLY
		]


	def _convert_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return 1
		elif period == tl.period.TWO_MINUTES:
			return 2
		elif period == tl.period.THREE_MINUTES:
			return 3
		elif period == tl.period.FOUR_MINUTES:
			return 4
		elif period == tl.period.FIVE_MINUTES:
			return 5
		elif period == tl.period.TEN_MINUTES:
			return 6
		elif period == tl.period.FIFTEEN_MINUTES:
			return 7
		elif period == tl.period.THIRTY_MINUTES:
			return 8
		elif period == tl.period.ONE_HOUR:
			return 9
		elif period == tl.period.FOUR_HOURS:
			return 10
		elif period == tl.period.TWELVE_HOURS:
			return 11
		elif period == tl.period.DAILY:
			return 12
		elif period == tl.period.WEEKLY:
			return 13
		elif period == tl.period.MONTHLY:
			return 14


	def _convert_sw_period(self, period):
		if period == 1:
			return tl.period.ONE_MINUTE
		elif period == 2:
			return tl.period.TWO_MINUTES
		elif period == 3:
			return tl.period.THREE_MINUTES
		elif period == 4:
			return tl.period.FOUR_MINUTES
		elif period == 5:
			return tl.period.FIVE_MINUTES
		elif period == 6:
			return tl.period.TEN_MINUTES
		elif period == 7:
			return tl.period.FIFTEEN_MINUTES
		elif period == 8:
			return tl.period.THIRTY_MINUTES
		elif period == 9:
			return tl.period.ONE_HOUR
		elif period == 10:
			return tl.period.FOUR_HOURS
		elif period == 11:
			return tl.period.TWELVE_HOURS
		elif period == 12:
			return tl.period.DAILY
		elif period == 13:
			return tl.period.WEEKLY
		elif period == 14:
			return tl.period.MONTHLY


	def _create_empty_asks_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 'ask_open', 'ask_high', 'ask_low', 'ask_close'
		]).set_index('timestamp')


	def _create_empty_mids_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 'mid_open', 'mid_high', 'mid_low', 'mid_close'
		]).set_index('timestamp')


	def _create_empty_bids_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 'bid_open', 'bid_high', 'bid_low', 'bid_close'
		]).set_index('timestamp')


	def _bar_data_constructor(self, payload, df):
		if not payload.trendbar is None:
			for i in payload.trendbar:
				df.loc[i.utcTimestampInMinutes * 60] = [
					(i.low + i.deltaOpen) / 100000, # Open
					(i.low + i.deltaHigh) / 100000, # High
					i.low / 100000, # Low
					(i.low + i.deltaClose) / 100000 # Close
				]

		return df.sort_index()


	def _tick_data_constructor(self, period, payload, df):
		offset = tl.period.getPeriodOffsetSeconds(period)

		c_ts = None
		bar_ts = None
		price = None
		ohlc = [None] * 4
		for i in range(len(payload.tickData)):
			tick = payload.tickData[i]

			if i == 0:
				c_ts = tick.timestamp
				price = tick.tick
				ohlc = [price] * 4

				# Get Current Bar Timestamp
				ref_ts = tl.utils.getWeekstartDate(tl.convertTimestampToTime(tick.timestamp/1000)).timestamp()
				bar_ts = (int(c_ts/1000) - (int(c_ts/1000) - ref_ts) % offset) * 1000

			else:
				c_ts += tick.timestamp
				price += tick.tick

			if c_ts < bar_ts:
				df.loc[int(bar_ts/1000)] = ohlc

				ref_ts = tl.utils.getWeekstartDate(tl.convertTimestampToTime(tick.timestamp/1000)).timestamp()
				bar_ts = tl.utils.getPrevTimestamp(period, int(bar_ts/1000), now=int(c_ts/1000)) * 1000

				ohlc = [price] * 4

			if ohlc[1] is None or price > ohlc[1]:
				ohlc[1] = price
			if ohlc[2] is None or price < ohlc[2]:
				ohlc[2] = price

			ohlc[0] = price

		df.values[:] = df.values[:] / 100000
		return df


	def addChild(self, child):
		self.children.append(child)


	def deleteChild(self, child):
		if child in self.children:
			del self.children[self.children.index(child)]
