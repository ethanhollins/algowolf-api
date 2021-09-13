import time
import traceback
import numpy as np
import pandas as pd
import json
import ntplib
from datetime import datetime
from copy import copy
from threading import Thread
from .spotware_connect.client import Client
from .spotware_connect.messages import OpenApiCommonMessages_pb2 as o1
from .spotware_connect.messages import OpenApiMessages_pb2 as o2
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException

ONE_HOUR = 60*60

class Spotware(Broker):

	def __init__(self,
		ctrl, is_demo, access_token=None, refresh_token=None,
		user_account=None, strategy_id=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False, is_parent=False, assets=None, symbols=None
	):
		print(f"SPOTWARE INIT: {strategy_id}")

		self.strategyId = strategy_id
		self.brokerId = strategy_id
		self.accounts = accounts

		if not is_parent:
			super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.SPOTWARE_NAME, accounts, display_name, is_dummy, True)

		self.ctrl = ctrl
		self.is_demo = is_demo
		self.is_parent = is_parent
		self.is_dummy = is_dummy

		self._handled_position_events = {
			tl.MARKET_ENTRY: {},
			tl.POSITION_CLOSE: {}
		}

		self._price_queue = []
		self._account_update_queue = []
		self.time_off = 0
		self._set_time_off()

		'''
		Setup Spotware Funcs
		'''
		if self.is_parent:
			super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.SPOTWARE_NAME, accounts, display_name, is_dummy, True)

			self.parent = self
			self.children = []

			user = {}
			if self.ctrl.app.config['SERVER'] == 0:
				user = self.ctrl.getDb().getUser("spotware")
			elif self.ctrl.app.config['SERVER'] == 1:
				user = self.ctrl.getDb().getUser("spotware_1")

			self.access_token = user.get('access_token')
			self.refresh_token = user.get('refresh_token')

			self.is_auth = self._add_user()
			self._subscribe_account_updates()

			# self.demo_client = Client(True)
			# self.live_client = Client(False)

			# self.demo_client.event('connect', self.connect)
			# self.demo_client.event('disconnect', self.disconnect)
			# self.demo_client.event('message', self.message)

			# self.live_client.event('connect', self.connect)
			# self.live_client.event('disconnect', self.disconnect)
			# self.live_client.event('message', self.message)

			# self.demo_client.connect()
			# self.live_client.connect()

			# while not self._spotware_connected:
			# 	pass


			# self._authorize_accounts(self.accounts, is_parent=True)

			if self.is_auth:
				CHARTS = ['EUR_USD']
				# self._subscribe_multiple_chart_updates(CHARTS)
				for instrument in CHARTS:
					print(f'LOADING {instrument}')
					# instrument = self._get_symbol(i)['symbolName']
					chart = self.createChart(instrument, await_completion=True)

			# Start refresh thread
			

		else:
			self.access_token = access_token
			self.refresh_token = refresh_token

			self.parent = ctrl.brokers.getBroker(tl.broker.SPOTWARE_NAME)
			self.parent.addChild(self)

			self.is_auth = self._add_user()

			# self.client = self.parent.client
			# self._authorize_accounts(accounts)

		t = Thread(target=self._handle_updates)
		t.start()

		if not is_dummy and self.is_auth:
			self._subscribe_account_updates()
			# for account_id in self.getAccounts():
			# 	if account_id != tl.broker.PAPERTRADER_NAME:
					# self._subscribe_account_updates(account_id)

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()


	def _set_time_off(self):
		try:
			client = ntplib.NTPClient()
			response = client.request('pool.ntp.org')
			self.time_off = response.tx_time - time.time()
		except Exception:
			pass


	def _set_tokens(self):
		res = self.ctrl.brokerRequest(
			'spotware', self.brokerId, 'get_tokens', self.brokerId
		)
		print(f'UPDATING TOKENS {self.brokerId}: {res}')

		if not 'error' in res:
			self._update_tokens(res)


	def _update_tokens(self, res):
		self.access_token = res['access_token']
		self.refresh_token = res['refresh_token']
		if self.is_parent:
			print(f'UPDATE PARENT {res}')
			self.ctrl.getDb().updateUser(
				'spotware',
				{
					'access_token': self.access_token,
					'refresh_token': self.refresh_token
				}
			)

		else:
			print(f'UPDATE ACCOUNT {res}')
			self.ctrl.getDb().updateBroker(
				self.userAccount.userId, self.brokerId, 
				{ 
					'access_token': self.access_token,
					'refresh_token': self.refresh_token
				}
			)


	'''
	Spotware messages
	'''

	def _periodic_refresh(self):
		TEN_SECONDS = 10
		while self.is_running:
			if time.time() - self._last_update > TEN_SECONDS:
				try:
					heartbeat = o1.ProtoHeartbeatEvent()
					self.demo_client.send(heartbeat)
					self.live_client.send(heartbeat)
					self._last_update = time.time()
				except Exception as e:
					print(f'[SC] {str(e)}')
					pass

			time.sleep(1)


	def _add_user(self):
		print('Add User')

		if self.userAccount is not None:
			user_id = self.userAccount.userId
		else:
			user_id = None

		res = self.ctrl.brokerRequest(
			'spotware', self.brokerId, 'add_user',
			user_id, self.brokerId, 
			self.access_token, self.refresh_token, self.accounts,
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


	def _wait(self, ref_id, polling=0.1, timeout=30):
		start = time.time()
		while not ref_id in self._handled:
			if time.time() - start >= timeout:
				return None
			time.sleep(polling)

		item = self._handled[ref_id]
		del self._handled[ref_id]
		return item


	def _wait_for_position(self, order_id, polling=0.1, timeout=30):
		start = time.time()
		while not order_id in self._handled_position_events[tl.MARKET_ENTRY]:
			if time.time() - start >= timeout:
				return None
			time.sleep(polling)

		return self._handled_position_events[tl.MARKET_ENTRY][order_id]


	def _wait_for_close(self, order_id, polling=0.1, timeout=30):
		start = time.time()
		while not order_id in self._handled_position_events[tl.POSITION_CLOSE]:
			if time.time() - start >= timeout:
				return None
			time.sleep(polling)

		return self._handled_position_events[tl.POSITION_CLOSE][order_id]


	def _get_client(self, account_id):
		if self.accounts[str(account_id)]['is_demo']:
			return self.parent.demo_client
		else:
			return self.parent.live_client


	def connect(self, is_demo):
		print('Spotware connected!')

		# Application Auth
		auth_req = o2.ProtoOAApplicationAuthReq(
			clientId = self.ctrl.app.config['SPOTWARE_CLIENT_ID'],
			clientSecret = self.ctrl.app.config['SPOTWARE_CLIENT_SECRET']
		)
		if is_demo:
			self.demo_client.send(auth_req, msgid=self.generateReference())
		else:
			self.live_client.send(auth_req, msgid=self.generateReference())


	def disconnect(self, is_demo):
		print('Spotware disconnected')


	def message(self, is_demo, payloadType, payload, msgid):

		if not payloadType in (2138,2131,2165,2120,2153,2160,2113,2115):
			print(f'MSG: ({payloadType}) {payload}')

		# Heartbeat
		if payloadType == 51:
			heartbeat = o1.ProtoHeartbeatEvent()
			if is_demo:
				self.demo_client.send(heartbeat)
			else:
				self.live_client.send(heartbeat)
			self._last_update = time.time()

		elif payloadType == 2101:
			self._spotware_connected = True

			for child in self.children:
				child._authorize_accounts(child.accounts)

		# Tick
		elif payloadType == 2131:
			if str(payload.symbolId) in self._subscriptions:
				self._subscriptions[str(payload.symbolId)](payload)

		else:
			result = None
			if 'ctidTraderAccountId' in payload.DESCRIPTOR.fields_by_name.keys():
				# print(f'MSG: {payload}')
				account_id = payload.ctidTraderAccountId
				for child in copy(self.children):
					if account_id in map(int, child.accounts.keys()):
						result = child._on_account_update(account_id, payload, msgid)

						if isinstance(result, dict): 
							for k, v in result.items():
								if v['accepted']:
									if v['type'] == tl.MARKET_ENTRY:
										self._handled_position_events[tl.MARKET_ENTRY][v['item'].order_id] = {k: v}
									elif v['type'] == tl.POSITION_CLOSE:
										self._handled_position_events[tl.POSITION_CLOSE][v['item'].order_id] = {k: v}

						break

			if msgid:
				if result is None:
					self._handled[msgid] = payload
				else:
					self._handled[msgid] = result


	def _set_options(self):
		path = self.ctrl.app.config['BROKERS']
		with open(path, 'r') as f:
			options = json.load(f)
		
		options[self.name] = {
			**options[self.name],
			**{
				"access_token": self.access_token,
				"refresh_token": self.refresh_token
			}
		}

		with open(path, 'w') as f:
			f.write(json.dumps(options, indent=2))


	def _refresh_token(self, is_parent=False):
		print(f'REFRESH: {self.refresh_token}')


		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_refresh_token'
		)


		# ref_id = self.generateReference()
		# refresh_req = o2.ProtoOARefreshTokenReq(
		# 	refreshToken=self.refresh_token
		# )
		# self.parent.demo_client.send(refresh_req, msgid=ref_id)
		# res = self.parent._wait(ref_id)
		# if res.payloadType == 2174:

		if 'error' not in res:
			self.access_token = res['access_token']
			self.refresh_token = res['refresh_token']
			if is_parent:
				self.ctrl.getDb().updateUser(
					self.name,
					{
						'access_token': self.access_token,
						'refresh_token': self.refresh_token
					}
				)

			else:
				self.ctrl.getDb().updateBroker(
					self.userAccount.userId, self.brokerId, 
					{ 
						'access_token': self.access_token,
						'refresh_token': self.refresh_token
					}
				)


	def _authorize_accounts(self, accounts, is_parent=False):
		print(f'MSG: {self.strategyId}, {self.brokerId}, {accounts}')

		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_authorize_accounts',
			accounts, is_parent=is_parent
		)


		# if self.refresh_token is not None:
		# 	self._refresh_token(is_parent=is_parent)

		# for account_id in accounts:
		# 	ref_id = self.generateReference()
		# 	acc_auth = o2.ProtoOAAccountAuthReq(
		# 		ctidTraderAccountId=int(account_id), 
		# 		accessToken=self.access_token
		# 	)
		# 	self._get_client(account_id).send(acc_auth, msgid=ref_id)
		# 	res = self.parent._wait(ref_id)
			
		# 	trader_ref_id = self.generateReference()
		# 	trader_req = o2.ProtoOATraderReq(
		# 		ctidTraderAccountId=int(account_id)
		# 	)
		# 	self._get_client(account_id).send(trader_req, msgid=trader_ref_id)
		# 	trader_res = self.parent._wait(trader_ref_id)

		# 	self._set_broker_info(account_id, trader_res.trader.brokerName)

			# if res.payloadType == 2142:
			# 	return self._authorize_accounts(accounts)


	def _set_broker_info(self, account_id, broker_name):
		if broker_name not in self.parent.assets or broker_name not in self.parent.symbols:
			print(f'Setting {broker_name} info...')

			asset_ref_id = self.generateReference()
			asset_req = o2.ProtoOAAssetListReq(
				ctidTraderAccountId=int(account_id)
			)
			self._get_client(account_id).send(asset_req, msgid=asset_ref_id)
			asset_res = self.parent._wait(asset_ref_id)

			self.parent.assets[broker_name] = {}
			self.parent.assets_by_name[broker_name] = {}
			for i in asset_res.asset:
				self.parent.assets[broker_name][str(i.assetId)] = {
					'name': i.name,
					'displayName': i.displayName
				}
				self.parent.assets_by_name[broker_name][str(i.name)] = {
					'assetId': i.assetId,
					'displayName': i.displayName
				}

			symbol_ref_id = self.generateReference()
			symbol_req = o2.ProtoOASymbolsListReq(
				ctidTraderAccountId=int(account_id) 
			)
			self._get_client(account_id).send(symbol_req, msgid=symbol_ref_id)
			symbol_res = self.parent._wait(symbol_ref_id)

			self.parent.symbols[broker_name] = {}
			self.parent.symbols_by_name[broker_name] = {}
			for i in symbol_res.symbol:
				self.parent.symbols[broker_name][str(i.symbolId)] = {
					'symbolName': i.symbolName,
					'baseAssetId': i.baseAssetId,
					'quoteAssetId': i.quoteAssetId,
					'symbolCategoryId': i.symbolCategoryId
				}
				self.parent.symbols_by_name[broker_name][str(i.symbolName)] = {
					'symbolId': i.symbolId,
					'baseAssetId': i.baseAssetId,
					'quoteAssetId': i.quoteAssetId,
					'symbolCategoryId': i.symbolCategoryId
				}


	'''
	Broker functions
	'''

	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):

		if isinstance(start, datetime):
			start = tl.convertTimeToTimestamp(start)
		if isinstance(end, datetime):
			end = tl.convertTimeToTimestamp(end)

		res = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_download_historical_data_broker',
			product, period, tz=tz, start=start, end=end,
			count=count, force_download=force_download
		)

		if 'error' in res:
			result = self._create_empty_df(period)
		else:
			result = pd.DataFrame.from_dict(res, dtype=float)
			result.index = result.index.astype(int)

		# sw_product = self._convert_product('Spotware', product)
		# sw_period = self._convert_period(period)

		# result = pd.concat((
		# 	self._create_empty_asks_df(), 
		# 	self._create_empty_mids_df(), 
		# 	self._create_empty_bids_df()
		# ))

		# dl_start = None
		# dl_end = None
		# if start:
		# 	dl_start = tl.utils.convertTimeToTimestamp(start)
		# if end:
		# 	dl_end = tl.utils.convertTimeToTimestamp(end)

		# if count:
		# 	if start:
		# 		dl_end = tl.utils.convertTimeToTimestamp(tl.utils.getCountDate(period, count+1, start=start))
		# 	elif end:
		# 		dl_start = tl.utils.convertTimeToTimestamp(tl.utils.getCountDate(period, count+1, end=end))
		# 	else:
		# 		dl_start = tl.utils.convertTimeToTimestamp(tl.utils.getCountDate(period, count+1))
		# 		dl_end = tl.utils.convertTimeToTimestamp(datetime.utcnow()) + tl.period.getPeriodOffsetSeconds(period)

		# while True:
		# 	ref_id = self.generateReference()
		# 	trendbars_req = o2.ProtoOAGetTrendbarsReq(
		# 		ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 		fromTimestamp=int(dl_start*1000), toTimestamp=int(dl_end*1000), 
		# 		symbolId=sw_product, period=sw_period
		# 	)
		# 	self._get_client(list(self.accounts.keys())[0]).send(trendbars_req, msgid=ref_id)

		# 	res = self._wait(ref_id)

		# 	'''
		# 	Bar Constructor
		# 	'''

		# 	if res.payloadType == 2138:
		# 		mids = self._bar_data_constructor(res, self._create_empty_mids_df())
		# 		asks = mids.copy()
		# 		asks.columns = ['ask_open', 'ask_high', 'ask_low', 'ask_close']
		# 		bids = mids.copy()
		# 		bids.columns = ['bid_open', 'bid_high', 'bid_low', 'bid_close']

		# 		result = pd.concat((
		# 			result,
		# 			pd.concat((asks, mids, bids), axis=1)
		# 		))

		# 		if count and result.shape[0] < count:
		# 			dl_end = dl_start
		# 			dl_start = tl.convertTimeToTimestamp(tl.utils.getCountDate(
		# 				period, count+1, end=tl.convertTimestampToTime(dl_end)
		# 			))
		# 		else:
		# 			break

		# 	else:
		# 		break

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
		order_id = str(pos['positionId'])
		product = self._convert_sw_product(int(pos['tradeData']['symbolId']))
		direction = tl.LONG if pos['tradeData']['tradeSide'] == 'BUY' else tl.SHORT
		lotsize = self._convert_from_sw_lotsize(float(pos['tradeData']['volume']))
		entry_price = float(pos['price'])
		sl = None if pos.get('stopLoss') is None or float(pos['stopLoss']) == 0 else round(float(pos['stopLoss']), 5)
		tp = None if pos.get('takeProfit') is None or float(pos['takeProfit']) == 0 else round(float(pos['takeProfit']), 5)
		open_time = float(pos['tradeData']['openTimestamp']) / 1000

		return tl.Position(
			self,
			order_id, str(account_id), product,
			tl.MARKET_ENTRY, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def convert_sw_order(self, account_id, order):
		entry_price = None
		if order['orderType'] == 'STOP':
			entry_price = float(order['stopPrice'])
			order_type = tl.STOP_ORDER
		elif order['orderType'] == 'LIMIT':
			entry_price = float(order['limitPrice'])
			order_type = tl.LIMIT_ORDER

		order_id = str(order['orderId'])
		product = self._convert_sw_product(int(order['tradeData']['symbolId']))
		direction = tl.LONG if order['tradeData']['tradeSide'] == 'BUY' else tl.SHORT
		lotsize = self._convert_from_sw_lotsize(float(order['tradeData']['volume']))

		if order.get('stopLoss') is not None:
			sl = round(float(order['stopLoss']), 5)
		elif order.get('relativeStopLoss') is not None:
			if direction == tl.LONG:
				sl = round(entry_price - tl.utils.convertToPrice(float(order.get('relativeStopLoss'))/10), 5)
			else:
				sl = round(entry_price + tl.utils.convertToPrice(float(order.get('relativeStopLoss'))/10), 5)
		else:
			sl = None

		if order.get('takeProfit') is not None:
			tp = round(float(order['takeProfit']), 5)
		elif order.get('relativeTakeProfit') is not None:
			if direction == tl.LONG:
				tp = round(entry_price + tl.utils.convertToPrice(float(order.get('relativeTakeProfit'))/10), 5)
			else:
				tp = round(entry_price - tl.utils.convertToPrice(float(order.get('relativeTakeProfit'))/10), 5)
		else:
			tp = None

		open_time = float(order['tradeData']['openTimestamp']) / 1000

		return tl.Order(
			self,
			order_id, str(account_id), product,
			order_type, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def _get_all_positions(self, account_id):
		print(f'[_get_all_positions] GET POSITIONS', flush=True)

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_get_all_positions',
			account_id
		)
		print(f'[_get_all_positions] {result}', flush=True)

		for account_id in result:
			for i in range(len(result[account_id])):
				result[account_id][i] = tl.Position.fromDict(self, result[account_id][i])
		print(f'[_get_all_positions] {result}', flush=True)

		# ref_id = self.generateReference()
		# pos_req = o2.ProtoOAReconcileReq(
		# 	ctidTraderAccountId=int(account_id)
		# )
		# self._get_client(account_id).send(pos_req, msgid=ref_id)
		# res = self.parent._wait(ref_id)

		# result = { account_id: [] }
		# if res.payloadType == 2125:
		# 	for pos in res.position:
		# 		new_pos = self.convert_sw_position(account_id, pos)

		# 		result[account_id].append(new_pos)

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


		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createPosition',
			product, lotsize, direction,
			account_id, entry_range, entry_price,
			sl_tp_prices, sl_tp_ranges
		)

		if 'order_id' in result:
			order_id = str(result.get('order_id'))
			pos_res = self._wait_for_position(order_id)
			if not pos_res is None:
				ref_id = list(pos_res.keys())[0]
				item = pos_res[ref_id]
				pos = item['item']
				print(f'CREATE POS -> {pos}')

				# Modify to correct SL/TP
				if len(sl_tp_prices) > 0:
					if not tp_price:
						tp_price = pos.tp
					if not sl_price:
						sl_price = pos.sl

					print('CREATE POS -> MODIFY')
					self.modifyPosition(pos, sl_price, tp_price)

				result = pos_res

		return result

		# ref_id = self.generateReference()

		# sl_tp_prices = {}
		# sl_tp_ranges = {}
		# if sl_price:
		# 	sl_tp_prices['stopLoss'] = sl_price

		# 	# Get range from current price for temp sl
		# 	chart = self.getChart(product)
		# 	if direction == tl.LONG:
		# 		sl_tp_ranges['relativeStopLoss'] = int((chart.ask[tl.period.ONE_MINUTE][3] - sl_price) * 100000)
		# 	else:
		# 		sl_tp_ranges['relativeStopLoss'] = int((sl_price - chart.bid[tl.period.ONE_MINUTE][3]) * 100000)

		# if sl_range:
		# 	sl_tp_ranges['relativeStopLoss'] = int(sl_range)

		# if tp_price:
		# 	sl_tp_prices['takeProfit'] = tp_price
		# 	# Get range from current price for temp tp
		# 	chart = self.getChart(product)
		# 	if direction == tl.LONG:
		# 		sl_tp_ranges['relativeTakeProfit'] = int((tp_price - chart.ask[tl.period.ONE_MINUTE][3]) * 100000)
		# 	else:
		# 		sl_tp_ranges['relativeTakeProfit'] = int((chart.bid[tl.period.ONE_MINUTE][3] - tp_price) * 100000)

		# if tp_range:
		# 	sl_tp_ranges['relativeTakeProfit'] = int(tp_range)
		
		# broker_name = self.accounts[account_id]['broker']
		# sw_product = self._convert_product(broker_name, product)
		# direction = 1 if direction == tl.LONG else 2
		# lotsize = round(lotsize / 100000) * 100000

		# '''
		# TEMP
		# '''
		# # sl_tp_ranges['relativeStopLoss'] = int(round(sl_tp_ranges['relativeStopLoss']/100) * 100)
		# # sl_tp_ranges['relativeTakeProfit'] = int(round(sl_tp_ranges['relativeTakeProfit']/100) * 100)
		# # lotsize = int(lotsize / 100000)
		# '''
		# TEMP
		# '''
		# start_time = time.time()
		# print(f'CREATE POSITION START: {self.brokerId}')

		# # Execute Market Order
		# new_order = o2.ProtoOANewOrderReq(
		# 	ctidTraderAccountId=int(account_id),
		# 	symbolId=sw_product, orderType=1, tradeSide=direction,
		# 	volume=lotsize, **sl_tp_ranges
		# )
		# print(f'Sending:\n{new_order}')
		# self._get_client(account_id).send(new_order, msgid=ref_id)
		# res = self.parent._wait(ref_id)
		# print(f'Result:\n{res}')

		# result = {}
		# if res.payloadType == 2126:
		# # 	new_pos = self.convert_sw_position(account_id, res.position)
		# 	pos_res = self.parent._wait_for_position(str(res.position.positionId))
		# 	print(f'Pos Res: {pos_res}')

		# 	if pos_res is not None:
		# 		ref_id = list(pos_res.keys())[0]
		# 		item = pos_res[ref_id]
		# 		pos = item['item']

		# 		if len(sl_tp_prices) > 0:
		# 			mod_ref_id = self.generateReference()

		# 			amend_req = o2.ProtoOAAmendPositionSLTPReq(
		# 				ctidTraderAccountId=int(pos.account_id),
		# 				positionId=int(pos.order_id), **sl_tp_prices
		# 			)
		# 			self._get_client(account_id).send(amend_req, msgid=mod_ref_id)

		# 			res = self.parent._wait(mod_ref_id)

		# 		result.update(pos_res)

		# elif not res is None and res.payloadType in (50, 2132):
		# 	result.update({
		# 		ref_id: {
		# 			'timestamp': time.time(),
		# 			'type': tl.MARKET_ENTRY,
		# 			'accepted': False,
		# 			'message': res.errorCode
		# 		}
		# 	})

		# else:
		# 	result.update({
		# 		ref_id: {
		# 			'timestamp': time.time(),
		# 			'type': tl.MARKET_ENTRY,
		# 			'accepted': False
		# 		}
		# 	})

		# print(f'CREATE POSITION END: {self.brokerId} {round(time.time() - start_time, 2)}s')
		return result


	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyPosition(
				pos, sl_price, tp_price, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyPosition',
			pos.account_id, pos.order_id, sl_price, tp_price
		)

		res = self._wait(result.get('ref_id'))

		if not isinstance(res, dict):
			# if not res is None and res.payloadType in (50, 2132):
			# 	res = {
			# 		ref_id: {
			# 			'timestamp': time.time(),
			# 			'type': order_type,
			# 			'accepted': False,
			# 			'message': res.errorCode
			# 		}
			# 	}
			# else:
			res = {
				ref_id: {
					'timestamp': time.time(),
					'type': tl.MODIFY,
					'accepted': False
				}
			}

		return res

		# ref_id = self.generateReference()

		# start_time = time.time()
		# print(f'MODIFY POSITION START: {self.brokerId}')
		# amend_req = o2.ProtoOAAmendPositionSLTPReq(
		# 	ctidTraderAccountId=int(pos.account_id),
		# 	positionId=int(pos.order_id), stopLoss=sl_price, takeProfit=tp_price
		# )
		# self._get_client(pos.account_id).send(amend_req, msgid=ref_id)
		# res = self.parent._wait(ref_id)

		# if not isinstance(res, dict):
		# 	if not res is None and res.payloadType in (50, 2132):
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': tl.MODIFY,
		# 				'accepted': False,
		# 				'message': res.errorCode
		# 			}
		# 		}
		# 	else:
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': tl.MODIFY,
		# 				'accepted': False
		# 			}
		# 		}

		# print(f'MODIFY POSITION END: {self.brokerId} {round(time.time() - start_time, 2)}s')
		# return result


	def deletePosition(self, pos, lotsize, override=False):
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deletePosition(
				pos, lotsize, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deletePosition',
			pos.account_id, pos.order_id, lotsize
		)

		if 'order_id' in result:
			order_id = str(result.get('order_id'))
			pos_res = self._wait_for_close(order_id)
			if not pos_res is None:
				ref_id = list(pos_res.keys())[0]
				item = pos_res[ref_id]
				print(f'Pos Res [delete]: {pos_res}', flush=True)

				result = pos_res

		return result
		# ref_id = self.generateReference()

		# close_req = o2.ProtoOAClosePositionReq(
		# 	ctidTraderAccountId=int(pos.account_id),
		# 	positionId=int(pos.order_id), volume=lotsize
		# )
		# self._get_client(pos.account_id).send(close_req, msgid=ref_id)

		# res = self.parent._wait(ref_id)

		# start_time = time.time()
		# print(f'DELETE POSITION START: {self.brokerId}')
		# # Handle delete result
		# result = {}
		# if res.payloadType == 2126:
		# 	pos_res = self.parent._wait_for_close(str(pos.order_id))
		# 	if not pos_res is None:
		# 		ref_id = list(pos_res.keys())[0]
		# 		item = pos_res[ref_id]
		# 		print(f'Pos Res [delete]: {pos_res}')

		# 		result.update(pos_res)

		# elif not res is None and res.payloadType in (50, 2132):
		# 	result.update({
		# 		ref_id: {
		# 			'timestamp': time.time(),
		# 			'type': tl.POSITION_CLOSE,
		# 			'accepted': False,
		# 			'message': res.errorCode
		# 		}
		# 	})

		# else:
		# 	result.update({
		# 		ref_id: {
		# 			'timestamp': time.time(),
		# 			'type': tl.POSITION_CLOSE,
		# 			'accepted': False
		# 		}
		# 	})

		# print(f'DELETE POSITION END: {self.brokerId} {round(time.time() - start_time, 2)}s')
		# return result


	def _get_all_orders(self, account_id):

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, '_get_all_orders',
			account_id
		)

		for account_id in result:
			for i in range(len(result[account_id])):
				result[account_id][i] = tl.Order.fromDict(self, result[account_id][i])


		# ref_id = self.generateReference()
		# order_req = o2.ProtoOAReconcileReq(
		# 	ctidTraderAccountId=int(account_id)
		# )
		# self._get_client(account_id).send(order_req, msgid=ref_id)
		# res = self.parent._wait(ref_id)

		# result = { account_id: [] }
		# if res.payloadType == 2125:
		# 	for order in res.order:
		# 		new_order = self.convert_sw_order(account_id, order)

		# 		result[account_id].append(new_order)

		return result


	def getAllAccounts(self):

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAllAccounts'
		)

		return result

		# ref_id = self.generateReference()
		# accounts_req = o2.ProtoOAGetAccountListByAccessTokenReq(
		# 	accessToken=self.access_token
		# )
		# self.parent.demo_client.send(accounts_req, msgid=ref_id)

		# res = self.parent._wait(ref_id)
		# if res is not None:
		# 	self.accounts = { str(i.ctidTraderAccountId): { 'is_demo': not i.isLive } for i in res.ctidTraderAccount }
		# 	self._authorize_accounts([i.ctidTraderAccountId for i in res.ctidTraderAccount])

		# 	result = []
		# 	for i in res.ctidTraderAccount:
		# 		if res.permissionScope == 1:

		# 			trader_ref_id = self.generateReference()
		# 			trader_req = o2.ProtoOATraderReq(
		# 				ctidTraderAccountId=int(i.ctidTraderAccountId)
		# 			)

		# 			self._get_client(i.ctidTraderAccountId).send(trader_req, msgid=trader_ref_id)

		# 			trader_res = self.parent._wait(trader_ref_id)

		# 			result.append({
		# 				'id': i.ctidTraderAccountId,
		# 				'is_demo': not i.isLive,
		# 				'account_id': i.traderLogin,
		# 				'broker': trader_res.trader.brokerName
		# 			})

		# 	return result

		# else:
		# 	return None


	def checkAccessToken(self, access_token):

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'checkAccessToken', access_token
		)

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

		print(f'GET ACCOUNT INFO: {result}')
		# ref_id = self.generateReference()
		# trader_req = o2.ProtoOATraderReq(
		# 	ctidTraderAccountId=int(account_id)
		# )
		# self._get_client(account_id).send(trader_req, msgid=ref_id)
		# res = self.parent._wait(ref_id)

		# # Handle account info result

		# result = {}

		# currency = self._get_asset(res.trader.brokerName, res.trader.depositAssetId)['name']
		# print(f'CURRENCY: {currency}')
		# balance = self.ctrl.spots[currency].convertFrom(res.trader.balance/100)
		# print(f'BALANCE: {currency}')

		# print(f'INFO: {currency}, {balance}')
		# if res.payloadType == 2122:
		# 	result[account_id] = {
		# 		'currency': currency,
		# 		'balance': balance,
		# 		'pl': None,
		# 		'margin': None,
		# 		'available': None
		# 	}
		
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

		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createOrder',
			product, lotsize, direction,
			account_id, order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		print(f'CREATE ORDER: {result}')

		if 'ref_id' in result:
			ref_id = result.get('ref_id')
			res = self._wait(ref_id)

			if not isinstance(res, dict):
				# if not res is None and res.payloadType in (50, 2132):
				# 	res = {
				# 		ref_id: {
				# 			'timestamp': time.time(),
				# 			'type': order_type,
				# 			'accepted': False,
				# 			'message': res.errorCode
				# 		}
				# 	}
				# else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': order_type,
						'accepted': False
					}
				}
		else:
			res = result


		return res
		# ref_id = self.generateReference()

		# # Convert symbol
		# # symbol_id = 

		# params = {}
		# if order_type == tl.STOP_ORDER:
		# 	params['stopPrice'] = entry_price
		# elif order_type == tl.LIMIT_ORDER:
		# 	params['limitPrice'] = entry_price

		# if sl_price:
		# 	params['stopLoss'] = sl_price
		# else:
		# 	params['relativeStopLoss'] = sl_range

		# if tp_price:
		# 	params['takeProfit'] = tp_price
		# else:
		# 	params['relativeTakeProfit'] = tp_range

		# direction = 1 if direction == tl.LONG else 2
		# sw_order_type = 3 if order_type == tl.STOP_ORDER else 2
		# # lotsize = round(lotsize / 1000000) * 1000000
		# lotsize = round(lotsize / 100000) * 100000


		# '''
		# TEMP
		# '''
		# # lotsize = int(lotsize / 100000)
		# '''
		# TEMP
		# '''

		# start_time = time.time()
		# print(f'CREATE ORDER START: {self.brokerId}')
		# broker_name = self.accounts[account_id]['broker']
		# new_order_req = o2.ProtoOANewOrderReq(
		# 	ctidTraderAccountId=int(account_id),
		# 	symbolId=self._convert_product(broker_name, product), orderType=sw_order_type, tradeSide=direction,
		# 	volume=lotsize, **params
		# )
		# self._get_client(account_id).send(new_order_req, msgid=ref_id)

		# res = self.parent._wait(ref_id)
		# print(f'CREATE ORDER END: {self.brokerId} {round(time.time() - start_time, 2)}s')

		# if not isinstance(res, dict):
		# 	if not res is None and res.payloadType in (50, 2132):
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': order_type,
		# 				'accepted': False,
		# 				'message': res.errorCode
		# 			}
		# 		}
		# 	else:
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': order_type,
		# 				'accepted': False
		# 			}
		# 		}

		# return result


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyOrder(
				order, lotsize, entry_price, sl_price, tp_price, override=override
			)

		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)


		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyOrder',
			order.account_id, order.order_id, order.order_type, lotsize, entry_price, sl_price, tp_price
		)

		if 'ref_id' in result:
			ref_id = result.get('ref_id')
			res = self._wait(ref_id)

			if not isinstance(res, dict):
				# if not res is None and res.payloadType in (50, 2132):
				# 	res = {
				# 		ref_id: {
				# 			'timestamp': time.time(),
				# 			'type': order_type,
				# 			'accepted': False,
				# 			'message': res.errorCode
				# 		}
				# 	}
				# else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.MODIFY,
						'accepted': False
					}
				}
		else:
			res = result

		return res

		# ref_id = self.generateReference()

		# args = {}
		# if not entry_price is None:
		# 	if order.order_type == tl.STOP_ORDER:
		# 		args['stopPrice'] = entry_price
		# 	elif order.order_type == tl.LIMIT_ORDER:
		# 		args['limitPrice'] = entry_price
		# if not lotsize is None:
		# 	'''
		# 	TEMP
		# 	'''
		# 	# lotsize = int(lotsize / 100000)
		# 	'''
		# 	TEMP
		# 	'''

		# 	args['volume'] = lotsize
		# if not sl_price is None:
		# 	args['stopLoss'] = sl_price
		# if not tp_price is None:
		# 	args['takeProfit'] = tp_price

		# start_time = time.time()
		# print(f'MODIFY ORDER START: {self.brokerId}')
		# amend_req = o2.ProtoOAAmendOrderReq(
		# 	ctidTraderAccountId=int(order.account_id), orderId=int(order.order_id),
		# 	**args
		# )
		# self._get_client(order.account_id).send(amend_req, msgid=ref_id)

		# res = self.parent._wait(ref_id)
		# print(f'MODIFY ORDER END: {self.brokerId} {round(time.time() - start_time, 2)}s')

		# if not isinstance(res, dict):
		# 	if not res is None and res.payloadType in (50, 2132):
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': tl.MODIFY,
		# 				'accepted': False,
		# 				'message': res.errorCode
		# 			}
		# 		}
		# 	else:
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': tl.MODIFY,
		# 				'accepted': False
		# 			}
		# 		}

		# print(f'MOD: {res}')

		# return result


	def deleteOrder(self, order, override=False):
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deleteOrder(order, override=override)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)


		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deleteOrder',
			order.account_id, order.order_id
		)

		if 'ref_id' in result:
			ref_id = result.get('ref_id')
			res = self._wait(result.get('ref_id'))

			if not isinstance(res, dict):
				# if not res is None and res.payloadType in (50, 2132):
				# 	res = {
				# 		ref_id: {
				# 			'timestamp': time.time(),
				# 			'type': order_type,
				# 			'accepted': False,
				# 			'message': res.errorCode
				# 		}
				# 	}
				# else:
				res = {
					ref_id: {
						'timestamp': time.time(),
						'type': tl.ORDER_CANCEL,
						'accepted': False
					}
				}
		else:
			res = result

		return res

		# ref_id = self.generateReference()

		# start_time = time.time()
		# print(f'DELETE ORDER START: {self.brokerId}')
		# cancel_req = o2.ProtoOACancelOrderReq(
		# 	ctidTraderAccountId=int(order.account_id), orderId=int(order.order_id)
		# )
		# self._get_client(order.account_id).send(cancel_req, msgid=ref_id)

		# res = self.parent._wait(ref_id)
		# print(f'DELETE ORDER END: {self.brokerId} {round(time.time() - start_time, 2)}s')

		# if not isinstance(res, dict):
		# 	if not res is None and res.payloadType in (50, 2132):
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': tl.ORDER_CANCEL,
		# 				'accepted': False,
		# 				'message': res.errorCode
		# 			}
		# 		}
		# 	else:
		# 		res = {
		# 			ref_id: {
		# 				'timestamp': time.time(),
		# 				'type': tl.ORDER_CANCEL,
		# 				'accepted': False
		# 			}
		# 		}

		# return result


	def _subscribe_account_updates(self):
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'spotware', self.brokerId, '_subscribe_account_updates', stream_id
		)
		self.ctrl.addBrokerListener(stream_id, self._on_account_update)


	def _handle_updates(self):
		time_off_timer = time.time()

		while True:
			# Handle Account Updates
			if len(self._account_update_queue):
				try:
					payload_type, account_id, update, ref_id = self._account_update_queue[0]
					self._handle_account_update(payload_type, account_id, update, ref_id)
				except Exception:
					print(traceback.format_exc())
				finally:
					del self._account_update_queue[0]

			# Handle Chart Updates
			if len(self._price_queue):
				try:
					chart, payload = self._price_queue[0]
					self._handle_chart_update(chart, payload)
				except Exception:
					print(traceback.format_exc())
				finally:
					del self._price_queue[0]

			# Handle Auto Bar End
			try:
				self._handle_chart_auto_bar_end()
			except Exception:
				print(traceback.format_exc())

			# Refresh Time Adjustment
			if time.time() - time_off_timer > ONE_HOUR:
				time_off_timer = time.time()
				self._set_time_off()
				# self._set_tokens()

			time.sleep(1)


	def _handle_account_update(self, payload_type, account_id, update, msg_id):
		print(f'_on_account_update: {payload_type} {account_id} {update}', flush=True)

		if update.get('type') == 'connected':
			print(f'[_on_account_update] RECONNECTED')
			if self.userAccount and self.brokerId:
				print(f'[_on_account_update] Retrieving positions/orders')
				self._handle_live_strategy_setup()

		elif payload_type is not None and int(payload_type) == 2126:
			# if not ref_id:
			ref_id = self.generateReference()

			print(f'Account Update: {update}')
			execution_type = update['executionType']

			result = {}
			# ORDER_FILLED
			# if execution_type in ('ORDER_FILLED', 'ORDER_PARTIAL_FILL'):
			if execution_type == 'ORDER_FILLED':
				# Check `closingOrder`
				if update['order']['closingOrder']:
					# Delete
					for i in range(len(self.positions)):
						pos = self.positions[i]
						if str(update['position']['positionId']) == pos.order_id:
							if update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
								pos.close_price = float(update['order']['executionPrice'])
								pos.close_time = float(update['order']['utcLastUpdateTimestamp']) / 1000

								del self.positions[i]

								if update['order'].get('limitPrice'):
									tp_dist = abs(float(update['order']['executionPrice']) - float(update['order']['limitPrice']))
								else:
									tp_dist = None

								if update['order'].get('stopPrice'):
									sl_dist = abs(float(update['order']['executionPrice']) - float(update['order']['stopPrice']))
								else:
									sl_dist = None

								if sl_dist is None:
									order_type = tl.TAKE_PROFIT
								elif tp_dist is None:
									order_type = tl.STOP_LOSS
								elif tp_dist < sl_dist:
									order_type = tl.TAKE_PROFIT
								else:
									order_type = tl.STOP_LOSS

								result.update({
									ref_id: {
										'timestamp': pos.close_time,
										'type': order_type,
										'accepted': True,
										'item': pos
									}
								})
							else:
								# Fully Closed
								if float(update['position']['tradeData']['volume']) == 0:
									pos.close_price = float(update['order']['executionPrice'])
									pos.close_time = float(update['order']['utcLastUpdateTimestamp']) / 1000

									del self.positions[i]

									result.update({
										ref_id: {
											'timestamp': pos.close_time,
											'type': tl.POSITION_CLOSE,
											'accepted': True,
											'item': pos
										}
									})

									self._handled_position_events[tl.POSITION_CLOSE][pos.order_id] = {
										ref_id: {
											'timestamp': pos.close_time,
											'type': tl.POSITION_CLOSE,
											'accepted': True,
											'item': pos
										}
									}

								# Partially Closed
								else:
									pos.lotsize -= self._convert_from_sw_lotsize(float(update['order']['executedVolume']))

									del_pos = tl.Position.fromDict(self, pos)
									del_pos.lotsize = self._convert_from_sw_lotsize(float(update['order']['executedVolume']))
									del_pos.close_price = float(update['order']['executionPrice'])
									del_pos.close_time = float(update['order']['utcLastUpdateTimestamp']) / 1000

									result.update({
										ref_id: {
											'timestamp': del_pos.close_time,
											'type': tl.POSITION_CLOSE,
											'accepted': True,
											'item': del_pos
										}
									})

									self._handled_position_events[tl.POSITION_CLOSE][del_pos.order_id] = {
										ref_id: {
											'timestamp': del_pos.close_time,
											'type': tl.POSITION_CLOSE,
											'accepted': True,
											'item': del_pos
										}
									}

							break
				else:
					pos_order = None
					order_type = tl.MARKET_ENTRY
					for i in range(len(self.orders)):
						order = self.orders[i]
						if str(update['order']['orderId']) == order.order_id:
							pos_order = order
							order.close_time = float(update['order']['utcLastUpdateTimestamp']) / 1000
							if order.order_type == tl.STOP_ORDER:
								order_type = tl.STOP_ENTRY
							elif order.order_type == tl.LIMIT_ORDER:
								order_type = tl.LIMIT_ENTRY
							del self.orders[i]

							self.handleOnTrade(
								account_id,
								{
									self.generateReference(): {
										'timestamp': order.close_time,
										'type': tl.ORDER_CANCEL,
										'accepted': True,
										'item': order
									}
								}
							)
							break

					# Create
					new_pos = self.convert_sw_position(account_id, update['position'])
					new_pos.setOrder(pos_order)
					if pos_order is not None:
						new_pos.handled_check = False
					else:
						new_pos.handled_check = True

					self.positions.append(new_pos)

					result.update({
						ref_id: {
							'timestamp': new_pos.open_time,
							'type': order_type,
							'accepted': True,
							'item': new_pos
						}
					})

					if order_type == tl.MARKET_ENTRY:
						self._handled_position_events[tl.MARKET_ENTRY][new_pos.order_id] = {
							ref_id: {
								'timestamp': new_pos.open_time,
								'type': order_type,
								'accepted': True,
								'item': new_pos
							}
						}

			# ORDER_ACCEPTED
			elif execution_type == 'ORDER_ACCEPTED':
				# Check if `STOP` or `LIMIT`
				if update['order']['orderType'] in ('LIMIT','STOP'):
					new_order = self.convert_sw_order(account_id, update['order'])
					self.orders.append(new_order)

					result.update({
						ref_id: {
							'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
							'type': new_order.order_type,
							'accepted': True,
							'item': new_order
						}
					})

				# Check if `STOP_LOSS_TAKE_PROFIT`
				elif update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
					for i in range(len(self.positions)):
						pos = self.positions[i]
						if str(update['position']['positionId']) == pos.order_id:
							new_sl = None if update['position'].get('stopLoss') is None else round(float(update['position']['stopLoss']), 5)
							new_tp = None if update['position'].get('takeProfit') is None else round(float(update['position']['takeProfit']), 5)

							if not pos.handled_check and pos.order is not None:
								pos.handled_check = True

								if pos.order.sl != new_sl or pos.order.tp != new_tp:
									Thread(target=pos.close, kwargs={'override': True}).start()
									print(f'ORDER NOT FULFILLED CORRECTLY, CLOSING POSITION: {pos.order_id}')
									return

							pos.sl = new_sl
							pos.tp = new_tp

							result.update({
								ref_id: {
									'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': pos
								}
							})

							break

			# ORDER_CANCELLED
			elif execution_type == 'ORDER_CANCELLED':
				# Check if `STOP` or `LIMIT`
				if update['order']['orderType'] in ('LIMIT','STOP'):
					# Update current order
					new_order = self.convert_sw_order(account_id, update['order'])
					for i in range(len(self.orders)):
						order = self.orders[i]
						if str(update['order']['orderId']) == order.order_id:
							order.close_time = float(update['order']['utcLastUpdateTimestamp']) / 1000

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
				elif update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
					for pos in self.positions:
						if str(update['position']['positionId']) == pos.order_id:
							new_sl = None if update['position'].get('stopLoss') is None else float(update['position']['stopLoss'])
							pos.sl = new_sl
							new_tp = None if update['position'].get('takeProfit') is None else float(update['position']['takeProfit'])
							pos.tp = new_tp

							result.update({
								ref_id: {
									'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': pos
								}
							})

							break

			# ORDER_REPLACED
			elif execution_type == 'ORDER_REPLACED':
				# Check if `STOP` or `LIMIT`
				if update['order']['orderType'] in ('LIMIT','STOP'):
					# Update current order
					new_order = self.convert_sw_order(account_id, update['order'])
					for order in self.orders:
						if str(update['order']['orderId']) == order.order_id:
							order.update(new_order)

							result.update({
								ref_id: {
									'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': order
								}
							})

				# Check if `STOP_LOSS_TAKE_PROFIT`
				elif update['order']['orderType'] == 'STOP_LOSS_TAKE_PROFIT':
					# Update current position
					for pos in self.positions:
						if str(update['position']['positionId']) == pos.order_id:
							new_sl = None if update['position'].get('stopLoss') is None else float(update['position']['stopLoss'])
							pos.sl = new_sl
							new_tp = None if update['position'].get('takeProfit') is None else float(update['position']['takeProfit'])
							pos.tp = new_tp

							result.update({
								ref_id: {
									'timestamp': float(update['order']['utcLastUpdateTimestamp'])/1000,
									'type': tl.MODIFY,
									'accepted': True,
									'item': pos
								}
							})

			# ORDER_REJECTED
			elif execution_type == 'ORDER_REJECTED':
				error_code = update.get('errorCode')
				print(f'ORDER REJECTED: {error_code}')


				position_id = str(update['position']['positionId'])
				for i in range(len(self.positions)):
					pos = self.positions[i]
					if pos.order_id == position_id:
						del pos[i]

						pos.close_time = time.time()
						result.update({
							ref_id: {
								'timestamp': time.time(),
								'type': tl.POSITION_CLOSE,
								'accepted': True,
								'item': pos
							}
						})

						print(f'REJECTING POSITION: {position_id}')

						break

				order_id = str(update['order']['orderId'])
				for i in range(len(self.orders)):
					order = self.orders[i]
					if order.order_id == order_id:
						del order[i]

						order.close_time = time.time()
						result.update({
							ref_id: {
								'timestamp': time.time(),
								'type': tl.ORDER_CANCEL,
								'accepted': True,
								'item': order
							}
						})

						print(f'REJECTING ORDER: {order_id}')

						break


			if len(result):
				print(f'SEND IT: {result}', flush=True)
				self._handled[msg_id] = result
				self.handleOnTrade(account_id, result)
				return result
			else:
				return None



	def _on_account_update(self, payload_type, account_id, update, ref_id):
		self._account_update_queue.append((payload_type, account_id, update, ref_id))


	def _subscribe_chart_updates(self, instrument, listener):
		# ref_id = self.generateReference()

		# product = self._convert_product('Spotware', product)
		# self.parent._subscriptions[str(product)] = listener

		# sub_req = o2.ProtoOASubscribeSpotsReq(
		# 	ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 	symbolId=[product]
		# )
		# self._get_client(list(self.accounts.keys())[0]).send(sub_req, msgid=ref_id)
		# self.parent._wait(ref_id)

		# # sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
		# # 	ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# # 	symbolId=product, period=1
		# # )
		# # self._get_client(list(self.accounts.keys())[0]).send(sub_req)

		# for i in range(14):
		# 	if i % 5 == 0:
		# 		time.sleep(1)

		# 	sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
		# 		ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 		symbolId=product, period=i+1
		# 	)
		# 	self._get_client(list(self.accounts.keys())[0]).send(sub_req)

		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(self.name, self.brokerId, '_subscribe_chart_updates', stream_id, instrument)
		self.ctrl.addBrokerListener(stream_id, listener)


	def _subscribe_multiple_chart_updates(self, products, listener):
		ref_id = self.generateReference()

		products = [self._convert_product(i) for i in products]
		for product in products:
			self.parent._subscriptions[str(product)] = listener

		sub_req = o2.ProtoOASubscribeSpotsReq(
			ctidTraderAccountId=int(list(self.accounts.keys())[0]),
			symbolId=products
		)
		self._get_client(list(self.accounts.keys())[0]).send(sub_req, msgid=ref_id)
		self.parent._wait(ref_id)

		# sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
		# 	ctidTraderAccountId=int(list(self.accounts.keys())[0]),
		# 	symbolId=products, period=1
		# )
		# self._get_client(list(self.accounts.keys())[0]).send(sub_req)

		for i in range(14):
			if i % 5 == 0:
				time.sleep(1)

			sub_req = o2.ProtoOASubscribeLiveTrendbarReq(
				ctidTraderAccountId=int(list(self.accounts.keys())[0]),
				symbolId=products, period=i+1
			)
			self.client.send(sub_req)


	def _handle_chart_update(self, chart, payload):
		result = []

		if 'ask' in payload:
			ask = float(payload['ask']) / 100000
		else:
			ask = None

		if 'bid' in payload:
			bid = float(payload['bid']) / 100000
		else:
			bid = None

		volume = None
		c_ts = time.time()+self.time_off
		# Iterate periods
		if tl.period.TICK in chart.getActivePeriods():
			if ask:
				chart.ask[tl.period.TICK] = ask
			if bid:
				chart.bid[tl.period.TICK] = bid
			if chart.bid[tl.period.TICK] is not None and chart.ask[tl.period.TICK] is not None:
				chart.mid[tl.period.TICK] = np.around((chart.ask[tl.period.TICK] + chart.bid[tl.period.TICK])/2, decimals=5)

			result.append({
				'broker': self.name,
				'product': chart.product,
				'period': tl.period.TICK,
				'bar_end': False,
				'timestamp': time.time()+self.time_off,
				'item': {
					'ask': chart.ask[tl.period.TICK],
					'mid': chart.mid[tl.period.TICK],
					'bid': chart.bid[tl.period.TICK]
				}
			})

		if 'trendbar' in payload:
			for i in payload['trendbar']:
				period = self._convert_sw_period(i['period'])
				if period in chart.getActivePeriods():
					if (isinstance(chart.bid.get(period), np.ndarray) and 
						isinstance(chart.ask.get(period), np.ndarray)):

						bar_ts = float(i['utcTimestampInMinutes'])*60
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
							print(f'[SW] ({period}) Next: {chart.lastTs[period]}')

						new_low = float(i['low']) / 100000
						new_open = (float(i['low']) + float(i['deltaOpen'])) / 100000
						new_high = (float(i['low']) + float(i['deltaHigh'])) / 100000
						new_close = chart.mid[tl.period.TICK]
						new_ohlc = np.array([new_open, new_high, new_low, new_close], dtype=np.float64)

						chart.ask[period] = new_ohlc
						chart.mid[period] = new_ohlc
						chart.bid[period] = new_ohlc

						result.append({
							'broker': self.name,
							'product': chart.product,
							'period': period,
							'bar_end': False,
							'timestamp': chart.lastTs[period],
							'item': {
								'ask': chart.ask[period].tolist(),
								'mid': chart.mid[period].tolist(),
								'bid': chart.bid[period].tolist()
							}
						})

		if len(result):
			chart.handleTick(result)


	def _handle_chart_auto_bar_end(self):
		for chart in self.charts:
			result = []
			c_ts = time.time()+self.time_off-1
			for period in chart.getActivePeriods():
				if period != tl.period.TICK and chart.volume[period] > 0:
					# Handle period bar end
					is_new_bar = chart.isNewBar(period, c_ts)
					if is_new_bar:
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
						chart.lastTs[period] = tl.getNextTimestamp(period, chart.lastTs[period], now=c_ts - tl.period.getPeriodOffsetSeconds(period))
						print(f'[SW] ({period}) Next: {chart.lastTs[period]}')
						chart.ask[period] = np.array([chart.ask[period][3]]*4, dtype=np.float64)
						chart.bid[period] = np.array([chart.bid[period][3]]*4, dtype=np.float64)
						chart.mid[period] = np.array(
							[np.around(
								(chart.ask[period][3] + chart.bid[period][3])/2,
								decimals=5
							)]*4, 
						dtype=np.float64)

			if len(result):
				chart.handleTick(result)



	def onChartUpdate(self, chart, payload):
		self._price_queue.append((chart, payload))


	def _convert_product(self, broker_name, product):
		if product == 'BTC_USD':
			product = 'BTC/USD'

		return int(self._get_symbol_by_name(broker_name, product.replace('_', ''))['symbolId'])


	def _convert_sw_product(self, product):
		if product == 2:
			return tl.product.GBPUSD
		elif product == 1:
			return tl.product.EURUSD


	def _get_asset(self, broker_name, asset_id):
		return self.parent.assets[broker_name][str(asset_id)]


	def _get_asset_by_name(self, broker_name, asset_name):
		return self.parent.assets_by_name[broker_name][asset_name]


	def _get_symbol(self, broker_name, symbol_id):
		return self.parent.symbols[broker_name][str(symbol_id)]


	def _get_symbol_by_name(self, broker_name, symbol_name):
		return self.parent.symbols_by_name[broker_name][symbol_name]


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


	def _convert_to_sw_lotsize(self, lotsize):
		return round((round(lotsize, 2) * 10000000) / 100000) * 100000


	def _convert_from_sw_lotsize(self, lotsize):
		return round(lotsize / 10000000, 2)


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
		if period == 'M1':
			return tl.period.ONE_MINUTE
		elif period == 'M2':
			return tl.period.TWO_MINUTES
		elif period == 'M3':
			return tl.period.THREE_MINUTES
		elif period == 'M4':
			return tl.period.FOUR_MINUTES
		elif period == 'M5':
			return tl.period.FIVE_MINUTES
		elif period == 'M10':
			return tl.period.TEN_MINUTES
		elif period == 'M15':
			return tl.period.FIFTEEN_MINUTES
		elif period == 'M30':
			return tl.period.THIRTY_MINUTES
		elif period == 'H1':
			return tl.period.ONE_HOUR
		elif period == 'H4':
			return tl.period.FOUR_HOURS
		elif period == 'H12':
			return tl.period.TWELVE_HOURS
		elif period == 'D1':
			return tl.period.DAILY
		elif period == 'W1':
			return tl.period.WEEKLY
		elif period == 'MN1':
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


	def deleteChild(self):
		res = self.ctrl.brokerRequest(
			'spotware', self.brokerId, 'deleteChild', self.brokerId
		)

	# TESTING
	def disconnectBroker(self):
		res = self.ctrl.brokerRequest(
			'spotware', self.brokerId, 'disconnectBroker'
		)

		print(f"[disconnectBroker] {res}")
