import pandas as pd
import numpy as np
import time
import math
import traceback
import ntplib
from threading import Thread
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required


ONE_HOUR = 60*60

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
		self.is_connected = False

		self._price_queue = []
		self.time_off = 0
		self._set_time_off()

		self.is_auth = self._add_user()


		if not is_dummy and not is_parent:
			Thread(target=self._handle_account_updates).start()
			self.subscribeAccountUpdates()
			# for account_id in self.getAccounts():
			# 	if account_id != tl.broker.PAPERTRADER_NAME:
			# 		self._subscribe_account_updates(account_id)

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

		print('FXOPEN INIT 1')
		# if is_parent:
		# 	Thread(target=self._handle_price_updates).start()
		# 	# Load Charts
		# 	CHARTS = ['EUR_USD']
		# 	print("CREATE CHARTS")
		# 	for instrument in CHARTS:
		# 		chart = self.createChart(instrument, await_completion=True)
		# 		# self.data_saver.subscribe(chart, PERIODS)
		# 	print("CREATE CHARTS DONE")

		if not is_dummy:
			Thread(target=self._periodic_check).start()


	def _periodic_check(self):
		WAIT_PERIOD = 60
		# Send ping to server to check connection status
		while self.is_running:
			try:
				self._last_update = time.time()

				res = self.ctrl.brokerRequest(
					'fxopen', self.brokerId, 'heartbeat'
				)

				print(f"[FXOpen] {res}")
				if "result" in res and not res["result"]:
					self.reauthorize_accounts()

			except Exception as e:
				print(traceback.format_exc())

			time.sleep(WAIT_PERIOD)

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
			user_id, self.strategyId, self.brokerId, self._key, self._web_api_id, self._web_api_secret, self._is_demo, self.accounts,
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


	def _set_time_off(self):
		try:
			client = ntplib.NTPClient()
			response = client.request('pool.ntp.org')
			self.time_off = response.tx_time - time.time()
		except Exception:
			pass


	def reauthorize_accounts(self):
		print("[FXOpen] Reauthorizing Accounts...")
		self.is_auth = self._add_user()
		self.subscribeAccountUpdates()
		if self.is_auth:
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

			if self.is_parent:
				CHARTS = ['EUR_USD']
				for instrument in CHARTS:
					print(f'LOADING {instrument}')
					chart = self.getChart(instrument)
					chart.start(True)


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
			result = pd.DataFrame.from_dict(res, dtype=float)
			result.index = result.index.astype(int)

		return result


	def _convert_fxo_position(self, account_id, pos):
		order_id = str(pos.get('Id'))
		instrument = self.convertFromFXOInstrument(pos.get('Symbol'))
		direction = tl.LONG if pos.get("Side") == "Buy" else tl.SHORT

		if pos.get('RemainingAmount'):
			lotsize = self.convertToLotsize(pos.get('RemainingAmount'))
		else:
			lotsize = self.convertToLotsize(pos.get('FilledAmount'))

		if pos.get('StopPrice'):
			entry_price = float(pos.get('StopPrice'))
		else:
			entry_price = float(pos.get('Price'))
			
		sl = None
		if pos.get('StopLoss'):
			sl = pos['StopLoss']
		tp = None
		if pos.get('TakeProfit'):
			tp = pos['TakeProfit']
		open_time = pos.get('Filled')/1000

		if pos["InitialType"] == "Stop":
			order_type = tl.STOP_ENTRY
		elif pos["InitialType"] == "Limit":
			order_type = tl.LIMIT_ENTRY
		else:
			order_type = tl.MARKET_ENTRY

		return tl.Position(
			self,
			order_id, str(account_id), instrument,
			order_type, direction, lotsize,
			entry_price, sl, tp, open_time
		)

	
	def _convert_fxo_order(self, account_id, order):
		if order.get("Type") == "Limit":
			order_type = tl.LIMIT_ORDER
		elif order.get("Type") == "Stop":
			order_type = tl.STOP_ORDER
		else:
			order_type = tl.MARKET_ORDER

		order_id = str(order.get('Id'))
		instrument = self.convertFromFXOInstrument(order.get('Symbol'))
		direction = tl.LONG if order.get("Side") == "Buy" else tl.SHORT
		lotsize = self.convertToLotsize(order.get('RemainingAmount'))

		if order.get('Price') is not None:
			entry_price = float(order.get('Price'))
		else:
			entry_price = float(order.get('StopPrice'))

		sl = None
		if order.get('StopLoss'):
			sl = order['StopLoss']
		tp = None
		if order.get('TakeProfit'):
			tp = order['TakeProfit']
		open_time = order.get('Modified')/1000

		return tl.Order(
			self,
			order_id, str(account_id), instrument,
			order_type, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def _handle_order_create(self, trade):

		account_id = str(trade["AccountId"])

		result = {}
		client_id = trade.get("ClientId")
		check_order = self.getOrderByID(str(trade["Id"]))
		if check_order is None:
			order = self._convert_fxo_order(account_id, trade)

			self.appendDbOrder(order)

			result[self.generateReference()] = {
				'timestamp': order["open_time"],
				'type': order["order_type"],
				'accepted': True,
				'item': order
			}
			print(f"[_handle_order_create] 4: {result}")

			if client_id is not None:
				# self._handled["ordercreate_" + client_id] = result
				self.addHandledItem("ordercreate_" + client_id, result)

		return result


	def _handle_order_fill_close(self, trade):

		account_id = str(trade["AccountId"])

		# Delete any existing order reference
		from_order = self.getOrderByID(str(trade["Id"]))
		if from_order is not None:
			self.deleteDbOrder(from_order["order_id"])

			self.handleOnTrade(account_id, {
				self.generateReference(): {
					'timestamp': from_order["close_time"],
					'type': tl.ORDER_CANCEL,
					'accepted': True,
					'item': from_order
				}
			})

		result = {}
		client_id = trade.get("ClientId")
		# Closed Position
		pos = self.getPositionByID(str(trade["Id"]))
		if pos is not None:
			size = pos["lotsize"] - self.convertToLotsize(trade["RemainingAmount"])

			if size >= pos["lotsize"]:
				if trade.get("Price"):
					pos["close_price"] = trade["Price"]
				else:
					pos["close_price"] = trade["StopPrice"]

				pos["close_time"] = trade["Modified"] / 1000

				comment = trade.get("Comment")
				if comment is not None and "TP" in comment:
					order_type = tl.TAKE_PROFIT
				elif comment is not None and "SL" in comment:
					order_type = tl.STOP_LOSS
				else:
					order_type = tl.POSITION_CLOSE

				result[self.generateReference()] = {
					'timestamp': pos["close_price"],
					'type': order_type,
					'accepted': True,
					'item': pos
				}
				self.deleteDbPosition(pos["order_id"])
			
			else:
				cpy = tl.Position.fromDict(self, pos)
				cpy.lotsize = size

				if trade.get("Price"):
					cpy.close_price = trade["Price"]
				else:
					cpy.close_price = trade["StopPrice"]

				cpy.close_time = trade["Modified"] / 1000

				# Modify open position
				pos["lotsize"] = self.convertToLotsize(trade["RemainingAmount"])

				self.replaceDbPosition(pos)

				result[self.generateReference()] = {
					'timestamp': cpy.close_price,
					'type': tl.POSITION_CLOSE,
					'accepted': True,
					'item': cpy
				}
			
			if client_id is not None:
				# self._handled["fillclose_" + client_id] = result
				self.addHandledItem("fillclose_" + client_id, result)
		
		return result


	def _handle_order_fill_open(self, trade):
		
		print(f"[_handle_order_fill_open] {trade}")

		account_id = str(trade["AccountId"])

		# Delete any existing order reference
		from_order = self.getOrderByID(str(trade["Id"]))
		if from_order is not None:
			self.deleteDbOrder(from_order["order_id"])

			self.handleOnTrade(account_id, {
				self.generateReference(): {
					'timestamp': from_order["close_time"],
					'type': tl.ORDER_CANCEL,
					'accepted': True,
					'item': from_order
				}
			})

		result = {}
		client_id = trade.get("ClientId")
		# Closed Position
		
		check_pos = self.getPositionByID(str(trade["Id"]))
		if check_pos is None:
			pos = self._convert_fxo_position(account_id, trade)
			self.appendDbPosition(pos)

			result[self.generateReference()] = {
				'timestamp': pos["open_time"],
				'type': pos["order_type"],
				'accepted': True,
				'item': pos
			}

			if client_id is not None:
				# self._handled["fillopen_" + client_id] = result
				self.addHandledItem("fillopen_" + client_id, result)
	
		print(f"[_handle_order_fill_open] {result}")
		print(f"[_handle_order_fill_open] {self._handled}")
	
		return result


	def _handle_order_cancel(self, trade):

		result = {}
		client_id = trade.get("ClientId")
		order = self.getOrderByID(str(trade["Id"]))
		if order is not None:
			order["close_time"] = trade["Modified"] / 1000
			self.deleteDbOrder(order["order_id"])

			result[self.generateReference()] = {
				'timestamp': order["close_time"],
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': order
			}

			if client_id is not None:
				# self._handled["ordercancel_" + client_id] = result
				self.addHandledItem("ordercancel_" + client_id, result)

		return result


	def _handle_modify(self, trade):
		
		result = {}
		client_id = trade.get("ClientId")
		
		if trade["Type"] == "Position":
			pos = self.getPositionByID(str(trade["Id"]))
			if pos is not None:
				pos["sl"] = trade.get("StopLoss")
				pos["tp"] = trade.get("TakeProfit")

				self.replaceDbPosition(pos)

				result[self.generateReference()] = {
					'timestamp': trade["Modified"] / 1000,
					'type': tl.MODIFY,
					'accepted': True,
					'item': pos
				}

				if client_id is not None:
					# self._handled["modify_" + client_id] = result
					self.addHandledItem("modify_" + client_id, result)

		else:
			order = self.getOrderByID(str(trade["Id"]))
			if order is not None:
				order["sl"] = trade.get("StopLoss")
				order["tp"] = trade.get("TakeProfit")
				order["lotsize"] = self.convertToLotsize(trade["RemainingAmount"])

				if "StopPrice" in trade:
					order["entry_price"] = trade["StopPrice"]
				else:
					order["entry_price"] = trade["Price"]

				self.replaceDbOrder(order)

				result[self.generateReference()] = {
					'timestamp': trade["Modified"] / 1000,
					'type': tl.MODIFY,
					'accepted': True,
					'item': order
				}

				if client_id is not None:
					# self._handled["modify_" + client_id] = result
					self.addHandledItem("modify_" + client_id, result)

		return result

	
	def _handle_trades(self, trades):
		new_positions = []
		new_orders = []

		for i in trades:
			account_id = i["AccountId"]
			if i.get("Type") == "Position":
				new_pos = self._convert_fxo_position(account_id, i)
				new_positions.append(new_pos)
			elif i.get("Type") == "Limit" or i.get("Type") == "Stop":
				new_order = self._convert_fxo_order(account_id, i)
				new_orders.append(new_order)

		self.setDbPositions(new_positions)
		self.setDbOrders(new_orders)

		return {
			self.generateReference(): {
				'timestamp': time.time(),
				'type': tl.UPDATE,
				'accepted': True,
				'item': {
					"positions": self.positions,
					"orders": self.orders
				}
			}
		}


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

		print(f"[FXOpen.createPosition] ({status_code}) {res}")

		result = {}
		# status_code = res.status_code
		# res = res.json()
		if 200 <= status_code < 300:
			result.update(self._wait(
				"fillopen_" + str(res.get("ClientId")),
				self._handle_order_fill_open, 
				res
			))
			print(f"[FXOpen.createPosition] -> {result}")

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
					'message': 'FXOpen internal server error.'
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
			result.update(self._wait(
				"modify_" + str(res.get("ClientId")),
				self._handle_modify, 
				res
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
						'order_id': pos["order_id"]
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': 'FXOpen internal server error.',
					'item': {
						'order_id': pos["order_id"]
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
			result.update(self._wait(
				"fillclose_" + str(res["Trade"].get("ClientId")),
				self._handle_order_fill_close, 
				res["Trade"]
			))

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
						'order_id': pos["order_id"]
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': 'FXOpen internal server error.',
					'item': {
						'order_id': pos["order_id"]
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
			result.update(self._wait(
				"ordercreate_" + str(res.get("ClientId")),
				self._handle_order_create, 
				res
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
					'message': 'FXOpen internal server error.'
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
			result.update(self._wait(
				"modify_" + str(res.get("ClientId")),
				self._handle_modify, 
				res
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
						'order_id': order["order_id"]
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': 'FXOpen internal server error.',
					'item': {
						'order_id': order["order_id"]
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
			result.update(self._wait(
				"ordercancel_" + str(res["Trade"].get("ClientId")),
				self._handle_modify, 
				res["Trade"]
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
						'order_id': order["order_id"]
					}
				}
			})
		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.ORDER_CANCEL,
					'accepted': False,
					'message': 'FXOpen internal server error.',
					'item': {
						'order_id': order["order_id"]
					}
				}
			})

		return result


	def _subscribe_chart_updates(self, instrument, listener):
		print(f'SUBSCRIBE')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'fxopen', self.brokerId, 'subscribe_price_updates', 
			stream_id, instrument
		)
		self.ctrl.addBrokerListener(stream_id, listener)


	def _handle_price_updates(self):
		time_off_timer = time.time()

		while True:
			result = []
			if len(self._price_queue):
				chart, timestamp, ask, bid, volume = self._price_queue[0]
				del self._price_queue[0]

				if timestamp is not None:
					# Convert time to datetime
					c_ts = timestamp

					# Iterate periods
					for period in chart.getActivePeriods():
						if (isinstance(chart.bid.get(period), np.ndarray) and 
							isinstance(chart.ask.get(period), np.ndarray)):

							# Handle period bar end
							if period != tl.period.TICK:
								is_new_bar = chart.isNewBar(period, c_ts)
								if is_new_bar:
									print(f'NEW BAR: {chart.volume[period]}', flush=True)
									if chart.volume[period] > 0:
										print(f'ADD NEW BAR 1: {period}', flush=True)
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
									print(f'[FXOpen] ({period}) Prev: {chart.lastTs[period]}, Next: {chart.lastTs[period]}')
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
								'timestamp': max(c_ts, chart.lastTs[period]),
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

			else:
				for chart in self.charts:
					c_ts = time.time()+self.time_off-1
					for period in chart.getActivePeriods():
						if period != tl.period.TICK and chart.volume[period] > 0:
							# Handle period bar end
							is_new_bar = chart.isNewBar(period, c_ts)
							if is_new_bar:
								print(f'ADD NEW BAR 2: {period}', flush=True)
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
								print(f'[FXOpen] ({period}) Prev: {chart.lastTs[period]}, Next: {chart.lastTs[period]}')
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


			if time.time() - time_off_timer > ONE_HOUR:
				time_off_timer = time.time()
				self._set_time_off()

			time.sleep(0.01)


	def onChartUpdate(self, *args):
		self._price_queue.append(args)


	def subscribeAccountUpdates(self):
		print(f'SUBSCRIBE')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'fxopen', self.brokerId, 'subscribe_account_updates', stream_id
		)
		stream_id = res
		print(f"[FXOpen.subscribeAccountUpdates] {stream_id}")
		self.ctrl.addBrokerListener(stream_id, self._on_account_update)


	def _on_account_update(self, update, account_id, handled_id):
		self._account_update_queue.append((update, account_id, handled_id))


	def _handle_account_updates(self):
		while True:
			if len(self._account_update_queue):
				update, account_id, handled_id = self._account_update_queue[0]
				del self._account_update_queue[0]

				print(f"HANDLED ID: {handled_id}")
				try:
					if handled_id is not None:
						print(F"[FXOpen._handle_account_updates] HANDLED 1: {handled_id}, {update}")
						self.addHandledItem(handled_id, update)
						print(F"[FXOpen._handle_account_updates] HANDLED 2: {self.getHandled(handled_id)}")
						# self._handled[handled_id] = update

					if len(update):
						self.handleOnTrade(account_id, update)
				except Exception:
					print(f"[_handle_account_updates] {traceback.format_exc()}")
			time.sleep(0.1)

			
	# def _handle_account_updates(self):
		
	# 	while True:
	# 		if len(self._account_update_queue):
	# 			update = self._account_update_queue[0]
	# 			del self._account_update_queue[0]
				
	# 			try:
	# 				item = update.get("Result")
	# 				result = {}
	# 				account_id = None

	# 				if item is not None:
	# 					event = item.get("Event")
	# 					# On Filled Event
	# 					if event == "Filled":
	# 						# Position Updates
	# 						account_id = str(item["Trade"]["AccountId"])
	# 						if "Profit" in item:
	# 							item["Trade"]["Price"] = item["Fill"]["Price"]
	# 							result = self._handle_order_fill_close(item["Trade"])
	# 						else:
	# 							result = self._handle_order_fill_open(item["Trade"])
					
	# 					# On Allocated Event
	# 					elif event == "Allocated":
	# 						if item["Trade"]["Type"] in ("Stop","Limit"):
	# 							account_id = str(item["Trade"]["AccountId"])
	# 							result = self._handle_order_create(item["Trade"])

	# 					# On Canceled Event
	# 					elif event == "Canceled":
	# 						account_id = str(item["Trade"]["AccountId"])
	# 						result = self._handle_order_cancel(item["Trade"])

	# 					# On Modified Event
	# 					elif event == "Modified":
	# 						account_id = str(item["Trade"]["AccountId"])
	# 						result = self._handle_modify(item["Trade"])

	# 					elif "Trades" in item:
	# 						result = self._handle_trades(item["Trades"])

	# 					if event is not None:
	# 						print(f"[FXOpen._handle_account_updates] {update}")

	# 				if len(result):
	# 					self.handleOnTrade(account_id, result)
	# 			except Exception:
	# 				print(f"[_handle_account_updates] {traceback.format_exc()}")

				


	def convertFromFXOInstrument(self, instrument):
		if instrument == "EURUSD":
			return "EUR_USD"
		else:
			return instrument


	def convertToFXOInstrument(self, instrument):
		if instrument == "EUR_USD":
			return "EURUSD"
		else:
			return instrument


	def convertToLotsize(self, size):
		return size / 100000


	def convertToUnitSize(self, size):
		return int(size * 100000)


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

	# TESTING
	def disconnectBroker(self):
		res = self.ctrl.brokerRequest(
			'fxopen', self.brokerId, 'disconnectBroker'
		)

		print(f"[disconnectBroker] {res}")
