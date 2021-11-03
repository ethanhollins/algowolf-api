import pandas as pd
import numpy as np
import time
import traceback
from threading import Thread
from app import tradelib as tl
from app.tradelib.broker import Broker


ONE_HOUR = 60*60

class LoadTest(Broker):

	def __init__(self, 
		ctrl, user_account=None, strategy_id=None, broker_id=None, 
		accounts={}, display_name=None, is_dummy=False, is_parent=False
	):
		super().__init__(ctrl, user_account, strategy_id, broker_id, "loadtest", accounts, display_name, is_dummy, True)
		
		print(f'LOADTEST INIT: {strategy_id}, {broker_id}', flush=True)

		self.dl = tl.DataLoader(broker=self)
		self.data_saver = tl.DataSaver(broker=self)

		self.is_dummy = is_dummy
		self.is_parent = is_parent

		self._last_update = time.time()
		self._subscriptions = []
		self._account_update_queue = []
		self.is_connected = False

		self._price_queue = []

		self.is_auth = self._add_user()

		if not is_dummy and not is_parent:
			Thread(target=self._handle_account_updates).start()
			self.subscribeAccountUpdates()

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

		if not is_dummy:
			Thread(target=self._periodic_check).start()


	def _periodic_check(self):
		WAIT_PERIOD = 60
		# Send ping to server to check connection status
		while self.is_running:
			try:
				self._last_update = time.time()

				res = self.ctrl.brokerRequest(
					'loadtest', self.brokerId, 'heartbeat'
				)

				print(f"[LoadTest] {res}")

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
			'loadtest', self.brokerId, 'add_user',
			user_id, self.strategyId, self.brokerId, is_parent=self.is_parent
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
		
		return self._create_empty_df(period)


	
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
		# print(f"[_handle_order_fill_open] {self._handled}")
	
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

		# self.positions = new_positions
		# self.orders = new_orders

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


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price,
		override=False
	):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createPosition',
			product, lotsize, direction,
			account_id, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		print(f"[LoadTest.createPosition] ({status_code}) {res}")

		result = {}
		result.update(self._wait(
			"fillopen_" + str(res.get("ClientId")),
			self._handle_order_fill_open, 
			res
		))
		return result


	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyPosition',
			pos, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		result.update(self._wait(
			"modify_" + str(res.get("ClientId")),
			self._handle_modify, 
			res
		))
		return result


	def deletePosition(self, pos, lotsize, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deletePosition',
			pos, lotsize
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		result.update(self._wait(
			"fillclose_" + str(res["Trade"].get("ClientId")),
			self._handle_order_fill_close, 
			res["Trade"]
		))
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
		result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'getAccountInfo',
			account_id
		)

		return result

	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price,
		override=False
	):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'createOrder',
			product, lotsize, direction,
			account_id, order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		result.update(self._wait(
			"ordercreate_" + str(res.get("ClientId")),
			self._handle_order_create, 
			res
		))

		return result

	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'modifyOrder',
			order, lotsize, entry_price, sl_price, tp_price
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		result.update(self._wait(
			"modify_" + str(res.get("ClientId")),
			self._handle_modify, 
			res
		))

		return result

	def deleteOrder(self, order, override=False):
		broker_result = self.ctrl.brokerRequest(
			self.name, self.brokerId, 'deleteOrder',
			order
		)

		status_code = broker_result.get('status')
		res = broker_result.get('result')

		result = {}
		result.update(self._wait(
			"ordercancel_" + str(res["Trade"].get("ClientId")),
			self._handle_modify, 
			res["Trade"]
		))

		return result


	def _subscribe_chart_updates(self, instrument, listener):
		print(f'SUBSCRIBE')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'loadtest', self.brokerId, 'subscribe_price_updates', 
			stream_id, instrument
		)
		self.ctrl.addBrokerListener(stream_id, listener)


	def _handle_price_updates(self):
		return


	def onChartUpdate(self, *args):
		return


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


	def subscribeAccountUpdates(self):
		print(f'SUBSCRIBE')
		stream_id = self.generateReference()
		res = self.ctrl.brokerRequest(
			'loadtest', self.brokerId, 'subscribe_account_updates', stream_id
		)
		stream_id = res
		print(f"[LoadTest.subscribeAccountUpdates] {stream_id}")
		self.ctrl.addBrokerListener(stream_id, self._on_account_update)


	def _on_account_update(self, update, account_id, handled_id):
		print(f"[_on_account_update] ({time.time()}) {handled_id}", flush=True)
		self._account_update_queue.append((update, account_id, handled_id))

	def _handle_account_updates(self):
		while True:
			if len(self._account_update_queue):
				update, account_id, handled_id = self._account_update_queue[0]
				del self._account_update_queue[0]
				print(f"[_handle_account_updates] ({time.time()}) {handled_id}", flush=True)

				try:
					if handled_id is not None:
						print(F"[LoadTest._handle_account_updates] HANDLED 1: {handled_id}, {update}", flush=True)
						self.addHandledItem(handled_id, update)
						print(F"[LoadTest._handle_account_updates] HANDLED 2: {self.getHandled(handled_id)}", flush=True)
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
	# 				print(traceback.format_exc())


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

