import time
import math
import numpy as np
import pandas as pd
from app import tradelib as tl
from app.error import OrderException
from copy import copy



class Backtester(object):

	def __init__(self, broker):
		self.broker = broker


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		if direction == tl.LONG:
			price = self.broker.getAsk(product)
		else:
			price = self.broker.getBid(product)

		# Calc stop loss
		sl = None
		if sl_price:
			sl = np.around(sl_price, 5)
		elif sl_range:
			if direction == tl.LONG:
				sl = np.around(price - tl.utils.convertToPrice(sl_range), 5)
			else:
				sl = np.around(price + tl.utils.convertToPrice(sl_range), 5)

		# Calc take profit
		tp = None
		if tp_price:
			tp = np.around(tp_price, 5)
		elif tp_range:
			if direction == tl.LONG:
				tp = np.around(price + tl.utils.convertToPrice(tp_range), 5)
			else:
				tp = np.around(price - tl.utils.convertToPrice(tp_range), 5)

		# Set entry to given price or current price
		entry = None
		if entry_price:
			entry = np.around(entry_price, 5)
		else:
			entry = np.around(price, 5)

		# Create position object
		open_time = math.floor(time.time())

		order_id = self.broker.generateReference()
		order = tl.Position(self.broker,
			order_id, account_id, product, tl.MARKET_ENTRY, direction, 
			lotsize, entry_price=entry, sl=sl, tp=tp,
			open_time=open_time
		)
		# Perform order validation
		self.broker.orderValidation(order)

		# Add to broker position list
		self.broker.positions.append(order)
		
		return order


	def createOrder(self,
		product, lotsize, direction, account_id,
		order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		if direction == tl.LONG:
			price = self.broker.getAsk(product)
		else:
			price = -self.broker.getBid(product)

		# Calc entry
		entry = None
		if entry_price:
			entry = np.around(entry_price, 5)
		elif entry_range:
			if order_type == tl.LIMIT_ORDER:
				entry = np.around(abs(tl.convertToPrice(entry_range) - price), 5)
			elif order_type == tl.STOP_ORDER:
				entry = np.around(abs(tl.convertToPrice(entry_range) + price), 5)
			
		# Calc stop loss
		sl = None
		if sl_price:
			sl = np.around(sl_price, 5)
		elif sl_range:
			if direction == tl.LONG:
				sl = np.around(entry - tl.convertToPrice(sl_range), 5)
			else:
				sl = np.around(entry + tl.convertToPrice(sl_range), 5)

		# Calc take profit
		tp = None
		if tp_price:
			tp = np.around(tp_price, 5)
		elif tp_range:
			if direction == tl.LONG:
				tp = np.around(entry + tl.convertToPrice(tp_range), 5)
			else:
				tp = np.around(entry - tl.convertToPrice(tp_range), 5)

		open_time = math.floor(time.time())

		order_id = self.broker.generateReference()
		order = tl.Order(self.broker,
			order_id, account_id, product, order_type, direction, lotsize,
			entry_price=entry, sl=sl, tp=tp, open_time=open_time
		)

		# Validate order
		self.broker.orderValidation(order)
		# Add to broker order list
		self.broker.orders.append(order)

		return order


	def modifyPosition(self, pos, sl_range=None, tp_range=None, sl_price=None, tp_price=None):
		if sl_range != None:
			if pos.direction == tl.LONG:
				pos.sl = round(pos.entry_price - tl.utils.convertToPrice(sl_range), 5)
			else:
				pos.sl = round(pos.entry_price + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price:
			pos.sl = sl_price

		if tp_range != None:
			if pos.direction == tl.LONG:
				pos.tp = round(pos.entry_price + tl.utils.convertToPrice(tp_range), 5)
			else:
				pos.tp = round(pos.entry_price - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price:
			pos.tp = tp_price

		return pos


	def deletePosition(self, pos, lotsize):
		if lotsize >= pos.lotsize:
			if pos.direction == tl.LONG:
				pos.close_price = self.broker.getBid(pos.product)
			else:
				pos.close_price = self.broker.getAsk(pos.product)
			pos.close_time = self.broker.getTimestamp(tl.product.GBPUSD, tl.period.ONE_MINUTE)

			result = pos

			# Delete position from broker positions
			del self.broker.positions[self.broker.positions.index(pos)]

		elif lotsize <= 0:
			# Raise error
			raise OrderException('Position close size must be greater than 0.')

		else:
			cpy = tl.Position.fromDict(self.broker, pos)
			cpy.lotsize = lotsize
			if pos.direction == tl.LONG:
				cpy.close_price = self.broker.getBid(pos.product)
			else:
				cpy.close_price = self.broker.getAsk(pos.product)
			cpy.close_time =  self.broker.getTimestamp(tl.product.GBPUSD, tl.period.ONE_MINUTE)
			
			result = cpy

			pos.lotsize -= lotsize

		return result


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price):
		if lotsize:
			order.lotsize = lotsize

		# Convert to price
		if entry_price:
			order.entry_price = entry_price

		if sl_price:
			order.sl = sl_price

		if tp_price:
			order.tp = tp_price

		return order


	def deleteOrder(self, order):
		order.close_time = self.broker.getTimestamp(tl.product.GBPUSD, tl.period.ONE_MINUTE)
		del self.broker.orders[self.broker.orders.index(order)]

		return order

	def createOrderPosition(self, order):
		if order.order_type == tl.LIMIT_ORDER:
			order_type = tl.LIMIT_ENTRY
		elif order.order_type == tl.STOP_ORDER:
			order_type = tl.STOP_ENTRY
		else:
			order_type = tl.MARKET_ENTRY

		pos = tl.Position.fromOrder(self.broker, order)
		pos.order_type = order_type
		self.broker.positions.append(pos)

		res = {
			self.broker.generateReference(): {
				'timestamp': math.floor(time.time()),
				'type': pos.order_type,
				'accepted': True,
				'item': pos
			}
		}

		return pos, res


	def handleTransaction(self, res):
		# On Trade
		if self.broker.acceptLive:
			# Delete Strategy Position
			self.broker.userAccount.updateTrades(
				self.broker.strategyId,
				self.broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
				self.broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
			)

		# Update transaction history
		self.broker.handleTransaction(res)


	def handleOrders(self, product, timestamp, ohlc, is_backtest=False):
		# Block any ticks before position check completed
		if not is_backtest and not self.broker.acceptLive:
			return

		ask = ohlc[:4]
		bid = ohlc[4:]

		for order in self.broker.getAllOrders():
			if order.product != product or order.account_id != tl.broker.PAPERTRADER_NAME:
				continue

			if order.order_type == tl.LIMIT_ORDER:
				if order.direction == tl.LONG:
					if ask[2] <= order.entry_price:
						# Enter Order Position LONG
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = timestamp

						# Delete Order
						del self.broker.orders[self.broker.orders.index(order)]

						# On Trade
						if pos and self.broker.acceptLive:
							# Delete Strategy Position
							self.broker.userAccount.updateTrades(
								self.broker.strategyId,
								self.broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
								self.broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
							)

							self.broker.handleOnTrade(order.account_id, res)

						# Update transaction history
						self.broker.handleTransaction(res)

				else:
					if bid[1] >= order.entry_price:
						# Enter Order Position SHORT
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = timestamp
						
						# Delete Order
						del self.broker.orders[self.broker.orders.index(order)]						

						

			elif order.order_type == tl.STOP_ORDER:
				if order.direction == tl.LONG:
					if ask[1] >= order.entry_price:
						# Enter Order Position SHORT
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = timestamp

						# Delete Order
						del self.broker.orders[self.broker.orders.index(order)]

						# On Trade
						if pos and self.broker.acceptLive:
							# Delete Strategy Position
							self.broker.userAccount.updateTrades(
								self.broker.strategyId,
								self.broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
								self.broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
							)

							self.broker.handleOnTrade(order.account_id, res)

						# Update transaction history
						self.broker.handleTransaction(res)

				else:
					if bid[2] <= order.entry_price:
						# Enter Order Position SHORT
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = timestamp

						# Delete Order
						del self.broker.orders[self.broker.orders.index(order)]

						# On Trade
						if pos and self.broker.acceptLive:
							# Delete Strategy Position
							self.broker.userAccount.updateTrades(
								self.broker.strategyId,
								self.broker.getAllPositions(account_id=tl.broker.PAPERTRADER_NAME),
								self.broker.getAllOrders(account_id=tl.broker.PAPERTRADER_NAME)
							)

							self.broker.handleOnTrade(order.account_id, res)

						# Update transaction history
						self.broker.handleTransaction(res)


	def handleStopLoss(self, product, timestamp, ohlc, is_backtest=False):
		# Block any ticks before position check completed
		if not is_backtest and not self.broker.acceptLive:
			return

		ask = ohlc[:4]
		bid = ohlc[4:]
		for pos in self.broker.getAllPositions():
			if pos.product != product or not pos.sl or pos.account_id != tl.broker.PAPERTRADER_NAME:
				continue

			if ((pos.direction == tl.LONG and bid[2] <= pos.sl) or
				(pos.direction == tl.SHORT and ask[1] >= pos.sl)):
				
				# Close Position
				pos.close_price = pos.sl
				pos.close_time = timestamp

				# Delete Position
				del self.broker.positions[self.broker.positions.index(pos)]

				res = {
					self.broker.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.STOP_LOSS,
						'accepted': True,
						'item': pos
					}
				}

				# On Trade
				if self.broker.acceptLive:
					self.broker.handleOnTrade(pos.account_id, res)
				self.handleTransaction(res)


	def handleTakeProfit(self, product, timestamp, ohlc, is_backtest=False):
		# Block any ticks before position check completed
		if not is_backtest and not self.broker.acceptLive:
			return

		ask = ohlc[:4]
		bid = ohlc[4:]

		for pos in self.broker.positions:
			if pos.product != product or not pos.tp or pos.account_id != tl.broker.PAPERTRADER_NAME:
				continue

			if ((pos.direction == tl.LONG and bid[1] >= pos.tp) or
				(pos.direction == tl.SHORT and ask[2] <= pos.tp)):
				
				# Close Position
				pos.close_price = pos.tp
				pos.close_time = timestamp

				# Delete Position
				del self.broker.positions[self.broker.positions.index(pos)]

				res = {
					self.broker.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.TAKE_PROFIT,
						'accepted': True,
						'item': pos
					}
				}

				# On Trade
				if self.broker.acceptLive:
					self.broker.handleOnTrade(pos.account_id, res)
				self.handleTransaction(res)


class IGBacktester(Backtester):

	def __init__(self, broker, sort_reversed=False):
		super(IGBacktester, self).__init__(broker)


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		result = super(IGBacktester, self).createPosition(
			product, lotsize, direction,
			account_id, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.open_time,
				'type': tl.MARKET_ENTRY,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(account_id, res)
		self.handleTransaction(res)

		return res


	def createOrder(self,
		product, lotsize, direction, account_id,
		order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		result = super(IGBacktester, self).createOrder(
			product, lotsize, direction, account_id,
			order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.open_time,
				'type': result.order_type,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(account_id, res)
		self.handleTransaction(res)

		return res


	def modifyPosition(self, pos, sl_price, tp_price):
		result = super(IGBacktester, self).modifyPosition(
			pos, sl_price=sl_price, tp_price=tp_price
		)

		res = {
			self.broker.generateReference(): {
				'timestamp': math.floor(time.time()),
				'type': tl.MODIFY,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


	def deletePosition(self, pos, lotsize):
		result = super(IGBacktester, self).deletePosition(pos, lotsize)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.close_time,
				'type': tl.POSITION_CLOSE,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price):
		result = super(IGBacktester, self).modifyOrder(
			order, lotsize, entry_price, sl_price, tp_price
		)

		res = {
			self.broker.generateReference(): {
				'timestamp': math.floor(time.time()),
				'type': tl.MODIFY,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


	def deleteOrder(self, order):
		result = super(IGBacktester, self).deleteOrder(order)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.close_time,
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


class OandaBacktester(Backtester):

	def __init__(self, broker, sort_reversed=False):
		super(OandaBacktester, self).__init__(broker)
		self._sort_reversed = sort_reversed


	def _net_off(self, account_id, direction, lotsize):
		positions = sorted(
			[
				pos for pos in self.broker.getAllPositions(account_id=account_id) 
				if pos.direction != direction
				if pos.account_id == tl.broker.PAPERTRADER_NAME
			],
			key=lambda x: x.open_time,
			reverse=self._sort_reversed
		)

		res = {}
		remaining = lotsize
		for pos in positions:
			delete_size = min(pos.lotsize, remaining)
			res.update(self.deletePosition(pos, delete_size))
			remaining -= delete_size
			if remaining <= 0:
				return remaining, res

		return remaining, res

	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		# Handle closing any opposite positions first
		remaining, res = self._net_off(account_id, direction, lotsize)
		if remaining > 0:
			# Create Position
			result = super(OandaBacktester, self).createPosition(
				product, remaining, direction,
				account_id, entry_range, entry_price,
				None, None, None, None
			)

			entry_res = {
				self.broker.generateReference(): {
					'timestamp': result.open_time,
					'type': tl.MARKET_ENTRY,
					'accepted': True,
					'item': result
				}
			}
			res.update(entry_res)

			if self.broker.acceptLive:
				# Handle On Trade
				self.broker.handleOnTrade(result.account_id, entry_res)

			# Add SL
			if sl_range or sl_price:
				t_result = super(OandaBacktester, self).modifyPosition(
					result, sl_range=sl_range, sl_price=sl_price
				)
				sl_res = {
					self.broker.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MODIFY,
						'accepted': True,
						'item': t_result
					}
				}
				res.update(sl_res)

				if self.broker.acceptLive:
					# Handle On Trade
					self.broker.handleOnTrade(t_result.account_id, sl_res)

			# Add TP
			if tp_range or tp_price:
				t_result = super(OandaBacktester, self).modifyPosition(
					result, tp_range=tp_range, tp_price=tp_price
				)
				tp_res = {
					self.broker.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MODIFY,
						'accepted': True,
						'item': t_result
					}
				}
				res.update(tp_res)

				if self.broker.acceptLive:
					# Handle On Trade
					self.broker.handleOnTrade(t_result.account_id, tp_res)

			self.handleTransaction(res)

		return res


	def createOrder(self,
		product, lotsize, direction, account_id,
		order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		result = super(OandaBacktester, self).createOrder(
			product, lotsize, direction, account_id,
			order_type, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.open_time,
				'type': result.order_type,
				'accepted': True,
				'item': result
			}
		}
		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


	def modifyPosition(self, pos, sl_price, tp_price):
		res = {}
		# Handle sl modify
		if pos.sl != sl_price:
			result = super(OandaBacktester, self).modifyPosition(
				pos, sl_price=sl_price
			)
			sl_res = {
				self.broker.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': True,
					'item': result
				}
			}
			res.update(sl_res)

			if self.broker.acceptLive:
				# Handle On Trade
				self.broker.handleOnTrade(result.account_id, sl_res)

		# Handle tp modify
		if pos.tp != tp_price:
			result = super(OandaBacktester, self).modifyPosition(
				pos, tp_price=tp_price
			)
			tp_res = {
				self.broker.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': True,
					'item': result
				}
			}
			res.update(tp_res)

			if self.broker.acceptLive:
				# Handle On Trade
				self.broker.handleOnTrade(result.account_id, tp_res)

		self.handleTransaction(res)

		return res


	def deletePosition(self, pos, lotsize):
		result = super(OandaBacktester, self).deletePosition(pos, lotsize)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.close_time,
				'type': tl.POSITION_CLOSE,
				'accepted': True,
				'item': result
			}
		}

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price):
		new_order = tl.Order.fromDict(self.broker, order)
		new_order.order_id = self.broker.generateReference()

		# Cancel current order
		order.close_time = math.floor(time.time())
		del self.broker.orders[self.broker.orders.index(order)]

		res = {}

		cancel_res = {
			self.broker.generateReference(): {
				'timestamp': order.close_time,
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': order
			}
		}
		res.update(cancel_res)


		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(order.account_id, cancel_res)

		# Modify new order
		result = super(OandaBacktester, self).modifyOrder(
			new_order, lotsize, entry_price, sl_price, tp_price
		)

		self.broker.orders.append(result)

		modify_res = {
			self.broker.generateReference(): {
				'timestamp': math.floor(time.time()),
				'type': result.order_type,
				'accepted': True,
				'item': result
			}
		}
		res.update(modify_res)

		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, modify_res)
		self.handleTransaction(res)

		return res


	def deleteOrder(self, order):
		result = super(OandaBacktester, self).deleteOrder(order)

		res = {
			self.broker.generateReference(): {
				'timestamp': result.close_time,
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': result
			}
		}


		if self.broker.acceptLive:
			# Handle On Trade
			self.broker.handleOnTrade(result.account_id, res)
		self.handleTransaction(res)

		return res


	def createOrderPosition(self, order):
		if order.order_type == tl.LIMIT_ORDER:
			order_type = tl.LIMIT_ENTRY
		elif order.order_type == tl.STOP_ORDER:
			order_type = tl.STOP_ENTRY
		else:
			order_type = tl.MARKET_ENTRY

		remaining, res = self._net_off(order.account_id, order.direction, order.lotsize)
		if remaining > 0:
			pos = tl.Position.fromOrder(self.broker, order)
			pos.order_id = self.broker.generateReference()
			pos.order_type = order_type
			pos.lotsize = remaining
			self.broker.positions.append(pos)

			res.update({
				self.broker.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': pos.order_type,
					'accepted': True,
					'item': pos
				}
			})

			return pos, res

		return None, res


