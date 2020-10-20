import time
import math
import numpy as np
import pandas as pd
from copy import copy


class Backtester(object):

	def __init__(self, broker):
		self.broker = broker
		self.result = []

		self._idx = 0


	def _create_empty_transaction_df(self):
		df = pd.DataFrame(columns=[
			'reference_id', 'timestamp', 'type', 'accepted',
			'order_id', 'account_id', 'product', 'order_type',
			'direction', 'lotsize', 'entry_price', 'close_price', 'sl', 'tp',
			'open_time', 'close_time'
		])
		return df.set_index('reference_id')


	def _order_validation(self, order, min_dist=0):

		if order.direction == tl.LONG:
			price = self.broker.getAsk(order.product)
		else:
			price = self.broker.getBid(order.product)

		# Entry validation
		if order.get('order_type') == tl.STOP_ORDER or order.get('order_type') == tl.LIMIT_ORDER:
			if order.entry_price == None:
				raise tl.error.OrderException('Order must contain entry price.')
			elif order_type == tl.LIMIT_ORDER:
				if direction == tl.LONG:
					if order.entry_price > price - tl.utils.convertToPrice(min_dist):
						raise tl.error.OrderException('Long limit order entry must be lesser than current price.')
				else:
					if order.entry_price < price + tl.utils.convertToPrice(min_dist):
						raise tl.error.OrderException('Short limit order entry must be greater than current price.')
			elif order_type == tl.STOP_ORDER:
				if order.direction == tl.LONG:
					if order.entry_price < price + tl.utils.convertToPrice(min_dist):
						raise tl.error.OrderException('Long stop order entry must be greater than current price.')
				else:
					if order.entry_price > price - tl.utils.convertToPrice(min_dist):
						raise tl.error.OrderException('Short stop order entry must be lesser than current price.')

		# SL/TP validation
		if order.direction == tl.LONG:
			if order.sl and order.sl > order.entry_price - tl.utils.convertToPrice(min_dist):
				raise tl.error.OrderException('Stop loss price must be lesser than entry price.')
			if order.tp and order.tp < order.entry_price + tl.utils.convertToPrice(min_dist):
				raise tl.error.OrderException('Take profit price must be greater than entry price.')
		else:
			if order.sl and order.sl < order.entry_price + tl.utils.convertToPrice(min_dist):
				raise tl.error.OrderException('Stop loss price must be greater than entry price.')
			if order.tp and order.tp > order.entry_price - tl.utils.convertToPrice(min_dist):
				raise tl.error.OrderException('Take profit price must be lesser than entry price.')


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		if direction == tl.LONG:
			price = self.broker.getAsk(product)
		else:
			price = self.broker.getBid(product)

		# Set Stoploss
		sl = None
		if sl_price:
			sl = np.around(sl_price, 5)
		elif sl_range:
			if direction == tl.LONG:
				sl = np.around(price - tl.utils.convertToPrice(sl_range), 5)
			else:
				sl = np.around(price + tl.utils.convertToPrice(sl_range), 5)

		# Set Takeprofit
		tp = None
		if tp_price:
			tp = np.around(tp_price, 5)
		elif tp_range:
			if direction == tl.LONG:
				tp = np.around(price + tl.utils.convertToPrice(tp_range), 5)
			else:
				tp = np.around(price - tl.utils.convertToPrice(tp_range), 5)

		# Set Entry Price
		entry =None
		if entry_price:
			entry = np.around(entry_price, 5)
		else:
			entry = np.around(price, 5)

		open_time = int(self.broker.getTimestamp(product, tl.period.ONE_MINUTE))

		order_id = self.broker.generateReference()
		order = tl.BacktestPosition(self.broker,
			order_id, account_id, product, tl.MARKET_ENTRY, 
			direction, lotsize, entry_price=entry, sl=sl, tp=tp,
			open_time=open_time
		)

		# Validate order
		self._order_validation(order)

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

		open_time = int(self.broker.getTimestamp(product, tl.period.ONE_MINUTE))
		

		order_id = self.broker.generateReference()
		order = tl.BacktestOrder(self.broker,
			order_id, account_id, product, 
			order_type, direction, lotsize,
			entry_price=entry, sl=sl, tp=tp,
			open_time=open_time
		)

		# Validate order
		self._order_validation(order)

		self.broker.orders.append(order)

		return order


	def modifyPosition(self, pos, sl_range, tp_range, sl_price, tp_price):
		if sl_range is not None:
			if pos.direction == tl.LONG:
				pos.sl = round(pos.entry_price - tl.utils.convertToPrice(sl_range), 5)
			else:
				pos.sl = round(pos.entry_price + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price is not None:
			pos.sl = sl_price

		if tp_range is not None:
			if pos.direction == tl.LONG:
				pos.tp = round(pos.entry_price + tl.utils.convertToPrice(tp_range), 5)
			else:
				pos.tp = round(pos.entry_price - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price is not None:
			pos.tp = tp_price

		return pos


	def deletePosition(self, pos, lotsize):
		if lotsize >= pos.lotsize:
			if pos.direction == tl.LONG:
				pos.close_price = self.broker.getBid(pos.product)
			else:
				pos.close_price = self.broker.getAsk(pos.product)
			pos.close_time = self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE)

			result = pos

			# Delete position from broker positions
			del self.broker.positions[self.broker.positions.index(pos)]

		elif lotsize <= 0:
			# Raise error
			raise tl.error.OrderException('Position close size must be greater than 0.')

		else:
			cpy = tl.Position.fromDict(self, self)
			cpy.lotsize = lotsize
			if pos.direction == tl.LONG:
				cpy.close_price = self.broker.getBid(pos.product)
			else:
				cpy.close_price = self.broker.getAsk(pos.product)
			cpy.close_time =  self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE)
			
			result = cpy

			pos.lotsize -= lotsize

		return result


	def modifyOrder(self, order, lotsize, sl_range, tp_range, sl_price, tp_price):
		if lotsize is not None:
			order.lotsize = lotsize

		# Convert to price
		if entry_range is not None:
			if direction == tl.LONG:
				order.entry_price = round(order.entry_price + tl.utils.convertToPrice(entry_range), 5)
			else:
				order.entry_price = round(order.entry_price - tl.utils.convertToPrice(entry_range), 5)
		elif entry_price is not None:
			order.entry_price = entry_price

		if sl_range is not None:
			if direction == tl.LONG:
				order.sl = round(self.entry - tl.utils.convertToPrice(sl_range), 5)
			else:
				order.sl = round(self.entry + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price is not None:
			order.sl = sl_price

		if tp_range is not None:
			if direction == tl.LONG:
				order.tp = round(self.entry + tl.utils.convertToPrice(tp_range), 5)
			else:
				order.tp = round(self.entry - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price is not None:
			order.tp = tp_price

		return order


	def deleteOrder(self, order):
		order.close_time = self.broker.getTimestamp(order.product, tl.period.ONE_MINUTE)
		del self.broker.orders[self.broker.orders.index(order)]

		return order


	def handleTransaction(self, res):
		for k, v in res.items():
			for func in self.broker.ontrade_subs:
				func(
					BrokerItem({
						'reference_id': k,
						'type': v.get('type'),
						'item': v.get('item')
					})
				)


	def createTransactionItem(self, ref_id, timestamp, order_type, prev_item, new_item):
		item = {
			'id': ref_id,
			'timestamp': timestamp,
			'type': order_type,
			'item': {
				'prev': prev_item,
				'new': new_item
			}
		}

		self.result.append(item)


	def createDrawing(self, timestamp, layer, drawing):
		item = {
			'timestamp': timestamp,
			'type': tl.CREATE_DRAWING,
			'item': drawing
		}

		self.result.append(item)


	def clearDrawingLayer(self, timestamp, layer):
		item = {
			'timestamp': timestamp,
			'type': tl.CLEAR_DRAWING_LAYER,
			'item': layer
		}

		self.result.append(item)


	def deleteAllDrawings(self, timestamp):
		item = {
			'timestamp': timestamp,
			'type': tl.CLEAR_ALL_DRAWINGS,
			'item': None
		}

		self.result.append(item)


	def createInfoItem(self, timestamp, item):
		item = {
			'timestamp': timestamp,
			'type': tl.CREATE_INFO,
			'item': item
		}

		self.result.append(item)


	def createLogItem(self, timestamp, item):
		item = {
			'timestamp': timestamp,
			'type': tl.CREATE_LOG,
			'item': item
		}

		self.result.append(item)


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
				'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
				'type': pos.order_type,
				'accepted': True,
				'item': pos
			}
		}

		return pos, res


	def handleOrders(self, product, timestamp, ohlc):
		ask = ohlc[:4]
		bid = ohlc[4:]

		for order in self.broker.getAllOrders():
			if order.product != product or not order.isBacktest():
				continue

			if order.order_type == tl.LIMIT_ORDER:
				if order.direction == tl.LONG:
					if ask[2] <= order.entry_price:
						# Enter Order Position LONG
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = chart.ts[chart.c_idx]

						# Delete Order
						del self.broker.orders[self.orders.index(order)]

						# On Trade
						self.handleTransaction(res)

				else:
					if bid[1] >= order.entry_price:
						# Enter Order Position LONG
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = chart.ts[chart.c_idx]
						
						# Delete Order
						del self.broker.orders[self.orders.index(order)]

						# On Trade
						self.handleTransaction(res)

			elif order.order_type == tl.STOP_ORDER:
				if order.direction == tl.LONG:
					if ask[1] >= order.entry_price:
						# Enter Order Position LONG
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = chart.ts[chart.c_idx]

						# Delete Order
						del self.broker.orders[self.orders.index(order)]

						# On Trade
						self.handleTransaction(res)

				else:
					if bid[2] <= order.entry_price:
						# Enter Order Position LONG
						pos, res = self.createOrderPosition(order)

						# Close Order
						order.close_price = order.entry_price
						order.close_time = chart.ts[chart.c_idx]

						# Delete Order
						del self.broker.orders[self.orders.index(order)]

						# On Trade
						self.handleTransaction(res)


	def handleStopLoss(self, product, timestamp, ohlc):
		ask = ohlc[:4]
		bid = ohlc[4:]
		for pos in self.broker.getAllPositions():
			if pos.product != product or not pos.sl or not pos.isBacktest():
				continue

			if ((pos.direction == tl.LONG and bid[2] <= pos.sl) or
				(pos.direction == tl.SHORT and ask[1] >= pos.sl)):
				
				prev_item = copy(pos)
				
				# Close Position
				pos.close_price = pos.sl
				pos.close_time = timestamp
				# Delete Position
				del self.broker.positions[self.broker.positions.index(pos)]

				ref_id = self.broker.generateReference()
				res = {
					ref_id: {
						'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
						'type': tl.STOP_LOSS,
						'accepted': True,
						'item': pos
					}
				}

				# On Trade
				self.handleTransaction(res)
				self.createTransactionItem(ref_id, pos.close_time, tl.STOP_LOSS, prev_item, copy(pos))


	def handleTakeProfit(self, product, timestamp, ohlc):
		ask = ohlc[:4]
		bid = ohlc[4:]

		for pos in self.broker.positions:
			if (pos.product != product or not pos.tp or not pos.isBacktest()):
				continue

			if ((pos.direction == tl.LONG and bid[1] >= pos.tp) or
				(pos.direction == tl.SHORT and ask[2] <= pos.tp)):
				
				prev_item = copy(pos)

				# Close Position
				pos.close_price = pos.tp
				pos.close_time = timestamp

				# Delete Position
				del self.broker.positions[self.broker.positions.index(pos)]

				ref_id = self.broker.generateReference()
				res = {
					ref_id: {
						'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
						'type': tl.TAKE_PROFIT,
						'accepted': True,
						'item': pos
					}
				}

				# On Trade
				self.handleTransaction(res)
				self.createTransactionItem(ref_id, pos.close_time, tl.TAKE_PROFIT, prev_item, copy(pos))


	def _process_chart_data(self, charts, start, end):
		# Create chart array with existing charts from start/end points
		dataframes = []
		periods = []
		for i in range(len(charts)):
			chart = charts[i]

			# Add list of periods and respective dataframes
			periods.append([period for period in chart._subscriptions])
			dataframes.append(
				[chart._data[period].copy() for period in chart._subscriptions]
			)

			# Sort periods by period offset
			periods[i] = sorted(
				periods[i], 
				key=lambda x: tl.period.getPeriodOffsetSeconds(x)
			)
			# Sort dataframes by period offset
			dataframes[i] = sorted(
				dataframes[i], 
				key=lambda x: tl.period.getPeriodOffsetSeconds(periods[i][dataframes[i].index(x)])
			)

			# Add period offset to dataframe index for correct period sorting
			for j in range(len(dataframes[i])):
				dataframes[i][j].index += tl.period.getPeriodOffsetSeconds(periods[i][j])

		return dataframes, periods


	def _process_timestamps(self, dataframes, periods, start, end):
		# Get all timestamps
		all_ts = []
		for i in range(len(dataframes)): # For each chart
			for j in range(len(dataframes[i])): # For each period
				df = dataframes[i][j]
				period = periods[i][j]

				if start and end:
					all_ts.append(df.index.values[(start<=df.index.values) & (df.index.values<end)])
				else:
					start_idx = charts[i]._idx[period]+1
					all_ts.append(df.index.values[start_idx:])# Period data from last idx

		# Collapse, remove duplicate timestamps, sort and convert to ndarray
		all_ts = np.unique(np.sort(np.concatenate(all_ts)))

		return all_ts


	def _process_indicies(self, dataframes, periods, all_ts, start, end):
		# Create container for chart indicies
		chart_indicies = []
		for i in range(len(dataframes)):
			chart_indicies.append(
				np.ones((len(dataframes[i]), all_ts.size), dtype=np.int32)*-1
			)

		# Set appropriate indicies per chart period
		for i in range(len(chart_indicies)):
			for j in range(chart_indicies[i].shape[0]):
				df = dataframes[i][j]
				period = periods[i][j]
				# Change all timestamp intersections to data index
				if start and end:
					off = np.where(start<=df.index.values)[0][0]
				else:
					off = charts[i]._idx[period]+1

				intersect = np.in1d(all_ts, df.index.values)
				chart_indicies[i][j, intersect] = np.arange(
					off, off+np.count_nonzero(intersect), 
					dtype=int
				)

		return chart_indicies


	def _event_loop(self, charts, periods, all_ts, chart_indicies, mode):
		'''Run event loop'''

		start = time.time()

		# Convert 
		if isinstance(mode, tl.broker.BacktestMode): mode = mode.value

		# For Each timestamp
		for i in range(all_ts.size):
			# For Each chart
			for x in range(len(charts)):
				# For Each period
				for j in range(len(periods[x])):
					if np.all(chart_indicies[x][j,i] != -1):
						# Set current index of current chart period
						period = periods[x][j]
						charts[x]._set_idx(period, chart_indicies[x][j,i])

						timestamp = int(charts[x].getTimestamp(period))
						ohlc = charts[x].getLastOHLC(period)

						idx = charts[x]._idx[period]
						charts[x].timestamps[period] = charts[x]._data[period].index.values[:idx+1][::-1]
						charts[x].asks[period] = charts[x]._data[period].values[:idx+1,:4][::-1]
						charts[x].bids[period] = charts[x]._data[period].values[:idx+1,4:][::-1]

						# If lowest period, do position/order check
						if j == 0:
							self.handleOrders(charts[x].product, timestamp, ohlc)
							self.handleStopLoss(charts[x].product, timestamp, ohlc)
							self.handleTakeProfit(charts[x].product, timestamp, ohlc)

						# Call threaded ontick functions
						if period in charts[x]._subscriptions:
							for func in charts[x]._subscriptions[period]:
								tick = BrokerItem({
									'chart': charts[x], 
									'timestamp': timestamp,
									'period': period, 
									'ask': ohlc[:4],
									'bid': ohlc[4:],
									'bar_end': True
								})

								self.broker.strategy.setTick(tick)
								func(tick)

			if mode == tl.broker.BacktestMode.STEP.value:
				input('(Enter) to continue...')


	def performBacktest(self, mode, start=None, end=None):
		# Get timestamps
		if start and end:
			start = tl.convertTimeToTimestamp(start)
			end = tl.convertTimeToTimestamp(end)

		# Process chart data
		charts = copy(self.broker.charts)
		dataframes, periods = self._process_chart_data(charts, start, end)

		# Process timestamps
		all_ts = self._process_timestamps(dataframes, periods, start, end)
		# If no timestamps, finish
		if all_ts.size == 0: 
			return self.result

		# Process indicies
		chart_indicies = self._process_indicies(dataframes, periods, all_ts, start, end)

		# Run event loop
		self._event_loop(charts, periods, all_ts, chart_indicies, mode)


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

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.open_time,
				'type': tl.MARKET_ENTRY,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.open_time, tl.MARKET_ENTRY, None, copy(result))

		return result


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

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.open_time,
				'type': result.order_type,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.open_time, result.order_type, None, copy(result))

		return result


	def modifyPosition(self, pos, sl_range, tp_range, sl_price, tp_price):
		prev_item = copy(pos)

		result = super(IGBacktester, self).modifyPosition(
			pos, sl_range, tp_range, sl_price, tp_price
		)

		ref_id = self.broker.generateReference()
		timestamp = self.broker.getTimestamp(result.product, tl.period.ONE_MINUTE)
		res = {
			ref_id: {
				'timestamp': timestamp,
				'type': tl.MODIFY,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, timestamp, tl.MODIFY, prev_item, copy(result))

		return result


	def deletePosition(self, pos, lotsize):
		prev_item = copy(pos)

		result = super(IGBacktester, self).deletePosition(pos, lotsize)

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.close_time,
				'type': tl.POSITION_CLOSE,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.close_time, tl.POSITION_CLOSE, prev_item, copy(result))

		return result


	def modifyOrder(self, order, lotsize, sl_range, tp_range, sl_price, tp_price):
		prev_item = copy(pos)

		result = super(IGBacktester, self).modifyOrder(
			order, lotsize, sl_range, tp_range, sl_price, tp_price
		)

		ref_id = self.broker.generateReference()
		timestamp = self.broker.getTimestamp(result.product, tl.period.ONE_MINUTE)
		res = {
			ref_id: {
				'timestamp': timestamp,
				'type': tl.MODIFY,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, timestamp, tl.MODIFY, prev_item, copy(result))

		return result


	def deleteOrder(self, order):
		prev_item = copy(pos)

		result = super(IGBacktester, self).deleteOrder(order)

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.close_time,
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.close_time, tl.ORDER_CANCEL, prev_item, copy(result))

		return result



class OandaBacktester(Backtester):

	def __init__(self, broker, sort_reversed=False):
		super(OandaBacktester, self).__init__(broker)
		self._sort_reversed = sort_reversed


	def _net_off(self, account_id, direction, lotsize):
		positions = sorted(
			[
				pos for pos in self.broker.getAllPositions(account_id=account_id) 
				if pos.direction != direction
				if pos.isBacktest()
			],
			key=lambda x: x.open_time,
			reverse=self._sort_reversed
		)

		result = []
		remaining = lotsize
		for pos in positions:
			delete_size = min(pos.lotsize, remaining)
			result.append(pos.close(lotsize=delete_size))
			remaining -= delete_size
			if remaining <= 0:
				return remaining, result

		return remaining, result


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		# Handle closing any opposite positions first
		remaining, _ = self._net_off(account_id, direction, lotsize)
		result = None
		if remaining > 0:
			# Create Position
			result = super(OandaBacktester, self).createPosition(
				product, remaining, direction, 
				account_id, entry_range, entry_price,
				None, None, None, None
			)

			res = {}
			ref_id = self.broker.generateReference()
			entry_res = {
				ref_id: {
					'timestamp': result.open_time,
					'type': tl.MARKET_ENTRY,
					'accepted': True,
					'item': result
				}
			}
			res.update(entry_res)

			# Add SL
			if sl_range or sl_price:
				result = super(OandaBacktester, self).modifyPosition(
					result, sl_range, None, sl_price, None
				)
				sl_res = {
					self.broker.generateReference(): {
						'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
						'type': tl.MODIFY,
						'accepted': True,
						'item': result
					}
				}
				res.update(sl_res)

			# Add TP
			if tp_range or tp_price:
				result = super(OandaBacktester, self).modifyPosition(
					result, None, tp_range, None, tp_price
				)
				tp_res = {
					self.broker.generateReference(): {
						'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
						'type': tl.MODIFY,
						'accepted': True,
						'item': result
					}
				}
				res.update(tp_res)

			self.handleTransaction(res)
			self.createTransactionItem(ref_id, result.open_time, tl.MARKET_ENTRY, None, copy(result))

		return result


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

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.open_time,
				'type': result.order_type,
				'accepted': True,
				'item': result
			}
		}
		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.open_time, result.order_type, None, copy(result))

		return result


	def modifyPosition(self, pos, sl_range, tp_range, sl_price, tp_price):
		prev_item = copy(pos)
		res = {}
		result = pos
		if pos.sl != sl_price:
			result = super(OandaBacktester, self).modifyPosition(
				pos, None, None, sl_price, None
			)
			sl_res = {
				self.broker.generateReference(): {
					'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
					'type': tl.MODIFY,
					'accepted': True,
					'item': result
				}
			}
			res.update(sl_res)


		if pos.tp != tp_price:
			result = super(OandaBacktester, self).modifyPosition(
				pos, None, None, None, tp_price
			)
			tp_res = {
				self.broker.generateReference(): {
					'timestamp': self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE),
					'type': tl.MODIFY,
					'accepted': True,
					'item': result
				}
			}
			res.update(tp_res)

		self.handleTransaction(res)
		self.createTransactionItem(
			self.broker.generateReference(), 
			self.broker.getTimestamp(pos.product, tl.period.ONE_MINUTE), 
			tl.MODIFY, prev_item, copy(result)
		)

		return result


	def deletePosition(self, pos, lotsize):
		prev_item = copy(pos)

		result = super(OandaBacktester, self).deletePosition(pos, lotsize)

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.close_time,
				'type': tl.POSITION_CLOSE,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.close_time, tl.POSITION_CLOSE, prev_item, copy(result))

		return result


	def modifyOrder(self, order, lotsize, sl_range, tp_range, sl_price, tp_price):
		prev_item = copy(order)

		new_order = tl.Order.fromDict(self.broker, order)
		new_order.order_id = self.broker.generateReference()

		order.close_time = self.broker.getTimestamp(order.product, tl.period.ONE_MINUTE)
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

		result = super(OandaBacktester, self).modifyOrder(
			new_order, lotsize, sl_range, tp_range, sl_price, tp_price
		)

		ref_id = self.broker.generateReference()
		timestamp = self.broker.getTimestamp(order.product, tl.period.ONE_MINUTE)
		modify_res = {
			ref_id: {
				'timestamp': timestamp,
				'type': result.order_type,
				'accepted': True,
				'item': result
			}
		}
		res.update(modify_res)
		
		self.handleTransaction(res)
		self.createTransactionItem(ref_id, timestamp, tl.MODIFY, prev_item, copy(result))

		return result


	def deleteOrder(self, order):
		prev_item = copy(order)

		result = super(OandaBacktester, self).deleteOrder(order)

		ref_id = self.broker.generateReference()
		res = {
			ref_id: {
				'timestamp': result.close_time,
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': result
			}
		}

		self.handleTransaction(res)
		self.createTransactionItem(ref_id, result.close_time, tl.ORDER_CANCEL, prev_item, copy(result))

		return result


	def createOrderPosition(self, order):
		if order.order_type == tl.LIMIT_ORDER:
			order_type = tl.LIMIT_ENTRY
		elif order.order_type == tl.STOP_ORDER:
			order_type = tl.STOP_ENTRY
		else:
			order_type = tl.MARKET_ENTRY

		remaining, result = self._net_off(account_id, order.direction, order.lotsize)
		if remaining > 0:
			pos = tl.Position.fromOrder(self.broker, order)
			pos.order_id = self.broker.generateReference()
			pos.order_type = order_type
			pos.lotsize = remaining
			self.positions.append(pos)
			result.append(pos)

			res = {
				self.broker.generateReference(): {
					'timestamp': self.broker.getTimestamp(order.product, tl.period.ONE_MINUTE),
					'type': pos.order_type,
					'accepted': True,
					'item': pos
				}
			}

			return result, res

		return result, {}


'''
Imports
'''
from app import pythonsdk as tl
from .broker import BrokerItem
