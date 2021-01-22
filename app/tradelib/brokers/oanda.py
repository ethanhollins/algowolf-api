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


class Subscription(object):
	ACCOUNT = 'account'
	CHART = 'chart'

	def __init__(self, sub_type, listener, *args):
		self.res = []
		self.sub_type = sub_type
		self.listener = listener
		self.args = args

		self.receive = False
		self.stream = None
		self.last_update = None

	def setStream(self, stream):
		self.receive = True
		self.stream = stream


class Oanda(Broker):

	def __init__(self, 
		ctrl, key, is_demo, 
		user_account=None, broker_id=None, 
		accounts={}, display_name=None,
		is_dummy=False, is_parent=False
	):
		super().__init__(ctrl, user_account, broker_id, tl.broker.OANDA_NAME, accounts, display_name)

		self.dl = tl.DataLoader(broker=self)
		self.data_saver = tl.DataSaver(broker=self)

		self._key = key
		self._is_demo = is_demo

		self._session = requests.session()
		self._headers = {
			'Authorization': 'Bearer '+self._key,
			'Connection': 'keep-alive',
			'Content-Type': 'application/json'
		}
		self._session.headers.update(self._headers)

		self._url = (
			'https://api-fxpractice.oanda.com'
			if self._is_demo else
			'https://api-fxtrade.oanda.com'
		)
		self._stream_url = (
			'https://stream-fxpractice.oanda.com'
			if self._is_demo else
			'https://stream-fxtrade.oanda.com'
		)

		self._last_update = time.time()
		self._subscriptions = []

		if not is_dummy:
			for account_id in self.getAccounts():
				if account_id != tl.broker.PAPERTRADER_NAME:
					self._subscribe_account_updates(account_id)

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

		if is_parent:
			# Load Charts
			CHARTS = ['EUR_USD']
			for instrument in CHARTS:
				chart = self.getChart(instrument)
				self.data_saver.subscribe(chart)


	def _periodic_check(self):
		TWENTY_SECONDS = 20
		self._last_update = time.time()
		# Check most recent Oanda `HEARTBEAT` was received or reconnect
		while self.is_running:
			if time.time() - self._last_update > TWENTY_SECONDS:
				print('RECONNECT')
				# Perform periodic refresh
				self._reconnect()
			time.sleep(5)

		for sub in self._subscriptions:
			for i in sub.res:
				i.close()


	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		result = {}

		dl_start = None
		dl_end = None
		if start:
			dl_start = tl.utils.convertTimeToTimestamp(start)
		if end:
			dl_end = tl.utils.convertTimeToTimestamp(end)

		if period == tl.period.TICK:
			period = tl.period.FIVE_SECONDS

		while True:
			# time.sleep(0.5)

			if count:
				if start:
					start = tl.utils.convertTimezone(start, 'UTC')

					start_str = start.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
					endpoint = '/v3/instruments/{}/candles?price=BAM' \
								'&from={}&count={}&granularity={}&smooth=True'.format(
									product, start_str, count, period
								)
				else:
					endpoint = '/v3/instruments/{}/candles?price=BAM' \
								'&count={}&granularity={}&smooth=True'.format(
									product, count, period
								)
			else:
				start = tl.utils.convertTimezone(start, 'UTC')
				end = tl.utils.convertTimezone(end, 'UTC')

				start_str = start.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
				end_str = end.strftime('%Y-%m-%dT%H:%M:%S.000000000Z')
				endpoint = '/v3/instruments/{}/candles?price=BAM' \
							'&from={}&to={}&granularity={}&smooth=True'.format(
								product, start_str, end_str, period
							)

			res = self._session.get(
				self._url + endpoint,
				headers=self._headers
			)

			if res.status_code == 200:
				if len(result) == 0:
					result['timestamp'] = []
					result['ask_open'] = []
					result['ask_high'] = []
					result['ask_low'] = []
					result['ask_close'] = []
					result['mid_open'] = []
					result['mid_high'] = []
					result['mid_low'] = []
					result['mid_close'] = []
					result['bid_open'] = []
					result['bid_high'] = []
					result['bid_low'] = []
					result['bid_close'] = []

				data = res.json()
				candles = data['candles']

				for i in candles:

					dt = datetime.strptime(i['time'], '%Y-%m-%dT%H:%M:%S.000000000Z')
					ts = tl.utils.convertTimeToTimestamp(dt)

					if ((not dl_start or ts >= dl_start) and 
						(not dl_end or ts < dl_end) and 
						(not len(result['timestamp']) or ts != result['timestamp'][-1])):
					
						result['timestamp'].append(ts)
						asks = list(map(float, i['ask'].values()))
						mids = list(map(float, i['mid'].values()))
						bids = list(map(float, i['bid'].values()))
						result['ask_open'].append(asks[0])
						result['ask_high'].append(asks[1])
						result['ask_low'].append(asks[2])
						result['ask_close'].append(asks[3])
						result['mid_open'].append(mids[0])
						result['mid_high'].append(mids[1])
						result['mid_low'].append(mids[2])
						result['mid_close'].append(mids[3])
						result['bid_open'].append(bids[0])
						result['bid_high'].append(bids[1])
						result['bid_low'].append(bids[2])
						result['bid_close'].append(bids[3])

				if count:
					if (not len(result['timestamp']) >= 5000 and
							start and end and not self._is_last_candle_found(period, start, end, count)):
						start = datetime.strptime(candles[-1]['time'], '%Y-%m-%dT%H:%M:%S.000000000Z')
						count = 5000
						continue
				
				return pd.DataFrame(data=result).set_index('timestamp')

			if res.status_code == 400:
				if (
					'Maximum' in res.json()['errorMessage'] or 
					('future' in res.json()['errorMessage'] and
						'\'to\'' in res.json()['errorMessage'])
				):
					count = 5000
					continue
				else:
					if len(result):
						return pd.DataFrame(data=result).set_index('timestamp')
					else:
						print(res.json())
						return None
			else:
				print('Error:\n{0}'.format(res.json()))
				return None

	def _is_last_candle_found(self, period, start_dt, end_dt, count):
		utcnow = tl.utils.setTimezone(datetime.utcnow(), 'UTC')
		if period == tl.period.ONE_MINUTE:
			new_dt = start_dt + timedelta(minutes=count)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.TWO_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*2)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.THREE_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*3)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FIVE_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*5)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.TEN_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*10)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FIFTEEN_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*15)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.THIRTY_MINUTES:
			new_dt = start_dt + timedelta(minutes=count*30)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.ONE_HOUR:
			new_dt = start_dt + timedelta(hours=count)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.FOUR_HOURS:
			new_dt = start_dt + timedelta(hours=count*4)
			return new_dt >= end_dt or new_dt >= utcnow
		elif period == tl.period.DAILY:
			new_dt = start_dt + timedelta(hours=count*24)
			return new_dt >= end_dt or new_dt >= utcnow
		else:
			raise Exception('Period not found.')

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
		lotsize = abs(float(res.get('units')))
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


	def _handle_order_fill(self, res):
		# Retrieve position information
		oanda_id = res.get('id')
		result = {}

		ts = tl.convertTimeToTimestamp(datetime.strptime(
			res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
		))

		from_order = self.getOrderByID(res.get('orderID'))
		if from_order is not None:
			del self.orders[self.orders.index(from_order)]

		if res.get('tradeOpened'):
			order_id = res['tradeOpened'].get('tradeID')

			account_id = res['accountID']
			product = res.get('instrument')
			direction = tl.LONG if float(res['tradeOpened'].get('units')) > 0 else tl.SHORT
			lotsize = abs(float(res['tradeOpened'].get('units')))
			entry_price = float(res.get('price'))
			
			if res.get('reason') == 'LIMIT_ENTRY':
				order_type = tl.LIMIT_ENTRY
			elif res.get('reason') == 'STOP_ENTRY':
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
				cpy.lotsize = abs(float(res['tradeReduced'].get('units')))
				cpy.close_price = float(res['tradeReduced'].get('price'))
				cpy.close_time = tl.convertTimeToTimestamp(datetime.strptime(
					res.get('time').split('.')[0], '%Y-%m-%dT%H:%M:%S'
				))

				# Modify open position
				pos.lotsize += float(res['tradeReduced'].get('units'))

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
		endpoint = f'/v3/accounts/{account_id}/openTrades'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		if res.status_code == 200:
			result = {account_id: []}
			res = res.json()
			print(res)
			for pos in res.get('trades'):
				order_id = pos.get('id')
				product = pos.get('instrument')
				direction = tl.LONG if float(pos.get('currentUnits')) > 0 else tl.SHORT
				lotsize = abs(float(pos.get('currentUnits')))
				entry_price = float(pos.get('price'))
				sl = None
				sl_id = None
				if pos.get('stopLossOrder'):
					sl = float(pos['stopLossOrder'].get('price'))
					sl_id = pos['stopLossOrder'].get('id')
				tp = None
				tp_id = None
				if pos.get('takeProfitOrder'):
					tp = float(pos['takeProfitOrder'].get('price'))
					tp_id = pos['takeProfitOrder'].get('id')
				open_time = datetime.strptime(pos.get('openTime').split('.')[0], '%Y-%m-%dT%H:%M:%S')

				new_pos = tl.Position(
					self,
					order_id, account_id, product,
					tl.MARKET_ENTRY, direction, lotsize,
					entry_price, sl, tp, 
					tl.utils.convertTimeToTimestamp(open_time),
					sl_id=sl_id, tp_id=tp_id
				)

				result[account_id].append(new_pos)

			return result
		else:
			return None

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
			endpoint = f'/v3/accounts/{order["account_id"]}/trades/{order["order_id"]}/orders'
			res = self._session.put(
				self._url + endpoint,
				headers=self._headers,
				data=json.dumps(payload)
			)

			if 200 <= res.status_code < 300:
				res = res.json()
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

		# Flip lotsize if direction is short
		if direction == tl.SHORT: lotsize *= -1

		payload = {
			'order': {
				'instrument': product,
				'units': str(int(lotsize)),
				'type': 'MARKET',
				'timeInForce': 'FOK',
				'positionFill': 'DEFAULT',
			}
		}

		endpoint = f'/v3/accounts/{account_id}/orders'
		res = self._session.post(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
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
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyPosition(
				pos, sl_price, tp_price, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		payload = {}

		if sl_price is None:
			payload['stopLoss'] = {
				'timeInForce': 'GTC',
				'price': sl_price
			}

		elif sl_price != pos.sl:
			sl_price = str(round(sl_price, 5))
			payload['stopLoss'] = {
				'timeInForce': 'GTC',
				'price': sl_price
			}

		if tp_price is None:
			payload['takeProfit'] = {
				'timeInForce': 'GTC',
				'price': tp_price
			}

		elif tp_price != pos.tp:
			tp_price = str(round(tp_price, 5))
			payload['takeProfit'] = {
				'timeInForce': 'GTC',
				'price': tp_price
			}
				
		if len(payload):
			endpoint = f'/v3/accounts/{pos.account_id}/trades/{pos.order_id}/orders'
			res = self._session.put(
				self._url + endpoint,
				headers=self._headers,
				data=json.dumps(payload)
			)
		else:
			raise OrderException('No specified stop loss or take profit to modify.')

		result = {}
		status_code = res.status_code
		res = res.json()
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
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deletePosition(
				pos, lotsize, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		if lotsize >= pos.lotsize: units = 'ALL'
		else: units = str(int(lotsize))

		payload = {
			'units': units
		}

		endpoint = f'/v3/accounts/{pos.account_id}/trades/{pos.order_id}/close'
		res = self._session.put(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
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
		endpoint = f'/v3/accounts/{account_id}/pendingOrders'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		if res.status_code == 200:
			result = {account_id: []}
			res = res.json()
			for order in res.get('orders'):
				if order.get('type') == 'LIMIT' or order.get('type') == 'STOP':
					order_id = order.get('id')
					product = order.get('instrument')
					direction = tl.LONG if float(order.get('units')) > 0 else tl.SHORT
					lotsize =  abs(float(order.get('units')))
					entry_price = float(order.get('price'))
					sl = None
					if order.get('stopLossOnFill'):
						sl = float(order['stopLossOnFill'].get('price'))
					tp = None
					if order.get('takeProfitOnFill'):
						tp = float(order['takeProfitOnFill'].get('price'))
					open_time = datetime.strptime(order.get('createTime').split('.')[0], '%Y-%m-%dT%H:%M:%S')

					if order.get('type') == 'LIMIT':
						order_type = tl.LIMIT_ORDER
					elif order.get('type') == 'STOP':
						order_type = tl.STOP_ORDER

					new_order = tl.Order(
						self,
						order_id, account_id, product, order_type,
						direction, lotsize, entry_price, sl, tp,
						tl.convertTimeToTimestamp(open_time)
					)

					result[account_id].append(new_order)

			return result
		else:
			return None


	def getAllAccounts(self):
		endpoint = f'/v3/accounts'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		result = []
		status_code = res.status_code
		data = res.json()
		if 200 <= status_code < 300:
			for account in data['accounts']:
				result.append(account.get('id'))

			return result
		else:
			return None


	def getAccountInfo(self, account_id, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)

		endpoint = f'/v3/accounts/{account_id}'
		res = self._session.get(
			self._url + endpoint,
			headers=self._headers
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		if 200 <= status_code < 300:
			result[account_id] = {
				'currency': res['account'].get('currency'),
				'balance': float(res['account'].get('balance')),
				'pl': float(res['account'].get('pl')),
				'margin': float(res['account'].get('marginUsed')),
				'available': float(res['account'].get('balance')) + float(res['account'].get('pl'))
			}
			
		elif 400 <= status_code < 500:
			# Response error
			msg = 'No message available.'
			if res.get('errorMessage'):
				msg = res.get('errorMessage')

			raise BrokerException(msg)

		else:
			raise BrokerException('Oanda internal server error')

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

		# Flip lotsize if direction is short
		if direction == tl.SHORT: lotsize *= -1

		# Convert `entry_range` to `entry_price`
		if entry_range:
			entry_range = tl.convertToPrice(entry_range)
			if order_type == tl.LIMIT_ORDER:
				if direction == tl.LONG:
					entry_price = round(self.getAsk(product) - entry_range, 5)
				else:
					entry_price = round(self.getBid(product) + entry_range, 5)

			elif order_type == tl.STOP_ORDER:
				if direction == tl.LONG:
					entry_price = round(self.getAsk(product) + entry_range, 5)
				else:
					entry_price = round(self.getBid(product) - entry_range, 5)



		# Convert order_type to Oanda readable string
		payload_order_type = None
		if order_type == tl.LIMIT_ORDER:
			payload_order_type = 'LIMIT'
		elif order_type == tl.STOP_ORDER:
			payload_order_type = 'STOP'

		payload = {
			'order': {
				'price': str(entry_price),
				'instrument': product,
				'units': str(int(lotsize)),
				'type': payload_order_type,
				'timeInForce': 'FOK',
				'positionFill': 'DEFAULT',
			}
		}
		
		if sl_price:
			payload['order']['stopLossOnFill'] = {
				'price': str(round(sl_price, 5))
			}

		elif sl_range:
			sl_range = tl.convertToPrice(sl_range)
			if direction == tl.LONG:
				sl_price = round(entry_price + sl_range, 5)
			else:
				sl_price = round(entry_price - sl_range, 5)

			payload['order']['stopLossOnFill'] = {
				'price': str(sl_price)
			}

		if tp_price:
			payload['order']['takeProfitOnFill'] = {
				'price': str(round(tp_price, 5))
			}

		elif tp_range:
			tp_range = tl.convertToPrice(tp_range)
			if direction == tl.LONG:
				tp_price = round(entry_price + tp_range, 5)
			else:
				tp_price = round(entry_price - tp_range, 5)

			payload['order']['takeProfitOnFill'] = {
				'price': str(tp_price)
			}

		endpoint = f'/v3/accounts/{account_id}/orders'
		res = self._session.post(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
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
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyOrder(
				order, lotsize, entry_price, sl_price, tp_price, override=override
			)

		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		payload_order_type = None
		if order.order_type == tl.LIMIT_ORDER:
			payload_order_type = 'LIMIT'
		elif order.order_type == tl.STOP_ORDER:
			payload_order_type = 'STOP'


		payload = {
			'order': {
				'price': str(entry_price),
				'instrument': order.product,
				'units': str(int(lotsize)),
				'type': payload_order_type,
				'timeInForce': 'FOK',
				'positionFill': 'DEFAULT',
			}
		}

		if sl_price:
			payload['order']['stopLossOnFill'] = {
				'price': str(round(sl_price, 5))
			}

		if tp_price:
			payload['order']['takeProfitOnFill'] = {
				'price': str(round(tp_price, 5))
			}


		endpoint = f'/v3/accounts/{order.account_id}/orders/{order.order_id}'
		res = self._session.put(
			self._url + endpoint,
			headers=self._headers,
			data=json.dumps(payload)
		)

		result = {}
		status_code = res.status_code
		res = res.json()
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
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deleteOrder(order, override=override)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		endpoint = f'/v3/accounts/{order.account_id}/orders/{order.order_id}/cancel'
		res = self._session.put(
			self._url + endpoint,
			headers=self._headers
		)

		result = {}
		status_code = res.status_code
		res = res.json()
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
		for sub in self._subscriptions:
			for i in copy(sub.res):
				i.close()
				del sub.res[sub.res.index(i)]

			try:
				if sub.sub_type == Subscription.ACCOUNT:
					self._perform_account_connection(sub)
				elif sub.sub_type == Subscription.CHART:
					self._perform_chart_connection(sub)

			except requests.exceptions.ConnectionError:
				return


	def _encode_params(self, params):
		return urlencode(dict([(k, v) for (k, v) in iter(params.items()) if v]))


	def _subscribe_chart_updates(self, product, listener):
		sub = Subscription(Subscription.CHART, listener, [product])
		self._subscriptions.append(sub)
		self._perform_chart_connection(sub)	


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
		self._perform_chart_connection(sub)


	def onChartUpdate(self, chart, *args, **kwargs):
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

			# print(np.around((ask + bid)/2, decimals=5))
			# print(bid)
			# print(f'Ask: {ask}, Mid: {np.around((ask + bid)/2, decimals=5)}, Mid (^): {math.ceil((ask + bid)/2)}, Mid (v): {math.floor((ask + bid)/2)}, Bid: {bid}')

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
		sub = Subscription(Subscription.ACCOUNT, self._on_account_update, account_id)
		self._subscriptions.append(sub)
		self._perform_account_connection(sub)


	def _perform_account_connection(self, sub):
		endpoint = f'/v3/accounts/{sub.args[0]}/transactions/stream'
		req = Request(f'{self._stream_url}{endpoint}', headers=self._headers)
		
		try:
			stream = urlopen(req, timeout=20)

			sub.setStream(stream)
			Thread(target=self._stream_account_update, args=(sub,)).start()
		except Exception as e:
			time.sleep(1)
			Thread(target=self._perform_account_connection, args=(sub,)).start()
			return


	def _stream_account_update(self, sub):
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
		self._perform_account_connection(sub)


	def _on_account_update(self, update):
		res = {}
		if update.get('type') == 'HEARTBEAT':
			self._last_update = time.time()

		elif update.get('type') == 'ORDER_FILL':
			if self._handled.get(update.get('id')):
				res.update(self._wait(update.get('id')))

			else:
				res.update(self._handle_order_fill(update))

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
			self.handleOnTrade(res)


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
