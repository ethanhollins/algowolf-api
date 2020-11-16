from datetime import datetime, timedelta
import requests
import json
import time
import math
import random
import string
import io
import pandas as pd
import traceback
import numpy as np
from copy import copy
from urllib.parse import quote

from threading import Thread
from app.controller import DictQueue
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.error import BrokerException
from app.v1 import AccessLevel, key_or_login_required
from app.tradelib.brokers.lightstreamer_client import LightstreamerClient as LSClient
from app.tradelib.brokers.lightstreamer_client import LightstreamerSubscription as Subscription

TWO_HOURS = 60*60*2

# Priority queue that groups by account id
class Working(list):

	def append(self, item):
		super().append(item)
		# self.sort(key=lambda x: x[0] if x[0] != None else '')

	def run(self, broker, account_id, target, args, kwargs):
		_id = ''.join(random.choice(string.ascii_lowercase) for i in range(4))
		# Add item to working list
		item = (account_id, _id)
		self.append(item)
		# Wait for account id to be at top of list
		while self[0][1] != _id:
			time.sleep(0.1)

		if account_id is not None:
			broker._switch_account(account_id)

		# Execute command
		result = target(*args, **kwargs)
		# del item from list once complete
		del self[self.index(item)]

		return result

class IG(Broker):

	__slots__ = (
		'dl', '_c_account', '_last_token_update', 'is_demo', '_headers', '_url', 
		'_creds', '_ls_endpoint', '_working', '_hist_download_queue', '_ls_client', 
		'_subscriptions', '_temp_data', '_last_refresh', '_last_transaction_ts'
	)
	def __init__(self, 
		ctrl, username, password, key, is_demo, 
		user_account=None, broker_id=None, accounts={}, display_name=None
	):
		super().__init__(ctrl, user_account, broker_id, tl.broker.IG_NAME, accounts, display_name)

		self.dl = tl.DataLoader(broker=self)

		self._c_account = None
		self._last_refresh = time.time()
		self._last_transaction_ts = time.time()
		self.is_demo = is_demo

		self._headers = {
			'Content-Type': 'application/json; charset=UTF-8',
			'Accept': 'application/json; charset=UTF-8',
			'X-IG-API-KEY': key, 
			'Version': '2',
			'X-SECURITY-TOKEN': '',
			'CST': ''
		}
		self._url = (
			'https://demo-api.ig.com/gateway/deal/'
			if is_demo else
			'https://api.ig.com/gateway/deal/'
		)
		self._creds = {
			'identifier': username,
			'password': password,
			'encryptedPassword': None
		}
		self._ls_endpoint = (
			'https://demo-apd.marketdatasystems.com'
			if is_demo else
			'https://apd.marketdatasystems.com'
		)

		# Dealing vars
		self._working = Working()
		self._hist_download_queue = DictQueue()

		self._get_tokens()

		# Handle strategy
		if self.userAccount and self.brokerId:
			self._handle_live_strategy_setup()

		# Live Updates
		self._ls_client = self._connect()
		self._subscriptions = []

		for acc in self.getAccounts():
			self._subscribe_account_updates(acc)

		self._temp_data = pd.DataFrame(columns=[
			'timestamp', 
			'ask_open', 'ask_high', 'ask_low', 'ask_close', 
			'bid_open', 'bid_high', 'bid_low', 'bid_close'
		]).set_index('timestamp')

		# Start refresh thread
		Thread(target=self._periodic_refresh).start()

	def _periodic_refresh(self):
		while self.is_running:
			if time.time() - self._last_refresh > TWO_HOURS:
				print('PERIODIC REFRESH')
				# Perform periodic refresh
				self._working.run(
					self, None,
					self._token_refresh,
					(), {}
				)

			time.sleep(1)

	'''
	Broker Utilities
	'''

	def _token_refresh(self):
		try:
			self._get_tokens(account_id=self._c_account)
			self._last_refresh = time.time()
			self._reconnect()
			return True
		except requests.exceptions.ConnectionError as e:
			print(e)
		except Exception as e:
			print(traceback.format_exc())

		return False


	def _get_tokens(self, account_id=None, attempts=0):
		endpoint = 'session'
		self._headers['Version'] = '2'
		self._headers['X-SECURITY-TOKEN'] = ''
		self._headers['CST'] = ''

		res = requests.post(
			self._url + endpoint, 
			data=json.dumps(self._creds),
			headers=self._headers
		)

		if res.status_code == 200:
			if res.headers.get('X-SECURITY-TOKEN'):
				self._headers['X-SECURITY-TOKEN'] = res.headers.get('X-SECURITY-TOKEN')
			if res.headers.get('CST'):
				self._headers['CST'] = res.headers.get('CST')

			self._c_account = res.json().get('currentAccountId')
			self._last_token_update = datetime.utcnow()

			if account_id:
				self._switch_account(account_id)

			return True
		elif res.status_code == 403:
			raise BrokerException('({}) Api key is invalid.\n{}'.format(
				res.status_code, json.dumps(res.json(), indent=2)
			))
		else:
			print('[IG] ({}) Retrying tokens.'.format(res.status_code))
			if attempts >= 5:
				raise BrokerException('({}) Unable to retrieve tokens. Exceeded max attempts.\n{}'.format(
					res.status_code, json.dumps(res.json(), indent=2)
				))
			else:
				print('[IG] Re-attempting token retrieval ({})'.format(attempts))
				return self._get_tokens(account_id, attempts=attempts+1)

	def _switch_account(self, account_id, attempts=0):
		if self._c_account == account_id:
			return True

		endpoint = 'session'
		self._headers['Version'] = '1'
		payload = {
			"accountId": account_id,
			"defaultAccount": None
		}
		res = requests.put(
			self._url + endpoint,
			data=json.dumps(payload),
			headers=self._headers
		)

		if res.status_code == 200 or res.status_code == 412:
			self._c_account = account_id
			if res.headers.get('X-SECURITY-TOKEN'):
				self._headers['X-SECURITY-TOKEN'] = res.headers.get('X-SECURITY-TOKEN')
			return True

		elif res.status_code == 401:
			print('[IG] Re-collecting Tokens ({0})'.format(res.status_code))
			return self._get_tokens(account_id)

		else:
			print('[IG] Error switching account ({0}):\n{1}'.format(res.status_code, res.json()))
			if attempts >= 5:
				raise BrokerException('({}) Unable to switch accounts. Exceeded max attempts.\n{}'.format(
					res.status_code, json.dumps(res.json(), indent=2)
				))
			else:
				attempts += 1
				print('[IG] [{0}] Reattempting account switch ({1})'.format(account_id, attempts))
				return self._switch_account(account_id, attempts=attempts+1)


	def _download_historical_data(self, product, period, start=None, end=None, count=None, force_download=False):
		# return self._hist_download_queue.handle(
		# 	f'{product}:{period}', self._perform_download_historical_data,
		# 	product, period, start=start, end=end, count=count, 
		# 	force_download=force_download
		# )

		return self._perform_download_historical_data(
			product, period, start=start, end=end, count=count, 
			force_download=force_download
		)


	def _perform_download_historical_data(self, product, period, start=None, end=None, count=None, force_download=False):
		df = self._create_empty_df()		
		result = {}
		page_number = 0
		ig_product = self._convert_to_main_ig_product(product)
		ig_period = self._convert_to_ig_period(period)
		while True:
			if start and end:
				start = tl.utils.convertTimezone(start, 'Australia/Melbourne')
				end = tl.utils.convertTimezone(end, 'Australia/Melbourne')
				if not force_download:
					df = self._load_data(product, period, start, end)
					if df.size > 0:
						start = tl.utils.convertTimezone(
							tl.convertTimestampToTime(df.index.values[-1]),
							'Australia/Melbourne'
						)
						print(start)

				endpoint = 'prices/{}?resolution={}&from={}&to={}&pageSize=5000&pageNumber={}'.format(
					ig_product, ig_period,
					start.strftime("%Y-%m-%dT%H:%M:%S"),
					end.strftime("%Y-%m-%dT%H:%M:%S"),
					page_number
				)
				version = {'Version': '3'}

			elif count:
				endpoint = 'prices/{}/{}/{}'.format(
					ig_product, ig_period,
					count
				)
				version = {'Version': '2'}

			print(endpoint)
			res = requests.get(
				self._url + endpoint,
				headers={**self._headers, **version}
			)

			if res.status_code == 200:
				if len(result) == 0:
					result['timestamp'] = []
					result['ask_open'] = []
					result['ask_high'] = []
					result['ask_low'] = []
					result['ask_close'] = []
					result['bid_open'] = []
					result['bid_high'] = []
					result['bid_low'] = []
					result['bid_close'] = []

				data = res.json()

				start = time.time()
				for price in data['prices']:
					if 'snapshotTimeUTC' in price:
						dt = datetime.strptime(price['snapshotTimeUTC'], '%Y-%m-%dT%H:%M:%S')
						ts = tl.utils.convertTimeToTimestamp(dt)
					else:
						dt = datetime.strptime(price['snapshotTime'], '%Y/%m/%d %H:%M:%S')
						dt = tl.utils.setTimezone(dt, 'Australia/Melbourne')
						ts = tl.utils.convertTimeToTimestamp(dt)

					result['timestamp'].append(int(ts))

					price_keys = ['openPrice', 'highPrice', 'lowPrice', 'closePrice']
					asks = [price[i]['ask'] for i in price_keys]
					bids = [price[i]['bid'] for i in price_keys]

					result['ask_open'].append(asks[0])
					result['ask_high'].append(asks[1])
					result['ask_low'].append(asks[2])
					result['ask_close'].append(asks[3])
					result['bid_open'].append(bids[0])
					result['bid_high'].append(bids[1])
					result['bid_low'].append(bids[2])
					result['bid_close'].append(bids[3])

				if 'metadata' in data:
					page_number = data['metadata']['pageData']['pageNumber']
					total_pages = data['metadata']['pageData']['totalPages']

					if page_number < total_pages:
						page_number += 1
						continue
					else:
						break
				else:
					break

			else:
				raise BrokerException('({}) Unable to retrieve historical prices.\n{}'.format(
					res.status_code, json.dumps(res.json(), indent=2)
				))

		# Concatenate loaded data
		if len(result) > 0:
			new_data = pd.DataFrame(data=result).set_index('timestamp').astype(np.float64)

			if not force_download and not self.is_demo:
				self.save_data(new_data.copy(), product, period)
				# Thread(
				# 	target=self.save_data, 
				# 	args=(new_data.copy(), product, period)
				# ).start()
			df = pd.concat((df, new_data)).sort_index()

		return self._process_df(df)

	def _create_empty_df(self):
		columns = [
			'timestamp', 
			'ask_open', 'ask_high', 'ask_low', 'ask_close', 
			'bid_open', 'bid_high', 'bid_low', 'bid_close'
		]
		return pd.DataFrame(columns=columns).set_index('timestamp')

	def _process_df(self, df):
		# Remove duplicates
		df = df[~df.index.duplicated(keep='first')]
		# Replace NaN with None
		# df = df.where(pd.notnull(df), None)
		df = df.dropna()
		# Round to 5 decimal places
		df = df.round(pd.Series([5]*8, index=df.columns))
		return df

	def _load_data(self, product, period, start, end):
		frags = []
		# Loop through each year
		for y in range(start.year, end.year+1):
			if y == start.year:
				ts_start = tl.utils.convertTimeToTimestamp(start)
			else:
				ts_start = tl.utils.convertTimeToTimestamp(datetime(year=y, month=1, day=1))

			if y == end.year:
				ts_end = tl.utils.convertTimeToTimestamp(end)
			else:
				ts_end = tl.utils.convertTimeToTimestamp(datetime(year=y+1, month=1, day=1))

			df = self.ctrl.getDb().getPrices(self.name, product, period, y)
			if isinstance(df, pd.DataFrame):
				# Get correct time range
				df = df.loc[(ts_start <= df.index) & (df.index < ts_end)]
				if df.size == 0: continue

				frags.append(df)

		if len(frags):
			# Concatenate loaded data
			result = pd.concat(frags).sort_index()
			return self._process_df(df)
		else:
			return self._create_empty_df()

	def save_data(self, df, product, period):
		if df.size == 0: return
		# if period != tl.period.ONE_MINUTE: return
		
		MAX_DOWNLOAD = 10000

		# Get start and end dates
		start = tl.utils.convertTimestampToTime(df.index.values[0])

		if tl.utils.isCurrentBar(period, df.index.values[-1]):
			df.drop(df.tail(1).index, inplace=True)
			if df.size == 0: return

		end = tl.utils.convertTimestampToTime(df.index.values[-1])

		# Load saved prices
		old_df = self.ctrl.getDb().getPrices(self.name, product, period, start.year)
		if isinstance(old_df, pd.DataFrame):
			old_df_end = tl.utils.convertTimestampToTime(old_df.index.values[-1])
	
			# Check if missing data doesn't exceed MAX_DOWNLOAD
			date_count = tl.utils.getDateCount(period, old_df_end, start)
			missing_df = self._create_empty_df()
			if date_count > 1 and date_count <= MAX_DOWNLOAD:
				# Get missing prices
				missing_df = self._download_historical_data(product, period, start=old_df_end, end=start, force_download=True)

			# Concatenate new data with old data
			df = pd.concat((old_df, missing_df, df)).sort_index()
		# Process DataFrame
		df = self._process_df(df)

		# Loop through each year
		for y in range(start.year, end.year+1):
			ts_start = tl.utils.convertTimeToTimestamp(datetime(year=y, month=1, day=1))
			ts_end = tl.utils.convertTimeToTimestamp(datetime(year=y+1, month=1, day=1))

			# Get correct time range
			t_data = df.loc[(ts_start <= df.index) & (df.index < ts_end)]
			if t_data.size == 0: continue

			# Set data types
			t_data.index = t_data.index.map(int)
			
			# Upload prices
			self.ctrl.getDb().updatePrices(self.name, product, period, y, t_data)

	def handle_live_data_save(self, res):
		SAVE_AT = 60
		self._temp_data.loc[res['timestamp']] = sum(res['item'].values(), [])

		if self._temp_data.shape[0] >= SAVE_AT:
			# Get data copy
			df = self._temp_data.copy()

			# Reset data store
			self._temp_data = self._create_empty_df()

			if not self.is_demo:
				self.save_data(df, res['product'], res['period'])

	'''
	Account Utilities
	'''

	def _get_account_details(self, account_id, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)

		endpoint = 'accounts'
		version = { 'Version': '1' }
		# Add command to working queue
		res = requests.get(
			self._url + endpoint, 
			headers={ **self._headers, **version }
		)

		if res.status_code == 200:
			result = {}
			for account in res.json()['accounts']:
				if account['accountId'] == account_id:
					result[account['accountId']] = {
						'currency': account.get('currency'),
						'balance': account['balance'].get('balance'),
						'pl': account['balance'].get('profitLoss'),
						'margin': account['balance'].get('deposit'),
						'available': account['balance'].get('available')
					}
					break
			return result 
		else:
			raise BrokerException('({}) Unable to get account details.\n{}'.format(
				res.status_code, json.dumps(res.json(), indent=2)
			))

	'''
	Dealing Utilities
	'''

	def _get_all_positions(self, account_id):
		endpoint = 'positions'
		self._headers['Version'] = '2'
		# Add command to working queue
		res = self._working.run(
			self, account_id,
			requests.get,
			(self._url + endpoint,),
			{ 'headers': self._headers }
		)

		if res.status_code == 200:
			result = {account_id: []}
			res = res.json()
			for pos in res['positions']:
				if pos['position'].get('size') is not None:
					lotsize = pos['position']['size']
				else:
					lotsize = pos['position']['dealSize']

				if pos['position'].get('level') is not None:
					entry_price = pos['position']['level']
				else:
					entry_price = pos['position']['openLevel']


				if pos['position']['stopLevel']: 
					sl = pos['position']['stopLevel']
				else:
					sl = None

				if pos['position']['limitLevel']:
					tp = pos['position']['limitLevel']
				else:
					tp = None

				if pos['position'].get('createdDateUTC'):
					open_time = tl.utils.convertTimeToTimestamp(
						datetime.strptime(
							pos['position']['createdDateUTC'],
							'%Y-%m-%dT%H:%M:%S'
						)
					)

				else:
					open_time = tl.utils.convertTimeToTimestamp(
						tl.utils.setTimezone(datetime.strptime(
							':'.join(pos['position']['createdDate'].split(':')[:-1]),
							'%Y/%m/%d %H:%M:%S'
						), 'Australia/Melbourne')
					)
					

				new_pos = tl.Position(
					self,
					pos['position']['dealId'], account_id,
					self._convert_to_standard_product(pos['market']['epic']), tl.MARKET_ENTRY,
					self._convert_to_standard_direction(pos['position']['direction']),
					lotsize, entry_price=entry_price, sl=sl, tp=tp,
					open_time=open_time
				)

				result[account_id].append(new_pos)

			return result
		else:
			raise BrokerException('Unable to retreive all positions ({}).\n{}'.format(account_id, res.json()))

	def _get_position(self, account_id, order_id):

		endpoint = 'positions/{}'.format(order_id)
		self._headers['Version'] = '2'
		# Add command to working queue
		res = self._working.run(
			self, account_id,
			requests.get,
			(self._url + endpoint,),
			{ 'headers': self._headers }
		)
		
		if res.status_code == 200:
			return res.json()
		elif res.status_code == 404:
			return None
		else:
			raise BrokerException('({}) Unable to retreive position ({}).\n{}'.format(
				res.status_code, account_id, json.dumps(res.json(), indent=2)
			))

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

		# Switch Account
		# self._switch_account(account_id)

		product = self._convert_to_ig_product(product)
		direction = self._convert_to_ig_direction(direction)

		endpoint = 'positions/otc'
		payload = {
			"epic": product,
			"expiry": "-",
			"direction": direction,
			"size": lotsize,
			"orderType": "MARKET",
			"timeInForce": "EXECUTE_AND_ELIMINATE",
			"level": None,
			"guaranteedStop": "false", # TODO: find if gslo account
			"stopLevel": sl_price,
			"stopDistance": sl_range,
			"trailingStop": "false",
			"trailingStopIncrement": None,
			"forceOpen": "true",
			"limitLevel": tp_price,
			"limitDistance": tp_range,
			"quoteId": None,
			"currencyCode": "USD"
		}

		self._headers['Version'] = '2'
		# Add command to working queue
		res = self._working.run(
			self, account_id,
			requests.post,
			(self._url + endpoint,),
			{
				'data': json.dumps(payload),
				'headers': self._headers
			}
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		if status_code == 200:
			ref = res['dealReference']
			status = 400

			# Wait for live callback
			timeout = 60*10 if self.is_demo else 5
			result = self._wait(ref, timeout=timeout)

			if result is None:
				result = {
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MARKET_ORDER,
						'accepted': False,
						'message': 'IG internal server error.'
					}
				}

		elif 400 <= status_code < 500:
			err = res.get('errorCode')
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MARKET_ORDER,
					'accepted': False,
					'message': err
				}
			})

		else:
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MARKET_ORDER,
					'accepted': False,
					'message': 'IG internal server error.'
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

		# Switch Account
		# self._switch_account(pos.account_id)

		endpoint = 'positions/otc/{}'.format(pos.order_id)
		payload = {
			"stopLevel": sl_price,
			"limitLevel": tp_price,
			"trailingStop": "false",
			"trailingStopDistance": None,
			"trailingStopIncrement": None
		}

		self._headers['Version'] = '2'
		# Add command to working queue
		res = self._working.run(
			self, pos.account_id,
			requests.put,
			(self._url + endpoint,),
			{
				'data': json.dumps(payload),
				'headers': self._headers
			}
		)

		result = {}
		status_code = res.status_code
		res = res.json()
		if status_code == 200:
			ref = res['dealReference']
			status = 400

			# Wait for live callback
			timeout = 60*10 if self.is_demo else 5
			result = self._wait(ref, timeout=timeout)

			if result is None:
				result = {
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.MODIFY,
						'accepted': False,
						'message': 'IG internal server error.',
						'item': {
							'order_id': pos.order_id
						}
					}
				}

		elif 400 <= status_code < 500:
			err = res.get('errorCode')
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.MODIFY,
					'accepted': False,
					'message': err,
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
					'message': 'IG internal server error.',
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

		# Switch Account
		# self._switch_account(pos.account_id)

		endpoint = 'positions/otc'
		payload = {
			"dealId": pos.order_id,
			"epic": None,
			"expiry": None,
			"size": lotsize,
			"level": None,
			"orderType": "MARKET",
			"timeInForce": None,
			"quoteId": None
		}

		if pos.direction == tl.LONG:
			payload['direction'] = 'SELL'
		else:
			payload['direction'] = 'BUY'

		self._headers['Version'] = '1'
		self._headers['_method'] = 'DELETE'
		# Add command to working queue
		res = self._working.run(
			self, pos.account_id,
			requests.post,
			(self._url + endpoint,),
			{
				'data': json.dumps(payload),
				'headers': self._headers
			}
		)

		self._headers.pop('_method', None)

		result = {}
		status_code = res.status_code
		res = res.json()
		if status_code == 200:
			ref = res['dealReference']
			status = 400

			# Wait for live callback
			timeout = 60*10 if self.is_demo else 5
			result = self._wait(ref, timeout=timeout)

			if result is None:
				result = {
					self.generateReference(): {
						'timestamp': math.floor(time.time()),
						'type': tl.POSITION_CLOSE,
						'accepted': False,
						'message': 'IG internal server error.',
						'item': {
							'order_id': pos.order_id
						}
					}
				}

		elif 400 <= status_code < 500:
			err = res.get('errorCode')
			result.update({
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': err,
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
					'message': 'IG internal server error.',
					'item': {
						'order_id': pos.order_id
					}
				}
			})

		return result


	def _get_all_orders(self, account_id):

		endpoint = 'workingorders'
		self._headers['Version'] = '2'

		res = self._working.run(
			self, account_id,
			requests.get,
			(self._url + endpoint,),
			{ 'headers': self._headers }
		)

		if res.status_code == 200:
			return {account_id: []}
		else:
			raise BrokerException('({}) Unable to get all orders.\n{}'.format(
				res.status_code, json.dumps(res.json(), indent=2)
			))

	def _get_order(self):
		return # TODO: Use _get_all_orders to get specific order

	def createOrder(self):
		return

	def modifyOrder(self):
		return

	def deleteOrder(self):
		return


	def _handle_transaction(self, account_id, trans, ref=None):

		order_id = trans.get('dealId')
		ts = datetime.strptime(trans.get('date'), '%Y-%m-%dT%H:%M:%S').timestamp()
		product = self._convert_to_standard_product(trans.get('epic'))

		trans_match = None

		for i in trans['details']['actions']:

			deal_reference = trans['details'].get('dealReference')
			direction = trans['details'].get('direction')
			lotsize = trans['details'].get('size')
			price = trans['details'].get('level')
			sl = trans['details'].get('stopLevel')
			tp = trans['details'].get('limitLevel')

			affected_order_id = i.get('affectedDealId')

			if i.get('actionType') == 'POSITION_OPENED':
				if not order_id in [pos.order_id for pos in self.getAllPositions()]:
					pos = tl.Position(
						self, order_id, account_id, product, tl.MARKET_ENTRY,
						direction, lotsize, entry_price=price, sl=sl, tp=tp, open_time=ts
					)
					self.positions.append(pos)
					print(self.positions)
					ref = self.generateReference()
					res = {
						ref: {
							'timestamp': pos.open_time,
							'type': tl.MARKET_ENTRY,
							'accepted': True,
							'item': pos
						}
					}

					self.handleOnTrade(res)
					self._handled[ref] = res

			elif i.get('actionType') == 'POSITION_CLOSED':
				for pos in copy(self.positions):
					if pos.order_id == affected_order_id:
						pos.close_price = price
						pos.close_time = ts

						del self.positions[self.positions.index(pos)]

						order_type = self._check_sl_tp_hit(pos)

						ref = self.generateReference()
						res = {
							ref: {
								'timestamp': pos.close_time,
								'type': order_type,
								'accepted': True,
								'item': pos
							}
						}

						self.handleOnTrade(res)
						self._handled[ref] = res

			elif i.get('actionType') == 'STOP_LIMIT_AMENDED':
				for pos in copy(self.positions):
					if pos.order_id == order_id:
						pos.sl = sl
						pos.tp = tp

						ref = self.generateReference()
						res = {
							ref: {
								'timestamp': ts,
								'type': tl.MODIFY,
								'accepted': True,
								'item': pos
							}
						}

						self.handleOnTrade(res)
						self._handled[ref] = res

			if ref is not None and deal_reference == ref:
				trans_match = res

		return trans_match


	def _handle_transactions(self, account_id, ref=None):
		self._switch_account(account_id)

		start = tl.utils.convertTimestampToTime(self._last_transaction_ts)
		end = datetime.now()

		endpoint = 'history/activity'

		res = requests.get(
			self._url + endpoint,
			headers={**self._headers, **{ 'Version': '3' }},
			params={
				'from': start.strftime("%Y-%m-%dT%H:%M:%S"),
				'to': end.strftime("%Y-%m-%dT%H:%M:%S"),
				'detailed': True
			}
		)

		trans_match = None
		status_code = res.status_code
		if status_code == 200:
			data = res.json()
			if 'activities' in data:
				ts = self._last_transaction_ts
				for i in data['activities'][::-1]:
					ts = datetime.strptime(i.get('date'), '%Y-%m-%dT%H:%M:%S').timestamp()
					if i.get('status') == 'ACCEPTED' and ts > self._last_transaction_ts:
						trans_search = self._handle_transaction(account_id, i, ref=ref)
						if trans_search is not None:
							trans_match = trans_search

				self._last_transaction_ts = ts

		return trans_match

	'''
	Live Utilities
	'''

	# Connectivity Functionality
	def _connect(self):
		while True:
			ls_client = LSClient(
				self,
				self._creds.get('identifier'),
				'CST-{}|XST-{}'.format(
					self._headers['CST'],
					self._headers['X-SECURITY-TOKEN']
				),
				self._ls_endpoint
			)

			try:
				ls_client.connect()
				return ls_client
			except Exception as e:
				time.sleep(1)


	def _reconnect(self):
		print('RECONNECTING')
		# Regenerate tokens
		old_ls_client = self._ls_client
		try:
			old_ls_client.disconnect()
		except Exception:
			print(traceback.format_exc())

		while True:
			self._ls_client = LSClient(
				self,
				self._creds.get('identifier'),
				'CST-{}|XST-{}'.format(
					self._headers['CST'],
					self._headers['X-SECURITY-TOKEN']
				),
				self._ls_endpoint
			)

			try:
				self._ls_client.connect(wait=False)
				break
			except Exception as e:
				print(traceback.format_exc())
				time.sleep(1)
				pass

		for sub in self._subscriptions:
			self._subscribe(*sub)
		
		# Check any missed transactions
		for acc in self.getAccounts():
			self._handle_transactions(acc)


	def _subscribe(self, mode, items, fields, listener):
		subscription = Subscription(
			mode, items, fields
		)

		subscription.addListener(listener)
		self._ls_client.subscribe(subscription)
		return subscription

	def _subscribe_account_updates(self, account_id):
		if account_id != tl.broker.PAPERTRADER_NAME:
			sub = (
				'DISTINCT',
				['TRADE:{}'.format(account_id)],
				['OPU', 'CONFIRMS'],
				self._on_account_update
			)
			self._subscribe(*sub)
			self._subscriptions.append(sub)

	# Update Handlers
	def _on_account_update(self, update):
		if update['values'].get('CONFIRMS'):
			try:
				update['values']['CONFIRMS'] = json.loads(update['values']['CONFIRMS'])
			except:
				pass
			self._handle_confirms(update)

		if update['values'].get('OPU'):
			try:
				update['values']['OPU'] = json.loads(update['values']['OPU'])
			except:
				pass
			self._handle_opu(update)

		# elif item['values'].get('WOU'):
		# 	wou = json.loads(item['values']['WOU'])
		# 	self._handle_wou(wou)

	def _handle_confirms(self, confirms):
		item = confirms['values']['CONFIRMS']
		if item['dealStatus'] == 'REJECTED':
			self._handled[item['dealReference']] = {
				self.generateReference(): {
					'timestamp': math.floor(time.time()),
					'accepted': False
				}
			}
			

	def _handle_opu(self, opu):
		item = opu['values']['OPU']
		account_id = opu['name'].split(':')[1]

		print(f'IG: {item}')
		
		if item['dealStatus'] == 'ACCEPTED':
			ref = item['dealReference']

			# On Position/Order Opened
			if item['status'] == 'OPEN':

				# ORDER
				if item.get('orderType'):
					order_id = item['dealId']
					
					if not order_id in [order.order_id for order in self.getAllOrders()]:
						product = self._convert_to_standard_product(item['epic'])
						direction = self._convert_to_standard_direction(item['direction'])
						lotsize = item['size']

						order_type = self._convert_to_standard_order_type(item['orderType'])
						
						entry = item['level']

						# Calculate Stop Loss
						if item.get('stopLevel'):
							sl = item.get('stopLevel')
						else:
							sl = item.get('stopDistance')

							if sl:
								if direction == tl.LONG:
									sl = round(entry - tl.utils.convertToPrice(sl), 5)
								else:
									sl = round(entry + tl.utils.convertToPrice(sl), 5)

						# Calculate Take Profit
						if item.get('limitLevel'):
							tp = item.get('limitLevel')
						else:
							tp = item.get('limitDistance')

							if tp:
								if direction == tl.LONG:
									tp = round(entry + tl.utils.convertToPrice(tp), 5)
								else:
									tp = round(entry - tl.utils.convertToPrice(tp), 5)

						dt = datetime.strptime(item['timestamp'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
						open_time = int(tl.utils.convertTimeToTimestamp(dt))

						order = tl.Order(
							self, order_id, account_id, product, order_type, 
							direction, lotsize, entry_price=entry, sl=sl, tp=tp, open_time=open_time
						)

						self.orders.append(order)

						res = {
							self.generateReference(): {
								'timestamp': open_time,
								'type': order_type,
								'accepted': True,
								'item': order
							}
						}
						self.handleOnTrade(res)

						self._handled[ref] = res
						self._last_transaction_ts = open_time
				
				# POSITION
				else:					
					order_id = item['dealId']
					
					if not order_id in [pos.order_id for pos in self.getAllPositions()]:
						product = self._convert_to_standard_product(item['epic'])
						direction = self._convert_to_standard_direction(item['direction'])
						lotsize = item['size']
						
						entry = item['level']

						# Calculate Stop Loss
						if item.get('stopLevel'):
							sl = item.get('stopLevel')
						else:
							sl = item.get('stopDistance')

							if sl:
								if direction == tl.LONG:
									sl = round(entry - tl.utils.convertToPrice(sl), 5)
								else:
									sl = round(entry + tl.utils.convertToPrice(sl), 5)

						# Calculate Take Profit
						if item.get('limitLevel'):
							tp = item.get('limitLevel')
						else:
							tp = item.get('limitDistance')

							if tp:
								if direction == tl.LONG:
									tp = round(entry + tl.utils.convertToPrice(tp), 5)
								else:
									tp = round(entry - tl.utils.convertToPrice(tp), 5)

						dt = datetime.strptime(item['timestamp'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
						open_time = int(tl.utils.convertTimeToTimestamp(dt))

						order_type = tl.MARKET_ENTRY
						pos = tl.Position(
							self, order_id, account_id, product, order_type,
							direction, lotsize, entry_price=entry, sl=sl, tp=tp, open_time=open_time
						)

						# Check if position entry is from order
						for i in range(len(self.orders)):
							order = self.orders[i]
							if order.order_id == pos.order_id:
								order.close_price = pos.entry_price
								order.close_time = pos.open_time
								if order.order_type == tl.STOP_ORDER:
									order_type = tl.STOP_ENTRY
								elif order.order_type == tl.LIMIT_ORDER:
									order_type = tl.LIMIT_ENTRY
								del self.orders[i]
								break

						self.positions.append(pos)

						res = {
							self.generateReference(): {
								'timestamp': open_time,
								'type': order_type,
								'accepted': True,
								'item': pos
							}
						}
						self.handleOnTrade(res)
						self._handled[ref] = res
						self._last_transaction_ts = open_time


			# On Position/Order Deleted
			elif item['status'] == 'DELETED':

				# ORDER
				if item.get('orderType'):
					order_id = item['dealId']
					for i in range(len(self.orders)):
						order = self.orders[i]

						# Apply order changes
						if order.order_id == order_id:
							order.close_price = item['level']

							dt = datetime.strptime(item['timestamp'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
							order.close_time = int(tl.utils.convertTimeToTimestamp(dt))

							del self.orders[i]

							res = {
								self.generateReference(): {
									'timestamp': order.close_time,
									'type': tl.ORDER_CLOSE,
									'accepted': True,
									'item': order
								}
							}

							self.handleOnTrade(res)
							self._handled[ref] = res
							self._last_transaction_ts = order.close_time

							return

				# POSITION
				else:
					order_id = item['dealId']
					for i in range(len(self.positions)):
						pos = self.positions[i]

						# Apply position changes
						if pos.order_id == order_id:
							pos.close_price = item['level']

							dt = datetime.strptime(item['timestamp'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
							pos.close_time = int(tl.utils.convertTimeToTimestamp(dt))

							order_type = self._check_sl_tp_hit(pos)

							del self.positions[i]

							res = {
								self.generateReference(): {
									'timestamp': pos.close_time,
									'type': order_type,
									'accepted': True,
									'item': pos
								}
							}

							self.handleOnTrade(res)
							self._handled[ref] = res
							self._last_transaction_ts = pos.close_time

							return

			# On Position/Order Modified
			elif item['status'] == 'UPDATED':
				
				# ORDER
				if item.get('orderType'):
					order_id = item['dealId']
					for order in self.orders:
						# Apply order changes
						if order.order_id == order_id:
							order.entry_price = item['level']

							# Calculate Stop Loss
							if item.get('stopLevel'):
								sl = item.get('stopLevel')
							else:
								sl = item.get('stopDistance')

								if sl:
									if direction == tl.LONG:
										sl = round(entry - tl.utils.convertToPrice(sl), 5)
									else:
										sl = round(entry + tl.utils.convertToPrice(sl), 5)

							# Calculate Take Profit
							if item.get('limitLevel'):
								tp = item.get('limitLevel')
							else:
								tp = item.get('limitDistance')

								if tp:
									if direction == tl.LONG:
										tp = round(entry + tl.utils.convertToPrice(tp), 5)
									else:
										tp = round(entry - tl.utils.convertToPrice(tp), 5)

							order.sl = sl
							order.tp = tp

							res = {
								self.generateReference(): {
									'timestamp': math.floor(time.time()),
									'type': tl.MODIFY,
									'accepted': True, 
									'item': order
								}
							}

							self.handleOnTrade(res)
							self._handled[ref] = res
							self._last_transaction_ts = math.floor(time.time())

							return

				# POSITION
				else:
					order_id = item['dealId']
					for pos in self.positions:
						# Apply position changes
						if pos.order_id == order_id:
							if float(item['size']) != pos.lotsize:
								# Create position copy with closed lotsize and price/time
								cpy = tl.Position.fromDict(self, dict(pos))
								cpy.lotsize = (pos.lotsize - float(item['size']))
								cpy.close_price = item['level']

								dt = datetime.strptime(item['timestamp'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
								cpy.close_time = tl.utils.convertTimeToTimestamp(dt)

								pos.lotsize = float(item['size'])

								res = {
									self.generateReference(): {
										'timestamp': cpy.close_time,
										'type': tl.POSITION_CLOSE,
										'accepted': True,
										'item': cpy
									}
								}

								self.handleOnTrade(res)
								self._handled[ref] = res
								self._last_transaction_ts = cpy.close_time

							else:
								# Calculate Stop Loss
								if item.get('stopLevel'):
									sl = item.get('stopLevel')
								else:
									sl = item.get('stopDistance')

									if sl:
										if direction == tl.LONG:
											sl = round(entry - tl.utils.convertToPrice(sl), 5)
										else:
											sl = round(entry + tl.utils.convertToPrice(sl), 5)

								# Calculate Take Profit
								if item.get('limitLevel'):
									tp = item.get('limitLevel')
								else:
									tp = item.get('limitDistance')

									if tp:
										if direction == tl.LONG:
											tp = round(entry + tl.utils.convertToPrice(tp), 5)
										else:
											tp = round(entry - tl.utils.convertToPrice(tp), 5)

								pos.sl = sl
								pos.tp = tp

								res = {
									self.generateReference(): {
										'timestamp': math.floor(time.time()),
										'type': tl.MODIFY,
										'accepted': True,
										'item': pos
									}
								}

								self.handleOnTrade(res)
								self._handled[ref] = res
								self._last_transaction_ts = math.floor(time.time())

							return

	def _handle_wou(self, wou):
		print('WOU:\n{}'.format(wou))
		return

	def _handle_heartbeat(self, item):
		self._token_check()

	def _subscribe_heartbeat_update(self):
		product = self._convert_to_ig_product(tl.product.GBPUSD)

		items = ['Chart:{}:TICK'.format(product)]
		fields = ['UTM']
		sub = ('DISTINCT', items, fields, self._handle_heartbeat)
		self._subscribe(*sub)
		self._subscriptions.append(sub)

	def _subscribe_chart_updates(self, product, listener):
		print('subscribe')
		product = self._convert_to_main_ig_product(product)
		period = self._convert_to_ig_live_period(tl.period.ONE_MINUTE)
		items = ['Chart:{}:{}'.format(product, period)]
		fields = [
			'CONS_END', 'UTM',
			'BID_OPEN', 'BID_HIGH', 'BID_LOW', 'BID_CLOSE',
			'OFR_OPEN', 'OFR_HIGH', 'OFR_LOW', 'OFR_CLOSE'
		]
		sub = ('MERGE', items, fields, listener)
		self._subscribe(*sub)
		self._subscriptions.append(sub)


	def onChartUpdate(self, chart, *args, **kwargs):
		item = args[0]
		if item.get('values'):
			values = item.get('values')

			# Process ask values
			ask = [
				values['OFR_OPEN'],
				values['OFR_HIGH'],
				values['OFR_LOW'],
				values['OFR_CLOSE']
			]

			# Process bid values
			bid = [
				values['BID_OPEN'],
				values['BID_HIGH'],
				values['BID_LOW'],
				values['BID_CLOSE']
			]

			# Cancel update if no useful information
			if not all(ask) and not all(bid): return
			try:
				ask = list(map(float, ask))
				bid = list(map(float, bid))
			except ValueError:
				return

			# Get timestamp
			new_ts = int(values['UTM']) // 1000
			# Get candle end
			candle_end = values['CONS_END']

			result = []

			for period in chart.getActivePeriods():
				if period == tl.period.TICK:
					chart.ask[tl.period.TICK] = ask[3]
					chart.bid[tl.period.TICK] = bid[3]
					result.append({
						'broker': self.name,
						'product': chart.product,
						'period': tl.period.TICK,
						'timestamp': new_ts,
						'item': {
							'ask': chart.ask[tl.period.TICK],
							'bid': chart.bid[tl.period.TICK]
						}
					})

				elif (isinstance(chart.bid.get(period), np.ndarray) and 
					isinstance(chart.ask.get(period), np.ndarray)):
					
					# Find if new bar
					is_new_bar = (
						candle_end and int(candle_end) == 1 and
						chart.isNewBar(
							period, new_ts + tl.period.getPeriodOffsetSeconds(period)
						)
					)

					# Ask
					ask = np.array(ask, dtype=np.float64)
					if chart.barReset[period]:
						# Apply all new values on new bar
						chart.ask[period] = ask
					else:
						chart.ask[period][1] = ask[1] if ask[1] > chart.ask[period][1] else chart.ask[period][1]
						chart.ask[period][2] = ask[2] if ask[2] < chart.ask[period][2] else chart.ask[period][2]
						chart.ask[period][3] = ask[3]

					# Bid
					bid = np.array(bid, dtype=np.float64)
					if chart.barReset[period]:
						# Apply all new values on new bar
						chart.bid[period] = bid
					else:
						chart.bid[period][1] = bid[1] if bid[1] > chart.bid[period][1] else chart.bid[period][1]
						chart.bid[period][2] = bid[2] if bid[2] < chart.bid[period][2] else chart.bid[period][2]
						chart.bid[period][3] = bid[3]

					if chart.barReset[period]:
						chart.barReset[period] = False

					if is_new_bar:
						chart.lastTs[period] = new_ts
						chart.barReset[period] = True

					# Add period result
					result.append({
						'broker': self.name,
						'product': chart.product,
						'period': period,
						'bar_end': is_new_bar,
						'timestamp': chart.lastTs[period],
						'item': {
							'ask': chart.ask[period].tolist(),
							'bid': chart.bid[period].tolist()
						}
					})

			# Call chart tick handler
			if len(result):
				chart.handleTick(result)
			
	def _check_sl_tp_hit(self, pos):
		if pos.direction == tl.LONG:
			# Check SL
			if pos.sl and pos.close_price <= pos.sl:
				return tl.STOP_LOSS

			# Check TP
			if pos.tp and pos.close_price >= pos.tp:
				return tl.TAKE_PROFIT
		else:
			# Check SL
			if pos.sl and pos.close_price >= pos.sl:
				return tl.STOP_LOSS
 
			# Check TP
			if pos.tp and pos.close_price <= pos.tp:
				return tl.TAKE_PROFIT

		return tl.POSITION_CLOSE

	'''
	Conversion Utilities
	'''

	def _convert_to_ig_product(self, product):
		if product == tl.product.GBPUSD:
			return 'CS.D.GBPUSD.MINI.IP'
		elif product == tl.product.AUDUSD:
			return 'CS.D.AUDUSD.MINI.IP'

	def _convert_to_main_ig_product(self, product):
		if product == tl.product.GBPUSD:
			return 'CS.D.GBPUSD.CFD.IP'
		elif product == tl.product.AUDUSD:
			return 'CS.D.AUDUSD.CFD.IP'

	def _convert_to_standard_product(self, product):
		if product == 'CS.D.GBPUSD.MINI.IP':
			return tl.product.GBPUSD
		elif product == 'CS.D.AUDUSD.MINI.IP':
			return tl.product.AUDUSD

	def _convert_to_ig_live_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return '1MINUTE'

	def _convert_to_ig_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return 'MINUTE'
		elif period == tl.period.TWO_MINUTES:
			return 'MINUTE_2'
		elif period == tl.period.THREE_MINUTES:
			return 'MINUTE_3'
		elif period == tl.period.FIVE_MINUTES:
			return 'MINUTE_5'
		elif period == tl.period.TEN_MINUTES:
			return 'MINUTE_10'
		elif period == tl.period.FIFTEEN_MINUTES:
			return 'MINUTE_15'
		elif period == tl.period.THIRTY_MINUTES:
			return 'MINUTE_30'
		elif period == tl.period.ONE_HOUR:
			return 'HOUR'
		elif period == tl.period.TWO_HOURS:
			return 'HOUR_2'
		elif period == tl.period.THREE_HOURS:
			return 'HOUR_3'
		elif period == tl.period.FOUR_HOURS:
			return 'HOUR_4'
		elif period == tl.period.DAILY:
			return 'DAY'
		elif period == tl.period.WEEKLY:
			return 'WEEK'
		elif period == tl.period.MONTHLY:
			return 'MONTH'


	def _convert_to_ig_direction(self, direction):
		if direction == tl.LONG:
			return 'BUY'
		else:
			return 'SELL'

	def _convert_to_standard_direction(self, direction):
		if direction == 'BUY':
			return tl.LONG
		else:
			return tl.SHORT

	def _convert_to_ig_order_type(self, order_type):
		if order_type == tl.STOP_ORDER:
			return 'STOP'
		elif order_type == tl.LIMIT_ORDER:
			return 'LIMIT'
		elif order_type == tl.MARKET_ORDER:
			return 'MARKET'

	def _convert_to_standard_order_type(self, order_type):
		if order_type == 'STOP':
			return tl.STOP_ORDER
		elif order_type == 'LIMIT':
			return tl.LIMIT_ORDER
		elif order_type == 'MARKET':
			return tl.MARKET_ORDER


