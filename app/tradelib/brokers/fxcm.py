import time
import traceback
import numpy as np
import pandas as pd
import fxcmpy
from datetime import datetime
from copy import copy
from threading import Thread
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException

class FXCM(Broker):

	def __init__(self,
		ctrl, key, is_demo,
		user_account=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False
	):
		super().__init__(ctrl, user_account, broker_id, tl.broker.FXCM_NAME, accounts, display_name)

		self._key = key
		self.is_demo = is_demo
		self._spotware_connected = False
		self._last_update = time.time()
		self._subscriptions = {}

		self.con = fxcmpy.fxcmpy(
			access_token=self._key,
			server='demo' if self.is_demo else 'real'
		)

		if not is_dummy:
			# for account_id in self.getAccounts():
			# 	if account_id != tl.broker.PAPERTRADER_NAME:
			# 		self._subscribe_account_updates(account_id)

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()


	'''
	Broker functions
	'''

	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		start = start.replace(tzinfo=None)
		end = end.replace(tzinfo=None)
		
		# Count
		if not count is None:
			self.con.get_candles(
				self._convert_product(product), 
				self._convert_period(period), 
				number=count
			)

		# Start -> End
		else:
			if self._convert_period(period) is None:
				res = self.con.get_candles(
					self._convert_product(product), 
					period=self._convert_period(tl.period.ONE_MINUTE), 
					start=start, stop=end
				)
			else:
				res = self.con.get_candles(
					self._convert_product(product), 
					period=self._convert_period(period), 
					start=start, stop=end
				)

		res_asks = res[['askopen', 'askhigh', 'asklow', 'askclose']]
		res_bids = res[['bidopen', 'bidhigh', 'bidlow', 'bidclose']]

		# Convert to result DF
		result = self._create_empty_df()
		mid_values = np.around((res_asks.values[:] + res_bids.values[:])/2, decimals=5)

		result = pd.DataFrame(
			index=res.index.map(lambda x: x.timestamp()).rename('timestamp'),
			columns=[
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'mid_open', 'mid_high', 'mid_low', 'mid_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			],
			data=np.concatenate((
				res_asks.values[:], mid_values[:], res_bids.values[:]
			), axis=1)
		)

		result = self._construct_bars(period, result)
		return result


	def _get_all_positions(self, account_id):
		return


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

		return


	def modifyPosition(self, pos, sl_price, tp_price, override=False):
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyPosition(
				pos, sl_price, tp_price, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		return


	def deletePosition(self, pos, lotsize, override=False):
		if pos.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deletePosition(
				pos, lotsize, override=override
			)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		return


	def _get_all_orders(self, account_id):
		return


	def getAllAccounts(self):
		return


	def getAccountInfo(self, account_id, override=False):
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.LIMITED)

		return


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

		return


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price, override=False):
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().modifyOrder(
				order, lotsize, entry_price, sl_price, tp_price, override=override
			)

		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		return


	def deleteOrder(self, order, override=False):
		if order.account_id == tl.broker.PAPERTRADER_NAME:
			return super().deleteOrder(order, override=override)
		# Check auth
		if not override:
			key_or_login_required(self.brokerId, AccessLevel.DEVELOPER)

		return


	def _on_account_update(self, account_id, update, ref_id):
		return


	def _subscribe_chart_updates(self, product, listener):
		return


	def onChartUpdate(self, chart, payload):
		return


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


	def _create_empty_df(self):
		return pd.DataFrame(columns=[
			'timestamp', 
			'ask_open', 'ask_high', 'ask_low', 'ask_close',
			'mid_open', 'mid_high', 'mid_low', 'mid_close',
			'bid_open', 'bid_high', 'bid_low', 'bid_close'
		]).set_index('timestamp')


	def _construct_bars(self, period, data, smooth=True):

		if not self._convert_period(period) is None:
			for i in range(1, data.shape[0]):
				data.values[i, 0] = data.values[i-1, 3]
				data.values[i, 4] = data.values[i-1, 7]
				data.values[i, 8] = data.values[i-1, 11]

			return data

		else:
			result = self._create_empty_df()

			first_data_ts = datetime.utcfromtimestamp(data.index.values[0]).replace(
				hour=0, minute=0, second=0, microsecond=0
			).timestamp()
			first_ts = data.index.values[0] - ((data.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))
			if first_ts != data.index.values[0]:
				first_ts = tl.utils.getNextTimestamp(period, first_ts, now=data.index.values[0])

			data = data.loc[data.index >= first_ts]
			c_ts = first_ts
			next_ts = tl.utils.getNextTimestamp(period, c_ts, now=c_ts)
			ohlc = data.values[0]
			for i in range(1, data.shape[0]):
				c_ohlc = data.values[i]
				ts = data.index.values[i]

				if ts >= next_ts:
					result.loc[c_ts] = ohlc
					new_ohlc = c_ohlc

					if smooth:
						new_ohlc[0] = ohlc[3]
						new_ohlc[4] = ohlc[7]
						new_ohlc[8] = ohlc[11]

					ohlc = new_ohlc
					c_ts = next_ts
					next_ts = tl.utils.getNextTimestamp(period, next_ts, now=ts)

				else:
					if c_ohlc[1] > ohlc[1]:
						ohlc[1] = c_ohlc[1]
					if c_ohlc[5] > ohlc[5]:
						ohlc[5] = c_ohlc[5]
					if c_ohlc[9] > ohlc[9]:
						ohlc[9] = c_ohlc[9]
					if c_ohlc[2] < ohlc[2]:
						ohlc[2] = c_ohlc[2]
					if c_ohlc[6] < ohlc[6]:
						ohlc[6] = c_ohlc[6]
					if c_ohlc[10] < ohlc[10]:
						ohlc[10] = c_ohlc[10]

					ohlc[3] = c_ohlc[3]
					ohlc[7] = c_ohlc[7]
					ohlc[11] = c_ohlc[11]


			return result


	def _convert_product(self, product):
		return product.replace('_', '/')


	def _convert_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return 'm1'
		elif period == tl.period.FIVE_MINUTES:
			return 'm5'

