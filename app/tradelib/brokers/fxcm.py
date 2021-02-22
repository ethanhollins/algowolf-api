import time
import traceback
import numpy as np
import pandas as pd
import dateutil.parser
import ntplib
import os
import sys
from datetime import datetime, timedelta
from copy import copy
from threading import Thread
from app import tradelib as tl
from app.tradelib.broker import Broker
from app.v1 import AccessLevel, key_or_login_required
from app.error import OrderException, BrokerException
from forexconnect import ForexConnect, fxcorepy, Common
from threading import Thread


class OffersTableListener(object):
	def __init__(self, instruments=[], listeners=[]):
		self.__instruments = instruments
		self.__listeners = listeners

	def addInstrument(self, instrument, listener):
		if instrument not in self.__instruments:
			self.__instruments.append(instrument)
			self.__listeners.append(listener)
			print(self.__instruments)
			print(self.__listeners)

	def on_added(self, table_listener, row_id, row):
		pass

	def on_changed(self, table_listener, row_id, row):
		if row.table_type == ForexConnect.OFFERS:
			self.print_offer(row, self.__instruments, self.__listeners)

	def on_deleted(self, table_listener, row_id, row):
		pass

	def on_status_changed(self, table_listener, status):
		pass

	def print_offer(self, offer_row, selected_instruments, listeners):
		offer_id = offer_row.offer_id
		instrument = offer_row.instrument
		time = offer_row.time
		bid = round(offer_row.bid, 5)
		ask = round(offer_row.ask, 5)
		volume = offer_row.volume

		try:
			idx = selected_instruments.index(instrument)
			listener = listeners[idx]
			listener(time, bid, ask, volume)

		except ValueError:
			pass


ONE_HOUR = 60*60

class FXCM(Broker):

	def __init__(self,
		ctrl, username, password, is_demo,
		user_account=None, strategy_id=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False, is_parent=False
	):
		super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.FXCM_NAME, accounts, display_name)

		self.data_saver = tl.DataSaver(broker=self)

		self.is_demo = is_demo
		self.username = username
		self.password = password
		self._spotware_connected = False
		self._last_update = time.time()
		self._subscriptions = {}
		self.session = None
		self._initialized = False
		self.job_queue = []
		self._price_queue = []

		self.fx = ForexConnect()
		self._login()
		self.offers_listener = None

		if is_parent:
			while self.session is None or self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTING:
				time.sleep(0.01)
			if self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED:
				self._get_offers_listener()

			# Load Charts
			CHARTS = ['EUR_USD']
			PERIODS = [tl.period.ONE_MINUTE]
			for instrument in CHARTS:
				chart = self.createChart(instrument, await_completion=True)
				self.data_saver.subscribe(chart, PERIODS)

		self._initialized = True

		if not is_dummy:
			# for account_id in self.getAccounts():
			# 	if account_id != tl.broker.PAPERTRADER_NAME:
			# 		self._subscribe_account_updates(account_id)

			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

		self.time_off = 0
		self._set_time_off()

		t = Thread(target=self._handle_chart_update)
		t.start()


	def _is_logged_in(self):
		if not self.session is None:
			if (
				self.session.session_status != fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED and
				tl.isWeekend(datetime.utcnow())
			):
				return True

			while self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTING:
				time.sleep(0.01)

			print(F'[FXCM] Is logged in: {self.session.session_status}')
			return self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED
		return False


	def _login(self):
		if not self._is_logged_in():
			try:
				print('[FXCM] Attempting login...')
				self.fx.login(
					user_id=self.username, password=self.password, 
					connection='demo' if self.is_demo else 'real',
					session_status_callback=self._on_status_change
				)
				return True

			except Exception:
				# print(traceback.format_exc(), flush=True)
				print('[FXCM] Login failed.')
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


	def _handle_job(self, func, *args, **kwargs):
		ref_id = self.generateReference()
		self.job_queue.append(ref_id)
		while self.job_queue.index(ref_id) > 0: pass
		result = func(*args, **kwargs)
		del self.job_queue[0]
		return result


	def _on_status_change(self, session, status):
		self.session = session

		print(f"Trading session status: {status}")
		if status in (
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.DISCONNECTED,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.SESSION_LOST,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.RECONNECTING,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.PRICE_SESSION_RECONNECTING,
			fxcorepy.AO2GSessionStatus.O2GSessionStatus.CHART_SESSION_RECONNECTING
		):
			print('[FXCM] Disconnected.')
			try:
				self.session.logout()
			except Exception:
				pass
			finally:
				self.session = None

			time.sleep(1)
			if not tl.isWeekend(datetime.utcnow()):
				self._login()

			sys.exit()

		elif status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED:
			print('[FXCM] Logged in.')
			# if self._initialized and self.offers_listener is None:
			# 	self._get_offers_listener()
			# 	self.data_saver.fill_all_missing_data()


	def _get_offers_listener(self):
		offers = self.fx.get_table(ForexConnect.OFFERS)
		self.offers_listener = OffersTableListener()

		table_listener = Common.subscribe_table_updates(
			offers,
			on_change_callback=self.offers_listener.on_changed,
			on_add_callback=self.offers_listener.on_added,
			on_delete_callback=self.offers_listener.on_deleted,
			on_status_change_callback=self.offers_listener.on_changed
		)

	'''
	Broker functions
	'''

	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		include_current=True,
		**kwargs
	):
		if count is not None:
			result = self.data_saver.get(product, period, count=count)
		else:
			start = start.replace(tzinfo=None)
			end = end.replace(tzinfo=None)
			result = self.data_saver.get(product, period, start=start, end=end)

		if include_current:
			chart = self.getChart(product)
			timestamp = chart.lastTs[period]
			current_bars = np.concatenate((chart.ask[period], chart.mid[period], chart.bid[period]))

			result.append(pd.DataFrame(
				index=pd.Index(data=[timestamp], name='timestamp'),
				columns=[
					'ask_open', 'ask_high', 'ask_low', 'ask_close',
					'mid_open', 'mid_high', 'mid_low', 'mid_close',
					'bid_open', 'bid_high', 'bid_low', 'bid_close'
				],
				data=[current_bars]
			))

		return result


	def _download_historical_data_broker(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		**kwargs
	):
		if tl.isWeekend(datetime.utcnow()) or not self._login():
			return self._download_historical_data(
				product, period, tz=tz, 
				start=start, end=end, count=count,
				include_current=False
			)

		# Count
		if not count is None:
			res = self._handle_job(
				self.fx.get_history,
				self._convert_product(product), 
				self._convert_period(period), 
				quotes_count=count
			)

		# Start -> End
		else:
			start = start.replace(tzinfo=None)
			end = end.replace(tzinfo=None)
			res = self._handle_job(
				self.fx.get_history,
				self._convert_product(product), 
				self._convert_period(period), 
				start, end
			)

		# Convert to result DF
		res = np.array(list(map(lambda x: list(x), res)))

		ask_prices = res[:, 5:9].astype(float)
		bid_prices = res[:, 1:5].astype(float)
		mid_prices = (ask_prices + bid_prices)/2
		timestamps = res[:, 0]
		prices = np.around(np.concatenate((ask_prices, mid_prices, bid_prices), axis=1), decimals=5)

		result = pd.DataFrame(
			index=pd.Index(timestamps).map(
				lambda x: int((x - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))
			).rename('timestamp'),
			columns=[
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'mid_open', 'mid_high', 'mid_low', 'mid_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			],
			data=prices
		)

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


	def _subscribe_chart_updates(self, instrument, listener):
		if not tl.isWeekend(datetime.utcnow()) and self._login():
			self.offers_listener.addInstrument(self._convert_product(instrument), listener)


	def _handle_chart_update(self):
		time_off_timer = time.time()

		while True:
			result = []
			if len(self._price_queue):
				chart, update_time, bid, ask, volume = self._price_queue[0]
				del self._price_queue[0]

				if update_time is not None:
					# Convert time to datetime
					c_ts = tl.convertTimeToTimestamp(update_time)
					# Iterate periods
					for period in chart.getActivePeriods():
						if (isinstance(chart.bid.get(period), np.ndarray) and 
							isinstance(chart.ask.get(period), np.ndarray)):

							# Handle period bar end
							if period != tl.period.TICK:
								is_new_bar = chart.isNewBar(period, c_ts)
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

									chart.lastTs[period] = tl.getNextTimestamp(period, chart.lastTs[period], now=c_ts - tl.period.getPeriodOffsetSeconds(period))
									print(f'[FXCM] ({period}) Prev: {chart.lastTs[period]}, Next: {chart.lastTs[period]}')
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

				# print(result)

			else:
				for chart in self.charts:
					c_ts = time.time()+self.time_off
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
								print(f'[FXCM] ({period}) Prev: {chart.lastTs[period]}, Next: {chart.lastTs[period]}')
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
			first_data_ts = datetime.utcfromtimestamp(data.index.values[0]).replace(
				hour=0, minute=0, second=0, microsecond=0
			).timestamp()
			first_ts = data.index.values[0] - ((data.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))
			data = data.loc[data.index >= first_ts]

			bar_ends = data.index.map(lambda x: (x-first_ts)%tl.period.getPeriodOffsetSeconds(period)==0)
			indicies = np.arange(data.shape[0])[bar_ends.values.astype(bool)]
			result = np.zeros((indicies.shape[0], 12), dtype=float)
			print(indicies.shape)
			print(indicies[:10])

			for i in range(1, indicies.shape[0]):
				idx = indicies[i]
				passed_count = indicies[i] - indicies[i-1]

				if idx - passed_count == 0:
					result[i] = [
						data.values[idx-passed_count, 0], np.amax(data.values[idx-passed_count:idx, 1]), 
						np.amin(data.values[idx-passed_count:idx, 2]), data.values[idx-1, 3],
						data.values[idx-passed_count, 4], np.amax(data.values[idx-passed_count:idx, 5]), 
						np.amin(data.values[idx-passed_count:idx, 6]), data.values[idx-1, 7],
						data.values[idx-passed_count, 8], np.amax(data.values[idx-passed_count:idx, 9]), 
						np.amin(data.values[idx-passed_count:idx, 10]), data.values[idx-1, 11]
					]
				else:
					result[i] = [
						data.values[idx-passed_count-1, 3], np.amax(data.values[idx-passed_count:idx, 1]), 
						np.amin(data.values[idx-passed_count:idx, 2]), data.values[idx-1, 3],
						data.values[idx-passed_count-1, 7], np.amax(data.values[idx-passed_count:idx, 5]), 
						np.amin(data.values[idx-passed_count:idx, 6]), data.values[idx-1, 7],
						data.values[idx-passed_count-1, 11], np.amax(data.values[idx-passed_count:idx, 9]), 
						np.amin(data.values[idx-passed_count:idx, 10]), data.values[idx-1, 11]
					]

				# print(data.values[idx-passed_count, 0])
				# print(data.values[idx-passed_count:idx, 1])
				# print(data.values[idx-passed_count:idx, 2])
				# print(data.values[idx, 3])
				# print(result[i])
				# print('---------')

			return pd.DataFrame(
				index=data[bar_ends].index, data=result, 
				columns=[ 
					'ask_open', 'ask_high', 'ask_low', 'ask_close',
					'mid_open', 'mid_high', 'mid_low', 'mid_close',
					'bid_open', 'bid_high', 'bid_low', 'bid_close'
				]
			)


	# def _construct_bars(self, period, data, smooth=True):

	# 	if not self._convert_period(period) is None:
	# 		for i in range(1, data.shape[0]):
	# 			data.values[i, 0] = data.values[i-1, 3]
	# 			data.values[i, 4] = data.values[i-1, 7]
	# 			data.values[i, 8] = data.values[i-1, 11]

	# 		return data

	# 	else:
	# 		result = self._create_empty_df()

	# 		first_data_ts = datetime.utcfromtimestamp(data.index.values[0]).replace(
	# 			hour=0, minute=0, second=0, microsecond=0
	# 		).timestamp()
	# 		first_ts = data.index.values[0] - ((data.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))

	# 		if first_ts != data.index.values[0]:
	# 			first_ts = tl.utils.getNextTimestamp(period, first_ts, now=data.index.values[0])

	# 		data = data.loc[data.index >= first_ts]
	# 		c_ts = first_ts
	# 		next_ts = tl.utils.getNextTimestamp(period, c_ts, now=c_ts)
	# 		ohlc = data.values[0]
	# 		for i in range(1, data.shape[0]):
	# 			c_ohlc = data.values[i]
	# 			ts = data.index.values[i]

	# 			if ts >= next_ts:
	# 				result.loc[c_ts] = ohlc
	# 				new_ohlc = c_ohlc

	# 				if smooth:
	# 					new_ohlc[0] = ohlc[3]
	# 					new_ohlc[4] = ohlc[7]
	# 					new_ohlc[8] = ohlc[11]

	# 				ohlc = new_ohlc
	# 				c_ts = next_ts
	# 				next_ts = tl.utils.getNextTimestamp(period, next_ts, now=ts)

	# 			else:
	# 				if c_ohlc[1] > ohlc[1]:
	# 					ohlc[1] = c_ohlc[1]
	# 				if c_ohlc[5] > ohlc[5]:
	# 					ohlc[5] = c_ohlc[5]
	# 				if c_ohlc[9] > ohlc[9]:
	# 					ohlc[9] = c_ohlc[9]
	# 				if c_ohlc[2] < ohlc[2]:
	# 					ohlc[2] = c_ohlc[2]
	# 				if c_ohlc[6] < ohlc[6]:
	# 					ohlc[6] = c_ohlc[6]
	# 				if c_ohlc[10] < ohlc[10]:
	# 					ohlc[10] = c_ohlc[10]

	# 				ohlc[3] = c_ohlc[3]
	# 				ohlc[7] = c_ohlc[7]
	# 				ohlc[11] = c_ohlc[11]


	# 		return result


	def _convert_product(self, product):
		return product.replace('_', '/')


	def _convert_period(self, period):
		if period == tl.period.ONE_MINUTE:
			return 'm1'
		elif period == tl.period.TWO_MINUTES:
			return 'm2'
		elif period == tl.period.THREE_MINUTES:
			return 'm3'
		elif period == tl.period.FIVE_MINUTES:
			return 'm5'
		elif period == tl.period.TEN_MINUTES:
			return 'm10'
		elif period == tl.period.FIFTEEN_MINUTES:
			return 'm15'
		elif period == tl.period.THIRTY_MINUTES:
			return 'm30'
		elif period == tl.period.ONE_HOUR:
			return 'H1'
		elif period == tl.period.TWO_HOURS:
			return 'H2'
		elif period == tl.period.THREE_HOURS:
			return 'H3'
		elif period == tl.period.FOUR_HOURS:
			return 'H4'
		elif period == tl.period.DAILY:
			return 'D1'
		elif period == tl.period.WEEKLY:
			return 'W1'
		elif period == tl.period.MONTHLY:
			return 'M1'

# tl.period.ONE_MINUTE,
# 			tl.period.TWO_MINUTES, tl.period.THREE_MINUTES,
# 			tl.period.FIVE_MINUTES, tl.period.TEN_MINUTES,
# 			tl.period.FIFTEEN_MINUTES, tl.period.THIRTY_MINUTES,
# 			tl.period.ONE_HOUR, tl.period.TWO_HOURS, 
# 			tl.period.THREE_HOURS, tl.period.FOUR_HOURS, 
# 			tl.period.DAILY, tl.period.WEEKLY, 
# 			tl.period.MONTHLY