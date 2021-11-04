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


ONE_HOUR = 60*60

class FXCM(Broker):

	def __init__(self,
		ctrl, username, password, is_demo,
		user_account=None, strategy_id=None, broker_id=None, accounts={}, 
		display_name=None, is_dummy=False, is_parent=False
	):
		print('FXCM INIT')

		super().__init__(ctrl, user_account, strategy_id, broker_id, tl.broker.FXCM_NAME, accounts, display_name, is_dummy, True)

		self.data_saver = tl.DataSaver(broker=self)

		self.is_demo = is_demo
		self.username = username
		self.password = password
		self.is_parent = is_parent
		self._last_update = time.time()
		self._subscriptions = {}
		self.session = None
		self._initialized = False
		self.is_running = True
		
		self._price_queue = []
		self.time_off = 0
		self._set_time_off()

		if not self.ctrl.app.config['IS_MAIN_STREAM']:
			self.brokerRequest = self.ctrl.mainBrokerRequest
		else:
			self.brokerRequest = self.ctrl.brokerRequest

		if is_parent:

			# Create Connection to FXCM Container
			self._add_user()

			# Load Charts
			CHARTS = ['EUR_USD']
			PERIODS = [tl.period.ONE_MINUTE]
			for instrument in CHARTS:
				chart = self.createChart(instrument, await_completion=True)
				self.data_saver.subscribe(chart, PERIODS)

		self._initialized = True

		if not is_dummy:
			# Handle strategy
			if self.userAccount and self.brokerId:
				self._handle_live_strategy_setup()

		t = Thread(target=self._handle_chart_update)
		t.start()

		if not is_dummy and self.ctrl.app.config['IS_MAIN_STREAM']:
			Thread(target=self._periodic_check).start()
		

	def _periodic_check(self):
		WAIT_PERIOD = 60
		# Send ping to server to check connection status
		while self.is_running:
			try:
				self._last_update = time.time()

				res = self.brokerRequest(
					'fxcm', self.brokerId, 'heartbeat'
				)

				print(f"[FXCM] {res}")
				if "result" in res and not res["result"]:
					self.reauthorize_accounts()

			except Exception as e:
				print(traceback.format_exc())

			time.sleep(WAIT_PERIOD)


	def _is_logged_in(self):
		if not self.session is None:
			if (
				self.session.session_status != fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED and
				tl.isWeekend(datetime.utcnow())
			):
				return True

			while self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTING:
				time.sleep(0.01)

			# print(F'[FXCM] Is logged in: {self.session.session_status}')
			return self.session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED
		return False


	def _add_user(self):
		print('Add User')

		res = self.brokerRequest(
			self.name, self.brokerId, 'add_user',
			self.username, self.password, self.is_demo,
			is_parent=self.is_parent
		)

		if 'error' in res:
			return self._add_user()
		else:
			return res

		
	def reauthorize_accounts(self):
		print("[FXCM] Reauthorizing Accounts...")
		self.is_auth = self._add_user()
		# self._subscribe_account_updates()
		# if self.is_auth:
		# 	if self.userAccount and self.brokerId:
		# 		self._handle_live_strategy_setup()

		if self.is_parent:
			CHARTS = ['EUR_USD']
			for instrument in CHARTS:
				print(f'LOADING {instrument}')
				chart = self.getChart(instrument)
				chart.start(True)


	def _set_time_off(self):
		try:
			client = ntplib.NTPClient()
			response = client.request('pool.ntp.org')
			self.time_off = response.tx_time - time.time()
		except Exception:
			pass


	'''
	Broker functions
	'''

	def _download_historical_data(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		include_current=True,
		**kwargs
	):
		print(f'FXCM DOWNLOAD HIST: {product}, {period}, {start}, {end}, {count}')

		if count is not None:
			result = self.data_saver.get(product, period, count=count)
		else:
			start = start.replace(tzinfo=None)
			end = end.replace(tzinfo=None)
			result = self.data_saver.get(product, period, start=start, end=end)

		if include_current:
			chart = self.getChart(product)

			if period in chart.lastTs:
				timestamp = chart.lastTs[period]

				if tl.convertTimeToTimestamp(end) >= timestamp:
					current_bars = np.concatenate((chart.ask[period], chart.mid[period], chart.bid[period]))

					result = result.append(pd.DataFrame(
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
		if tl.isWeekend(datetime.utcnow()) or self.ctrl.app.config['ENV'] == "development" or self.ctrl.app.config['NO_DATA_DOWNLOAD']:
			return self._download_historical_data(
				product, period, tz=tz, 
				start=start, end=end, count=count,
				include_current=False
			)

		if isinstance(start, datetime):
			start = tl.convertTimeToTimestamp(start)
		if isinstance(end, datetime):
			end = tl.convertTimeToTimestamp(end)

		# Count
		res = self.brokerRequest(
			self.name, self.brokerId, '_download_historical_data_broker',
			product, period, tz=tz, start=start, end=end,
			count=count, **kwargs
		)

		result = pd.DataFrame.from_dict(res, dtype=float)
		result.index = result.index.astype(int)

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
		stream_id = self.generateReference()
		res = self.brokerRequest(self.name, self.brokerId, '_subscribe_chart_updates', stream_id, instrument)
		stream_id = res
		print(f"[FXCM._subscribe_chart_updates] {stream_id}")
		self.ctrl.addBrokerListener(stream_id, listener)


	def _handle_chart_update(self):
		time_off_timer = time.time()

		while True:
			result = []
			if len(self._price_queue):
				chart, update_time, bid, ask, volume = self._price_queue[0]
				del self._price_queue[0]

				if update_time is not None:
					# Convert time to datetime
					c_ts = update_time

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
			first_data_ts = tl.convertTimeToTimestamp(datetime.utcfromtimestamp(data.index.values[0]).replace(
				hour=0, minute=0, second=0, microsecond=0
			))
			first_ts = data.index.values[0] - ((data.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))
			data = data.loc[data.index >= first_ts]

			bar_ends = data.index.map(lambda x: (x-first_ts)%tl.period.getPeriodOffsetSeconds(period)==0)
			indicies = np.arange(data.shape[0])[bar_ends.values.astype(bool)]
			result = np.zeros((indicies.shape[0], 12), dtype=float)

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

			return pd.DataFrame(
				index=data[bar_ends].index, data=result, 
				columns=[ 
					'ask_open', 'ask_high', 'ask_low', 'ask_close',
					'mid_open', 'mid_high', 'mid_low', 'mid_close',
					'bid_open', 'bid_high', 'bid_low', 'bid_close'
				]
			)


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
