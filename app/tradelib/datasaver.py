import os
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from app import tradelib as tl
from app import ROOT_DIR
from threading import Thread

SAVE_DELAY = 60 * 60

class DataSaver(object):

	def __init__(self, broker):
		self.broker = broker
		self.data = {}
		self._price_queue = []
		self._save_periods = [tl.period.ONE_MINUTE]

		self.timer = time.time()
		# t = Thread(target=self._handle_price_data)
		# t.start()


	def _init_data_csv(self, chart, period):
		for period in self._save_periods:
			if not os.path.exists(os.path.join(ROOT_DIR, f'data/{self.broker.name}/{chart.product}/{period}')):
				os.makedirs(os.path.join(ROOT_DIR, f'data/{self.broker.name}/{chart.product}/{period}'))


	def _create_empty_df(self, period):
		if period == tl.period.TICK:
			return pd.DataFrame(columns=['timestamp', 'ask', 'bid'], dtype=float).set_index('timestamp')
		else:
			return pd.DataFrame(columns=[
				'timestamp', 
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'mid_open', 'mid_high', 'mid_low', 'mid_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			], dtype=float).set_index('timestamp')


	def subscribe(self, chart, periods):
		# Subscribe to live updates
		if not chart.product in self.data:
			self.data[chart.product] = {}

		for period in periods:
			self._init_data_csv(chart, period)
			self.data[chart.product][period] = self._create_empty_df(period)
			sub_id = self.broker.generateReference()
			chart.subscribe(period, self.broker.brokerId, sub_id, self._handle_price_data)

			# Fill missing data
			self._fill_missing_data(chart.product, period)


	def _get_dates(self, product, period, start, end):
		if period != tl.period.TICK:
			load_period = tl.period.ONE_MINUTE
		else:
			load_period = tl.period.TICK

		frags = []
		c_dt = start.replace(hour=0, minute=0, second=0, microsecond=0)
		while c_dt < end + timedelta(days=1):
			path = os.path.join(ROOT_DIR, f'data/{self.broker.name}/{product}/{load_period}/{c_dt.strftime("%Y%m%d")}.csv.gz')

			if os.path.exists(path):
				if period == tl.period.TICK:
					old_data = pd.read_csv(
						path, sep=',', 
						names=['timestamp', 'ask', 'bid'], 
						index_col='timestamp', compression='gzip',
						dtype=float
					).round(decimals=5)
				else:
					old_data = pd.read_csv(
						path, sep=',', 
						names=[
							'timestamp', 
							'ask_open', 'ask_high', 'ask_low', 'ask_close',
							'mid_open', 'mid_high', 'mid_low', 'mid_close',
							'bid_open', 'bid_high', 'bid_low', 'bid_close'
						], 
						index_col='timestamp', compression='gzip',
						dtype=float
					).round(decimals=5)

				frags.append(old_data.loc[
					(old_data.index >= tl.convertTimeToTimestamp(start)) & 
					(old_data.index < tl.convertTimeToTimestamp(end))
				])

			c_dt += timedelta(days=1)

		# Add any current relevant memory data
		if product in self.data and load_period in self.data[product]:
			frags.append(self.data[product][load_period].loc[
				(self.data[product][load_period].index >= tl.convertTimeToTimestamp(start)) & 
				(self.data[product][load_period].index < tl.convertTimeToTimestamp(end))
			])

		if len(frags):
			result = pd.concat(frags)
			# Create Mid Prices
			# result = pd.concat((
			# 	result, pd.DataFrame(
			# 		index=result.index, 
			# 		columns=['mid_open', 'mid_high', 'mid_low', 'mid_close'],
			# 		data=np.around((result.values[:, :4] + result.values[:, 4:])/2, decimals=5),
			# 		dtype=float
			# 	)
			# ), axis=1)[[
			# 	'ask_open', 'ask_high', 'ask_low', 'ask_close',
			# 	'mid_open', 'mid_high', 'mid_low', 'mid_close',
			# 	'bid_open', 'bid_high', 'bid_low', 'bid_close'
			# ]]

			if load_period == tl.period.ONE_MINUTE:
				result = self._remove_weekend_data(result)
				result = self._construct_bars(period, result)
				result = result.loc[~(result==0).all(axis=1)]

		else:
			result = pd.DataFrame(
				columns=[
					'timestamp',
					'ask_open', 'ask_high', 'ask_low', 'ask_close',
					'mid_open', 'mid_high', 'mid_low', 'mid_close',
					'bid_open', 'bid_high', 'bid_low', 'bid_close'
				]
			).set_index('timestamp')

		return result


	def _get_count(self, product, period, count):
		if period != tl.period.TICK:
			load_period = tl.period.ONE_MINUTE
		else:
			load_period = tl.period.TICK

		result = pd.DataFrame(
			columns=[
				'timestamp',
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'mid_open', 'mid_high', 'mid_low', 'mid_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			]
		).set_index('timestamp')

		c_dt = datetime.utcnow()
		temp_data = pd.DataFrame(
			columns=[
				'timestamp',
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'mid_open', 'mid_high', 'mid_low', 'mid_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			]
		).set_index('timestamp')

		print(f'COUNT: {product}, {period}, {count}')
		while result.shape[0] < count:
			path = os.path.join(ROOT_DIR, f'data/{self.broker.name}/{product}/{load_period}/{c_dt.strftime("%Y%m%d")}.csv.gz')

			if os.path.exists(path):
				if period == tl.period.TICK:
					old_data = pd.read_csv(
						path, sep=',', 
						names=['timestamp', 'ask', 'bid'], 
						index_col='timestamp', compression='gzip',
						dtype=float
					).round(decimals=5)
				else:
					temp_data = pd.concat((
						pd.read_csv(
							path, sep=',', 
							names=[
								'timestamp', 
								'ask_open', 'ask_high', 'ask_low', 'ask_close',
								'mid_open', 'mid_high', 'mid_low', 'mid_close',
								'bid_open', 'bid_high', 'bid_low', 'bid_close'
							], 
							index_col='timestamp', compression='gzip',
							dtype=float
						).round(decimals=5),
						temp_data
					))

				# construction_data = pd.concat((
				# 	temp_data, pd.DataFrame(
				# 		index=temp_data.index, 
				# 		columns=['mid_open', 'mid_high', 'mid_low', 'mid_close'],
				# 		data=np.around((temp_data.values[:, :4] + temp_data.values[:, 4:])/2, decimals=5),
				# 		dtype=float
				# 	)
				# ), axis=1)[[
				# 	'ask_open', 'ask_high', 'ask_low', 'ask_close',
				# 	'mid_open', 'mid_high', 'mid_low', 'mid_close',
				# 	'bid_open', 'bid_high', 'bid_low', 'bid_close'
				# ]]

				complete_data = self._construct_bars(period, temp_data)
				result = pd.concat((complete_data, result))
				if complete_data.size > 0:
					temp_data = temp_data.loc[temp_data.index < complete_data.index.values[0]]

			c_dt -= timedelta(days=1)

		return result.iloc[-count:]



	def get(self, product, period, start=None, end=None, count=None):
		''' Retrieve from saved data and memory '''

		if start is not None and end is not None:
			return self._get_dates(product, period, start, end)
		elif count is not None:
			return self._get_count(product, period, count)


	def queue_price_data(self, item):
		self._price_queue.append(item)


	def _handle_price_data(self, item):
		''' Handle live data feed '''

		# while True:
		# 	if len(self._price_queue):
		# 		# Pop item
		# 		item = self._price_queue[0]
		# 		del self._price_queue[0]

		# 		print(f'handle {item}')
		data = self.data[item['product']][item['period']]
		# if item['period'] == tl.period.TICK:
		# 	self.data[item['product']][item['period']] = data.append(pd.DataFrame(
		# 		index=pd.Index(data=[item['timestamp']], name='timestamp'),
		# 		columns=['ask', 'bid'],
		# 		data=[[item['item']['ask'], item['item']['bid']]]
		# 	))
		# 	# data.loc[item['timestamp']] = [item['item']['ask'], item['item']['bid']]

		# 	if time.time() - self.timer >= SAVE_DELAY:
		# 		self.timer = time.time()
		# 		print(f'Saving {data.shape[0]} ticks.')
		# 		# Reset DF
		# 		self.data[item['product']][item['period']] = self._create_empty_df(item['period'])
		# 		# Save Data
		# 		self._save_data(item['product'], item['period'], data.copy())

		# else:
		if item['period'] == tl.period.ONE_MINUTE:
			if item['bar_end']:
				self.data[item['product']][item['period']] = data.append(pd.DataFrame(
					index=pd.Index(data=[item['timestamp']], name='timestamp'),
					columns=[
						'ask_open', 'ask_high', 'ask_low', 'ask_close',
						'mid_open', 'mid_high', 'mid_low', 'mid_close',
						'bid_open', 'bid_high', 'bid_low', 'bid_close'
					],
					data=[np.concatenate(
						(item['item']['ask'], item['item']['mid'], item['item']['bid'])
					)]
				))

			# elif self.data[item['product']][item['period']].shape[0] == 0:
			# 	timestamp = self.broker.getChart(item['product']).lastTs[item['period']]
			# 	print(f'set {timestamp}')
			# 	self.data[item['product']][item['period']] = data.append(pd.DataFrame(
			# 		index=pd.Index(data=[timestamp], name='timestamp'),
			# 		columns=[
			# 			'ask_open', 'ask_high', 'ask_low', 'ask_close',
			# 			'bid_open', 'bid_high', 'bid_low', 'bid_close'
			# 		],
			# 		data=[np.concatenate(
			# 			(item['item']['ask'], item['item']['bid'])
			# 		)]
			# 	))

			# else:
			# 	self.data[item['product']][item['period']].iloc[-1] = np.concatenate(
			# 		(item['item']['ask'], item['item']['bid'])
			# 	)

			if time.time() - self.timer >= SAVE_DELAY:
				self.timer = time.time()
				# Reset DF
				self.data[item['product']][item['period']] = self._create_empty_df(item['period'])
				# Save Data
				self._save_data(item['product'], item['period'], data.copy())

		# 	if item['bar_end']:
		# 		data.loc[item['timestamp']] = np.concatenate(
		# 			(item['item']['ask'], item['item']['bid'])
		# 		)


	# def _construct_bars(self, period, data, smooth=True):
	# 	''' Construct other period bars from appropriate saved data '''
	# 	if data.size > 0:
	# 		first_data_ts = datetime.utcfromtimestamp(data.index.values[0]).replace(
	# 			hour=0, minute=0, second=0, microsecond=0
	# 		).timestamp()
	# 		first_ts = data.index.values[0] - ((data.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))
	# 		data = data.loc[data.index >= first_ts]

	# 		index = data.index.copy().append(pd.Index(
	# 			[data.index.values[-1] + tl.period.getPeriodOffsetSeconds(tl.period.ONE_MINUTE)]
	# 		))
	# 		bar_ends = index.map(lambda x: (x-first_ts)%tl.period.getPeriodOffsetSeconds(period)==0)
	# 		indicies = np.arange(bar_ends.shape[0])[bar_ends.values.astype(bool)]
	# 		result = np.zeros((max(0, indicies.shape[0]-1), 12), dtype=float)

	# 		for i in range(1, indicies.shape[0]):
	# 			idx = indicies[i]
	# 			passed_count = indicies[i] - indicies[i-1]

	# 			if idx - passed_count == 0:
	# 				result[i-1] = [
	# 					data.values[idx-passed_count, 0], np.amax(data.values[idx-passed_count:idx, 1]), 
	# 					np.amin(data.values[idx-passed_count:idx, 2]), data.values[idx-1, 3],
	# 					data.values[idx-passed_count, 4], np.amax(data.values[idx-passed_count:idx, 5]), 
	# 					np.amin(data.values[idx-passed_count:idx, 6]), data.values[idx-1, 7],
	# 					data.values[idx-passed_count, 8], np.amax(data.values[idx-passed_count:idx, 9]), 
	# 					np.amin(data.values[idx-passed_count:idx, 10]), data.values[idx-1, 11]
	# 				]
	# 			else:
	# 				result[i-1] = [
	# 					data.values[idx-passed_count-1, 3], np.amax(data.values[idx-passed_count:idx, 1]), 
	# 					np.amin(data.values[idx-passed_count:idx, 2]), data.values[idx-1, 3],
	# 					data.values[idx-passed_count-1, 7], np.amax(data.values[idx-passed_count:idx, 5]), 
	# 					np.amin(data.values[idx-passed_count:idx, 6]), data.values[idx-1, 7],
	# 					data.values[idx-passed_count-1, 11], np.amax(data.values[idx-passed_count:idx, 9]), 
	# 					np.amin(data.values[idx-passed_count:idx, 10]), data.values[idx-1, 11]
	# 				]

	# 		if bar_ends[-1]:
	# 			data = data[bar_ends[:-1]]
	# 			return pd.DataFrame(
	# 				index=data.iloc[data.shape[0] - result.shape[0]:].index,
	# 				data=result, 
	# 				columns=[ 
	# 					'ask_open', 'ask_high', 'ask_low', 'ask_close',
	# 					'mid_open', 'mid_high', 'mid_low', 'mid_close',
	# 					'bid_open', 'bid_high', 'bid_low', 'bid_close'
	# 				]
	# 			)
	# 		else:
	# 			data = data[bar_ends[:-1]]
	# 			return pd.DataFrame(
	# 				index=data.iloc[data.shape[0] - result.shape[0]:].index - tl.period.getPeriodOffsetSeconds(period), 
	# 				data=result, 
	# 				columns=[ 
	# 					'ask_open', 'ask_high', 'ask_low', 'ask_close',
	# 					'mid_open', 'mid_high', 'mid_low', 'mid_close',
	# 					'bid_open', 'bid_high', 'bid_low', 'bid_close'
	# 				]
	# 			)
			
	# 	else:
	# 		return data

	def _remove_weekend_data(self, df):
		if df.size > 0:
			ts = df.index.values[0] - timedelta(days=7).total_seconds()
			while ts < df.index.values[-1]:
				weekend_start = tl.utils.getWeekstartDate(datetime.utcfromtimestamp(ts))
				weekend_end = tl.utils.getWeekendDate(datetime.utcfromtimestamp(ts))

				df = df.loc[(df.index < tl.convertTimeToTimestamp(weekend_end)) | (df.index > tl.convertTimeToTimestamp(weekend_start))]
				ts += timedelta(days=7).total_seconds()

		return df


	def _construct_bars(self, period, data, smooth=True):
		''' Construct other period bars from appropriate saved data '''
		if data.size > 0 and period != tl.period.ONE_MINUTE:
			first_data_ts = tl.convertTimeToTimestamp(datetime.utcfromtimestamp(data.index.values[0]).replace(
				hour=0, minute=0, second=0, microsecond=0
			))
			first_ts = data.index.values[0] - ((data.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))
			next_ts = tl.utils.getNextTimestamp(period, first_ts, now=data.index.values[0])
			data = data.loc[data.index >= first_ts]
			timestamps = np.zeros((data.shape[0],), dtype=float)
			result = np.zeros(data.shape, dtype=float)

			idx = 0
			passed_count = 1
			for i in range(1, data.shape[0]+1):
				# idx = indicies[i]
				# passed_count = indicies[i] - indicies[i-1]
				if i == data.shape[0]:
					ts = tl.utils.getNextTimestamp(period, data.index.values[i-1], now=data.index.values[i-1])
				else:
					ts = data.index.values[i]

				if ts >= next_ts:
					timestamps[idx] = next_ts - tl.period.getPeriodOffsetSeconds(period)
					next_ts = tl.utils.getNextTimestamp(period, next_ts, now=ts)

					if i - passed_count == 0:
						result[idx] = [
							data.values[i-passed_count, 0], np.amax(data.values[i-passed_count:i, 1]), 
							np.amin(data.values[i-passed_count:i, 2]), data.values[i-1, 3],
							data.values[i-passed_count, 4], np.amax(data.values[i-passed_count:i, 5]), 
							np.amin(data.values[i-passed_count:i, 6]), data.values[i-1, 7],
							data.values[i-passed_count, 8], np.amax(data.values[i-passed_count:i, 9]), 
							np.amin(data.values[i-passed_count:i, 10]), data.values[i-1, 11]
						]
					else:
						result[idx] = [
							data.values[i-passed_count-1, 3], np.amax(data.values[i-passed_count:i, 1]), 
							np.amin(data.values[i-passed_count:i, 2]), data.values[i-1, 3],
							data.values[i-passed_count-1, 7], np.amax(data.values[i-passed_count:i, 5]), 
							np.amin(data.values[i-passed_count:i, 6]), data.values[i-1, 7],
							data.values[i-passed_count-1, 11], np.amax(data.values[i-passed_count:i, 9]), 
							np.amin(data.values[i-passed_count:i, 10]), data.values[i-1, 11]
						]

					idx += 1
					passed_count = 1
				else:
					passed_count += 1


			timestamps = timestamps[:idx]
			result = result[:idx]

			return pd.DataFrame(
				index=pd.Index(data=timestamps, name='timestamp'),
				data=result, 
				columns=[ 
					'ask_open', 'ask_high', 'ask_low', 'ask_close',
					'mid_open', 'mid_high', 'mid_low', 'mid_close',
					'bid_open', 'bid_high', 'bid_low', 'bid_close'
				]
			)

		else:
			return data


	def fill_all_missing_data(self):
		for product in self.data:
			for period in self.data[product]:
				self._fill_missing_data(product, period)


	def _fill_missing_data(self, product, period):
		''' Fill any data that was missed '''

		# Retrieve saved data
		path = os.path.join(ROOT_DIR, f'data/{self.broker.name}/{product}/{period}')

		if len(os.listdir(path)) > 0:
			# Get last saved file
			last_file = sorted(
				os.listdir(path), 
				key=lambda x: datetime.strptime(x.replace('.csv.gz', ''), '%Y%m%d'), 
				reverse=True
			)[0]

			# Get last saved timestamp
			if period == tl.period.TICK:
				old_data = pd.read_csv(
					os.path.join(path, last_file), sep=',', 
					names=['timestamp', 'ask', 'bid'], 
					compression='gzip'
				).set_index('timestamp')
				last_ts = old_data.index.values[-1]
			else:
				old_data = pd.read_csv(
					os.path.join(path, last_file), sep=',', 
					names=[
						'timestamp', 
						'ask_open', 'ask_high', 'ask_low', 'ask_close',
						'mid_open', 'mid_high', 'mid_low', 'mid_close',
						'bid_open', 'bid_high', 'bid_low', 'bid_close'
					], 
					compression='gzip'
				).set_index('timestamp')
				last_ts = old_data.index.values[-1]

			# Retrieve new data
			current_ts = self.broker.getChart(product).lastTs[period]

			data = self.broker._download_historical_data_broker(
				product, period,
				start=tl.convertTimestampToTime(last_ts),
				end=datetime.utcnow()
			)
			data = data.loc[(data.index > last_ts) & (data.index < current_ts)]

			if data.shape[0] > 0:
				# Delete duplicate memory data
				if product in self.data and period in self.data[product]:
					mem_data = self.data[product][period]
					self.data[product][period] = mem_data.loc[mem_data.index > data.index.values[-1]]

				# Save new data
				self._save_data(
					product, period, data
				)


	def _save_data(self, product, period, data):
		''' Save data to storage '''
		if data.size > 0 and self.broker.ctrl.connection_id == 0:
			start_ts = data.index.values[0]
			start_dt = tl.convertTimestampToTime(start_ts)
			end_ts = data.index.values[-1]
			end_dt = tl.convertTimestampToTime(end_ts)

			if period in (tl.period.TICK, tl.period.ONE_MINUTE):
				c_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
				while c_dt < end_dt + timedelta(days=1):
					path = os.path.join(ROOT_DIR, f'data/{self.broker.name}/{product}/{period}/{c_dt.strftime("%Y%m%d")}.csv.gz')

					# Append data to existing file
					c_data = data.loc[
						(data.index >= tl.convertTimeToTimestamp(c_dt)) & 
						(data.index < tl.convertTimeToTimestamp(c_dt + timedelta(days=1)))
					]
					if c_data.shape[0] > 0:
						c_data.to_csv(path, sep=',', mode='a', header=False, compression='gzip')

					c_dt += timedelta(days=1)


			elif period in (tl.period.ONE_HOUR, tl.period.DAILY):
				pass


