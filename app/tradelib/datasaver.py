import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from app import tradelib as tl

class DataSaver(object):

	def __init__(self, broker):
		self.broker = broker
		self.data = {}


	def _create_empty_df(self, period):
		if period == tl.period.TICK:
			return pd.DataFrame(columns=['timestamp', 'ask', 'bid']).set_index('timestamp')
		else:
			return pd.DataFrame(columns=[
				'timestamp', 
				'ask_open', 'ask_high', 'ask_low', 'ask_close',
				'bid_open', 'bid_high', 'bid_low', 'bid_close'
			]).set_index('timestamp')


	def subscribe(chart):
		if not chart.product in self.data:
			self.data[chart.product] = {}

		for period in [tl.period.TICK, tl.period.ONE_MINUTE]:
			self.data[chart.product][period] = self._create_empty_df(period)
			sub_id = self.broker.generateReference()
			chart.subscribe(period, self.broker.brokerId, sub_id, self._handle_price_data)

		self._fill_missing_data(chart.product, period)


	def get(self, period, start, end):
		''' Retrieve from saved data and memory '''
		return


	def _handle_price_data(self, item):
		''' Handle live data feed '''

		if item['period'] == tl.period.TICK:
			self.data[item['product']][item['period']].loc[item['timestamp']] = [item['item']['ask'], item['item']['bid']]
		else:
			if item['bar_end']:
				self.data[item['product']][item['period']].loc[item['timestamp']] = np.concatenate(
					(item['item']['ask'], item['item']['bid']), 
					axis=1
				)


	def _construct_bars(self, period, data, smooth=True):
		''' Construct other period bars from appropriate saved data '''
		first_data_ts = datetime.utcfromtimestamp(data.index.values[0]).replace(
			hour=0, minute=0, second=0, microsecond=0
		).timestamp()
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


	def _fill_missing_data(self, product, period):
		''' Fill any data that was missed '''

		# Retrieve saved data

		# Run historical data search from last saved timestamp to now
		return


	def _save_data(self, product, period, data):
		''' Save data to storage '''

		start_ts = data.index.values[0]
		start_dt = tl.convertTimestampToTime(start_ts)
		end_ts = data.index.values[-1]
		end_dt = tl.convertTimestampToTime(end_ts)

		if period in (tl.period.TICK, tl.period.ONE_MINUTE):
			c_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
			while c_dt < end_dt + timedelta(days=1):
				# Load any existing file at this time

				# Append data to existing file

				c_dt += timedelta(days=1)


		elif period in (tl.period.ONE_HOUR, tl.period.DAILY):
			pass


