import json
import os
import datetime
import pandas as pd

from app import tradelib as tl
from app.error import DataLoaderException

class DataLoader(object):

	def __init__(self, broker=None, data_path=''):
		if broker:
			self.broker = broker
		else:
			self.broker = self._set_default_broker()

		self._data_path = data_path

	def _set_default_broker(self):
		options = self._get_options()
		key = options['default']['key']
		is_demo = options['default']['is_demo']

		return tl.broker.Oanda(key, is_demo=is_demo)

	def _get_options(self):
		path = os.path.join(os.path.split(__file__)[0], 'options.json')
		if os.path.exists(path):
			with open(path, 'r') as f:
				return json.load(f)
				
		else:
			raise DataLoaderException('Options file does not exist.')

	def get(self, product, period, start=None, end=None, download=False):
		if not start:
			start = tl.utils.TS_START_DATE
		if not end:
			end = datetime.datetime.utcnow()

		data_path = os.path.join(self._data_path, 'data/', '{}/{}/{}'.format(self.broker.name, product, period))
		if os.path.exists(data_path) and len(os.listdir(data_path)) > 0:
			data = self._perform_load(product, period, start, end)

			if download:
				data = pd.concat((
					self.download(
						product, period,
						start,
						tl.utils.convertTimestampToTime(data.index.values[0])
					),
					data
				))

				data = pd.concat((
					data,
					self.download(
						product, period,
						tl.utils.convertTimestampToTime(data.index.values[-1]),
						end
					)
				))

				data = data[~data.index.duplicated(keep='first')]
				start_dt = tl.utils.convertTimestampToTime(data.index[0])
				end_dt = tl.utils.convertTimestampToTime(data.index[-1])
				self.save(data, product, period, start_dt, end_dt)

			return data
		else:
			return self.download(product, period, start, end, save=True)

	def load(self, product, period, start=None, end=None, count=None):
		if not start and not end:
			if count:
				start = self._get_date_by_count(period, count)
				end = datetime.datetime.utcnow()
			else:
				raise DataLoaderException('Cannot load data without specified time or count.')

		data_dir = os.path.join(self._data_path, 'data/', '{}/{}/{}/'.format(self.broker.name, product, period))
		frags = []
		for y in range(start.year, end.year+1):
			data_path = os.path.join(data_dir, '{}-{}.csv'.format(y, y+1))
			if os.path.exists(data_path):
				t_data = pd.read_csv(data_path, sep=' ')
				if y == start.year:
					ts_start = tl.utils.convertTimeToTimestamp(start)
					t_data = t_data.loc[t_data['timestamp'] >= ts_start]
				if y == end.year:
					ts_end = tl.utils.convertTimeToTimestamp(end)
					t_data = t_data.loc[t_data['timestamp'] <= ts_end]
				frags.append(t_data)

		if len(frags) > 0:
			ask_keys = ['ask_open', 'ask_high', 'ask_low', 'ask_close']
			bid_keys = ['bid_open', 'bid_high', 'bid_low', 'bid_close']
			
			data = pd.concat(frags).set_index('timestamp')
			return data[ask_keys + bid_keys]
		else:
			return pd.DataFrame()

	def download(self, product, period, start=None, end=None, count=None, save=False):
		data = self.broker._download_historical_data(product, period, start=start, end=end, count=count)
		if type(data) == pd.DataFrame:
			ask_keys = ['ask_open', 'ask_high', 'ask_low', 'ask_close']
			bid_keys = ['bid_open', 'bid_high', 'bid_low', 'bid_close']

			data = data[~data.index.duplicated(keep='first')]
			data = data[ask_keys + bid_keys]
			if save:
				self.save(data, product, period, start, end)
		return data

	def save(self, data, product, period, start, end):
		data_dir = os.path.join(self._data_path, 'data/', '{}/{}/{}/'.format(self.broker.name, product, period))
		if not os.path.exists(data_dir):
			os.makedirs(data_dir)

		for y in range(start.year, end.year+1):
			dt_start = datetime.datetime(year=y, month=1, day=1)
			dt_end = datetime.datetime(year=y+1, month=1, day=1)
			ts_start = tl.utils.convertTimeToTimestamp(dt_start)
			ts_end = tl.utils.convertTimeToTimestamp(dt_end)

			# Load saved data
			old_data = self.load(product, period, start=dt_start, end=dt_end)

			t_data = data.loc[(ts_start <= data.index) & (data.index < ts_end)]
			if t_data.size == 0:
				continue
			t_data = pd.concat((
				old_data,
				t_data,
			)).sort_index()
			t_data = t_data[~t_data.index.duplicated(keep='first')]
			t_data = t_data.round(pd.Series([5]*8, index=t_data.columns))

			data_path = os.path.join(data_dir, '{}-{}.csv'.format(y, y+1))
			t_data.to_csv(data_path, sep=' ', header=True)


	def _get_date_by_count(self, period, count):
		offset = tl.period.getPeriodOffsetSeconds(period)+1
		return tl.utils.convertTimezone(datetime.datetime.utcnow(), 'UTC') - datetime.timedelta(seconds=count*offset)

