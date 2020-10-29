import numpy as np
import functools


class Indicator(object):

	def __init__(self, name, properties, storage, period=None):
		self.name = name
		self.properties = properties
		self.storage = storage
		self.period = period

		self.idx = 0
		self.asks = None
		self.bids = None
		self._asks = None
		self._bids = None

	def _preprocessing(self, data):
		timestamps = data.index.values
		asks = data.values[:,:4]
		bids = data.values[:,4:]
		
		return timestamps, asks, bids

	def _perform_calculation(self, price_type, ohlc, idx):
		return

	def _set_idx(self, idx):
		self.idx = idx
		self.asks = self._asks[:self.idx+1][::-1]
		self.bids = self._bids[:self.idx+1][::-1]


	def limit(self):
		self._asks = self._asks[-1000:]
		self._bids = self._bids[-1000:]
		self.idx = self._asks.shape[0]-1

		self.asks = self._asks[:self.idx+1][::-1]
		self.bids = self._bids[:self.idx+1][::-1]


	def isIndicator(self, name, props):
		return (
			name.lower() == self.name and
			props == self.properties
		)

	def calculate(self, data, idx):		
		timestamps, asks, bids = self._preprocessing(data)

		# Calculate ask prices
		for i in range(idx, timestamps.shape[0]):
			new_ask = [self._perform_calculation('ask', asks, i)]
			if isinstance(self._asks, type(None)):
				self._asks = np.array(new_ask, dtype=np.float64)
			elif i < self._asks.shape[0]:
				self._asks[i] = new_ask[0]
			else:
				self._asks = np.concatenate((self._asks, new_ask))

		# Calculate bid prices
		for i in range(idx, timestamps.shape[0]):
			new_bid = [self._perform_calculation('bid', bids, i)]

			if isinstance(self._bids, type(None)):
				self._bids = np.array(new_bid, dtype=np.float64)
			elif i < self._bids.shape[0]:
				self._bids[i] = new_bid[0]
			else:
				self._bids = np.concatenate((self._bids, new_bid))

		# self.idx = self._asks.shape[0]-1

		# self.asks = self._asks[:self.idx+1][::-1]
		# self.bids = self._bids[:self.idx+1][::-1]

	def getCurrentAsk(self):
		return self._asks[self.idx]

	def getCurrentBid(self):
		return self._bids[self.idx]

	def getAsk(self, off, amount):
		return self._asks[max(self.idx+1-off-amount,0):self.idx-off]

	def getBid(self, off, amount):
		return self._bids[max(self.idx+1-off-amount,0):self.idx+1-off]

	def setPeriod(self, period):
		self.period = period

'''
Overlays
'''

# Donchian Channel
class DONCH(Indicator):

	def __init__(self, period):
		super().__init__('donch', [period], None)

	def _perform_calculation(self, price_type, ohlc, idx):
		# Properties:
		period = self.properties[0]

		# Get relevant OHLC
		ohlc = ohlc[max((idx)-period, 0):idx]
		# Check min period met
		if ohlc.shape[0] < period:
			return [np.nan]*2

		high_low = [0,0]
		for i in range(ohlc.shape[0]):
			if high_low[0] == 0 or ohlc[i,1] > high_low[0]:
				high_low[0] = ohlc[i][1]
			if high_low[1] == 0 or ohlc[i,2] < high_low[1]:
				high_low[1] = ohlc[i,2]
		return high_low


# Exponential Moving Average
class EMA(Indicator):

	def __init__(self, period):
		super().__init__('ema', [period], [0, 0])

	def _perform_calculation(self, price_type, ohlc, idx):
		# Properties:
		period = self.properties[0]
		# if price_type == 'ask':
		# 	prev_ema = self.storage[0]
		# else:
		# 	prev_ema = self.storage[1]

		# Get relevant OHLC
		ohlc = ohlc[max((idx+1)-period, 0):idx+1]
		# Check min period met
		if ohlc.shape[0] < period:
			return [np.nan]

		# Perform calculation

		if idx > period:
			if price_type == 'ask':
				prev_ema = self._asks[idx-1, 0]
			else:
				prev_ema = self._bids[idx-1, 0]

			multi = 2 / (period + 1)
			ema = (ohlc[-1, 3] - prev_ema) * multi + prev_ema

		else:
			ma = 0
			for i in range(ohlc.shape[0]):
				ma += ohlc[i,3]

			ema = ma / period

		# if price_type == 'ask':
		# 	if self.asks is None or idx > len(self.asks)-1:
		# 		self.storage[0] = ema
		# else:
		# 	if self.bids is None or idx > len(self.bids)-1:
		# 		self.storage[1]	= ema

		return [ema]


# Simple Moving Average
class SMA(Indicator):

	def __init__(self, period):
		super().__init__('sma', [period], None)

	def _perform_calculation(self, price_type, ohlc, idx):
		# Properties:
		period = self.properties[0]

		# Get relevant OHLC
		ohlc = ohlc[max((idx+1)-period, 0):idx+1]
		# Check min period met
		if ohlc.shape[0] < period:
			return [np.nan]

		# Perform calculation
		ma = 0
		for i in range(ohlc.shape[0]):
			ma += ohlc[i,3]
		return [np.around(ma / period, decimals=5)]


'''
Studies
'''

# Commodity Channel Index
class CCI(Indicator):

	def __init__(self, period):
		super().__init__('cci', [period], None)

	def _perform_calculation(self, price_type, ohlc, idx):
		# Properties:
		period = self.properties[0]

		# Get relevant OHLC
		ohlc = ohlc[max((idx+1)-period, 0):idx+1]
		# Check min period met
		if ohlc.shape[0] < period:
			return [np.nan]

		# Perform calculation

		# Calculate Typical price SMA
		c_typ = (ohlc[-1,1] + ohlc[-1,2] + ohlc[-1,3])/3.0
		typ_sma = 0.0
		for i in range(ohlc.shape[0]):
			typ_sma += (ohlc[i,1] + ohlc[i,2] + ohlc[i,3])/3.0

		typ_sma /= period
		
		# Calculate Mean Deviation
		mean_dev = 0.0
		for i in range(ohlc.shape[0]):
			mean_dev += np.absolute(
				((ohlc[i,1] + ohlc[i,2] + ohlc[i,3])/3.0) - typ_sma
			)

		mean_dev /= period
		const = .015

		if mean_dev == 0:
			return 0

		return [np.around((c_typ - typ_sma) / (const * mean_dev), decimals=5)]


# Relative Strength Index
class RSI(Indicator):

	def __init__(self, period):
		super().__init__('rsi', [period], [0, 0, 0, 0])

	def _perform_calculation(self, price_type, ohlc, idx):
		# Properties:
		period = self.properties[0]
		if price_type == 'ask':
			prev_gain = self.storage[0]
			prev_loss = self.storage[1]
		else:
			prev_gain = self.storage[2]
			prev_loss = self.storage[3]

		# Get relevant OHLC
		ohlc = ohlc[max((idx+1)-(period+1), 0):idx+1]
		# Check min period met
		if ohlc.shape[0] < period+1:
			return [np.nan]

		# Perform calculation
		gain_sum = 0.0
		loss_sum = 0.0
			
		if prev_gain and prev_loss:
			chng = ohlc[-1,3] - ohlc[-2,3]
			if chng >= 0:
				gain_sum += chng
			else:
				loss_sum += np.absolute(chng)

			gain_avg = (prev_gain * (period-1) + gain_sum)/period
			loss_avg = (prev_loss * (period-1) + loss_sum)/period

		else:
			for i in range(1, ohlc.shape[0]):
				chng = ohlc[i,3] - ohlc[i-1,3]

				if chng >= 0:
					gain_sum += chng
				else:
					loss_sum += np.absolute(chng)

			gain_avg = gain_sum / period
			loss_avg = loss_sum / period

		if price_type == 'ask':
			if idx > len(self.asks)-1:
				self.storage[0] = gain_avg
				self.storage[1] = loss_avg
		else:
			if idx > len(self.bids)-1:
				self.storage[2] = gain_avg
				self.storage[3] = loss_avg

		if loss_avg == 0.0:
			return [100.0]
		else:
			return [100.0 - (100.0 / (1.0 + gain_avg/loss_avg))]


'''
Imports
'''

from app import pythonsdk as tl


