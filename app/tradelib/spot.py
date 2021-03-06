from app import tradelib as tl

SPOTWARE_PAIRS = [
	'EUR_USD', 'AUD_USD', 'USD_CAD', 'USD_CHF', 'GBP_USD',
	'USD_JPY', 'USD_MXN', 'USD_NOK', 'NZD_USD', 'USD_SEK',
	'USD_RUB', 'USD_CNH', 'USD_TRY', 'USD_ZAR', 'USD_PLN',
	'USD_HUF', 'USD_CZK', 'USD_SGD'
]

class Spot(object):

	def __init__(self, ctrl, currency):
		self.ctrl = ctrl
		self.currency = currency
		self.rate = self.getRate()
		print(f'{self.currency} rate: {self.rate}')


	def _get_pair(self):
		for pair in SPOTWARE_PAIRS:
			if self.currency in pair:
				return pair


	def convertTo(self, price):
		return price * (1 / self.rate)


	def convertFrom(self, price):
		return price * self.rate


	def getRate(self):
		spotware = self.ctrl.brokers.get('spotware')
		if spotware is not None:
			pair = self._get_pair()
			df = spotware._download_historical_data(
				pair, tl.period.ONE_MINUTE, count=5
			)

			if pair[:3] == 'USD':
				rate = 1 / df.values[-1, 3]
			else:
				rate = df.values[-1, 3]

			return rate
