from app import tradelib as tl

SPOTWARE_PAIRS = [
	'EUR_USD', 'AUD_USD', 'USD_CAD', 'USD_CHF', 'GBP_USD',
	'USD_JPY', 'USD_MXN', 'USD_NOK', 'NZD_USD', 'USD_SEK',
	'USD_RUB', 'USD_CNH', 'USD_TRY', 'USD_ZAR', 'USD_PLN',
	'USD_HUF', 'USD_CZK', 'USD_SGD'
]

TEMP_PRICES = {
	"USD": 1.0,
	"EUR": 1.1953491294,
	"AUD": 0.7759772229,
	"CAD": 0.8017214459,
	"CHF": 1.0752328045,
	"GBP": 1.3921464559,
	"JPY": 0.0091755518,
	"MXN": 0.0482467518,
	"NOK": 0.1185582234,
	"NZD": 0.717701726,
	"SEK": 0.1179590884,
	"RUB": 0.0136365659,
	"CNY": 0.153947181,
	"TRY": 0.1323155877,
	"ZAR": 0.0668959103,
	"PLN": 0.2608400647,
	"HUF": 0.0032562302,
	"CZK": 0.0456554405,
	"SGD": 0.7437709851,
	"HKD": 0.1288240033,
	"DKK": 0.1607334027
}


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


	def __getRate(self):
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


	# def getRate(self):
	# 	result = self.ctrl.xecd.convert_from(self.currency, 'USD', 1.0)
	# 	if result.get('to') and len(result.get('to')):
	# 		return result['to'][0]['mid']


	def getRate(self):
		return TEMP_PRICES[self.currency]
