from app import tradelib as tl

SPOTWARE_PAIRS = [
	'EUR_USD', 'AUD_USD', 'USD_CAD', 'USD_CHF', 'GBP_USD',
	'USD_JPY', 'USD_MXN', 'USD_NOK', 'NZD_USD', 'USD_SEK',
	'USD_RUB', 'USD_CNH', 'USD_TRY', 'USD_ZAR', 'USD_PLN',
	'USD_HUF', 'USD_CZK', 'USD_SGD'
]

TEMP_PRICES = {
	"USD": 1.0,
	"EUR": 1.1796212,
	"AUD": 0.73686166,
	"CAD": 0.79902541,
	"CHF": 1.0923735,
	"GBP": 1.3867757,
	"JPY": 0.0091233601,
	"MXN": 0.050307654,
	"NOK": 0.11380854,
	"NZD": 0.70395148,
	"SEK": 0.11573751,
	"RUB": 0.013641736,
	"CNY": 0.15438507,
	"TRY": 0.1173543,
	"ZAR": 0.067898063,
	"PLN": 0.25824463,
	"HUF": 0.0033480322,
	"CZK": 0.046443222,
	"SGD": 0.73807896,
	"HKD": 0.12848572,
	"DKK": 0.15860493
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
