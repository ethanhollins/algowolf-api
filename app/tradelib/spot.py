from app import tradelib as tl

SPOTWARE_PAIRS = [
	'EUR_USD', 'AUD_USD', 'USD_CAD', 'USD_CHF', 'GBP_USD',
	'USD_JPY', 'USD_MXN', 'USD_NOK', 'NZD_USD', 'USD_SEK',
	'USD_RUB', 'USD_CNH', 'USD_TRY', 'USD_ZAR', 'USD_PLN',
	'USD_HUF', 'USD_CZK', 'USD_SGD'
]

TEMP_PRICES = {
	"USD": 1.0,
	"EUR": 1.1868151,
	"AUD": 0.73430805,
	"CAD": 0.80153995,
	"CHF": 1.103722,
	"GBP": 1.3899807,
	"JPY": 0.0091186104,
	"MXN": 0.050318779,
	"NOK": 0.1133117,
	"NZD": 0.69687174,
	"SEK": 0.11632341,
	"RUB": 0.013676551,
	"CNY": 0.15476891,
	"TRY": 0.11829344,
	"ZAR": 0.068458834,
	"PLN": 0.25984771,
	"HUF": 0.0033092352,
	"CZK": 0.046484227,
	"SGD": 0.73841701,
	"HKD": 0.12863552,
	"DKK": 0.15955351
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
