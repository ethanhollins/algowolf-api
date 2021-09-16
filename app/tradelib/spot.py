from app import tradelib as tl

SPOTWARE_PAIRS = [
	'EUR_USD', 'AUD_USD', 'USD_CAD', 'USD_CHF', 'GBP_USD',
	'USD_JPY', 'USD_MXN', 'USD_NOK', 'NZD_USD', 'USD_SEK',
	'USD_RUB', 'USD_CNH', 'USD_TRY', 'USD_ZAR', 'USD_PLN',
	'USD_HUF', 'USD_CZK', 'USD_SGD'
]

TEMP_PRICES = {
	"USD": 1.0,
	"EUR": 1.1806446,
	"AUD": 0.73200599,
	"CAD": 0.79125179,
	"CHF": 1.0865407,
	"GBP": 1.3833383,
	"JPY": 0.0091526674,
	"MXN": 0.050288819,
	"NOK": 0.11644146,
	"NZD": 0.71084791,
	"SEK": 0.11632741,
	"RUB": 0.013814622,
	"CNY": 0.15540928,
	"TRY": 0.11837523,
	"ZAR": 0.069313315,
	"PLN": 0.25865791,
	"HUF": 0.0033833856,
	"CZK": 0.046640028,
	"SGD": 0.74535177,
	"HKD": 0.12847686,
	"DKK": 0.1587698
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
