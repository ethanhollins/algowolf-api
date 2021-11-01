from app import tradelib as tl

SPOTWARE_PAIRS = [
	'EUR_USD', 'AUD_USD', 'USD_CAD', 'USD_CHF', 'GBP_USD',
	'USD_JPY', 'USD_MXN', 'USD_NOK', 'NZD_USD', 'USD_SEK',
	'USD_RUB', 'USD_CNH', 'USD_TRY', 'USD_ZAR', 'USD_PLN',
	'USD_HUF', 'USD_CZK', 'USD_SGD'
]

TEMP_PRICES = {
	"USD": 1.0,
	"EUR": 1.1575346,
	"AUD": 0.73096466,
	"CAD": 0.80183135,
	"CHF": 1.0780798,
	"GBP": 1.3613515,
	"JPY": 0.0089116171,
	"MXN": 0.048279731,
	"NOK": 0.11755631,
	"NZD": 0.69300367,
	"SEK": 0.11422377,
	"RUB": 0.013924852,
	"CNY": 0.15518221,
	"TRY": 0.11137389,
	"ZAR": 0.066905445,
	"PLN": 0.25148179,
	"HUF": 0.0032131509,
	"CZK": 0.045473845,
	"SGD": 0.73775048,
	"HKD": 0.12846204,
	"DKK": 0.15552586
}


class Spot(object):

	def __init__(self, ctrl, currency, rate=None):
		self.ctrl = ctrl
		self.currency = currency

		if rate is not None:
			self.ctrl.redis_client.hset("rates", currency, rate)
		print(f'{self.currency} rate: {self.getRate()}')


	def _get_pair(self):
		for pair in SPOTWARE_PAIRS:
			if self.currency in pair:
				return pair


	def convertTo(self, price):
		return price * (1 / self.getRate())


	def convertFrom(self, price):
		return price * self.getRate()


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
		return float(self.ctrl.redis_client.hget("rates", self.currency).decode())


	def getRateBackup(self):
		return TEMP_PRICES[self.currency]
