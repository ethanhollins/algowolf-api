import os
from app.pythonsdk.app import App

class Strategy(object):

	def __init__(self, strategy_id, api, package):
		self.strategyId = strategy_id
		self.api = api
		self.package = package
		self.app = App(self.api, package, strategy_id=self.strategyId)


	def run(self, accounts, input_variables={}):
		if input_variables is None:
			input_variables = {}

		# Check if already started
		self.app.run(accounts, input_variables)


	def stop(self, accounts):
		# Check if already stopped
		self.app.stop(accounts)


	def restart(self):
		self.stop()
		self.run()


	def backtest(self, _from, to, mode, input_variables={}):
		if input_variables is None:
			input_variables = {}

		return self.app.backtest(_from, to, mode, input_variables)


	def compile(self):
		return self.app.compile()


	def setPackage(self, package):
		self.app.stop()
		self.app = App(self.api, package, strategy_id=self.strategyId)


	def isRunning(self, account):
		return account in self.app.strategies

