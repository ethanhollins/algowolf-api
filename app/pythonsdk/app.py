import importlib
import sys
import requests
import json
import time
import traceback
import os
from datetime import datetime, timedelta
from threading import Thread

STRATEGY_PACKAGE = 'app.strategies'

class App(dict):

	def __init__(self, api, package, strategy_id, broker_id):
		if package.endswith('.py'):
			package = package.strip('.py')
		self.api = api
		self.package = package
		self.strategyId = strategy_id
		self.brokerId = broker_id

		# Containers
		self.strategies = {}
		self.indicators = []
		self.charts = []


	# Enable use of dot operators
	def __getattr__(self, key):
		return self[key]

	def __setattr__(self, key, value):
		self[key] = value


	# TODO: Add accounts parameter
	def run(self, accounts, input_variables):
		# Start strategy for each account
		starting = []
		for account_id in accounts:
			self.startStrategy(account_id, input_variables)
			if self.strategies[account_id].get('strategy').getBroker().state.value <= 2:
				# Run strategy
				t = Thread(target=self.strategies[account_id].get('strategy').run)
				t.start()
				starting.append((account_id, t))

		# Join threads
		for t in starting:
			t[1].join()

		for t in starting:
			module = self.strategies[t[0]].get('module')
			# Call strategy onStart
			if 'onStart' in dir(module) and callable(module.onStart):
				module.onStart()


	def stop(self, accounts):
		for account_id in accounts:
			if account_id in self.strategies:
				strategy = self.strategies[account_id].get('strategy')
				strategy.stop()
				del self.strategies[account_id]


	def backtest(self, _from, to, mode, input_variables):
		account_id = 'ACCOUNT_1'

		e = None
		try:
			if isinstance(_from, str):
				_from = datetime.strptime(_from, '%Y-%m-%dT%H:%M:%SZ')
			if isinstance(to, str):
				to = datetime.strptime(to, '%Y-%m-%dT%H:%M:%SZ')
			self.startStrategy(account_id, input_variables)
			self.strategies[account_id].get('strategy').getBroker().setName(self.api.name)

			backtest_id = self.strategies[account_id].get('strategy').backtest(_from, to, mode)
		except Exception as err:
			print(traceback.format_exc())
			e = err
		finally:
			if account_id in self.strategies:
				del self.strategies[account_id]

			if e is not None:
				raise TradelibException(str(e))

		return backtest_id


	def compile(self):
		account_id = 'ACCOUNT_1'
		properties = {}
		e = None
		try:
			self.startStrategy(account_id, {})

		except Exception as err:
			print(traceback.format_exc())
			e = err
		finally:
			if account_id in self.strategies:
				strategy = self.strategies[account_id].get('strategy')
				properties['input_variables'] = strategy.input_variables
				del self.strategies[account_id]

			if e is not None:
				raise TradelibException(str(e))

			return properties


	def getPackageModule(self, package):
		spec = importlib.util.find_spec(package)
		module = importlib.util.module_from_spec(spec)
		# sys.modules[spec.name] = module
		spec.loader.exec_module(module)

		if '__version__' in dir(module):
			return self.getPackageModule(package + '.' + module.__version__)

		return module

	def startStrategy(self, account_id, input_variables):
		if account_id not in self.strategies:
			module = self.getPackageModule(f'{STRATEGY_PACKAGE}.{self.package}')

			strategy = Strategy(self.api, module, strategy_id=self.strategyId, broker_id=self.brokerId, account_id=account_id, user_variables=input_variables)
			strategy.setApp(self)
			
			self.strategies[account_id] = {
				'strategy': strategy,
				'module': module
			}

			# Set global variables
			module.print = strategy.log

			module.strategy = strategy
			module.utils = tl.utils
			module.product = tl.product
			module.period = tl.period
			module.indicator = tl.indicator
			for i in dir(tl.constants):
				vars(module)[i] = vars(tl.constants)[i]

			# Initialize strategy
			if 'init' in dir(module) and callable(module.init):
				module.init()

			# Search for convertional function names
			if 'onTrade' in dir(module) and callable(module.onTrade):
				strategy.getBroker().subscribeOnTrade(module.onTrade)

			if 'onTick' in dir(module) and callable(module.onTick):
				for chart in strategy.getBroker().getAllCharts():
					for period in chart.periods:
						chart.subscribe(period, module.onTick)


	def getStrategy(self, account_id):
		return self.strategies.get(account_id)


	def addChart(self, chart):
		self.charts.append(chart)


	def getChart(self, broker, product):
		for i in self.charts:
			if i.isChart(broker, product):
				return i
		return None


'''
Imports
'''


from .strategy import Strategy
from .error import BrokerlibException, TradelibException
from app import pythonsdk as tl
