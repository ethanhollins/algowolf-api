import math
import time
import json
import requests

class Order(dict):

	def __init__(self, broker, order_id, account_id, product, order_type, direction, lotsize, entry_price=None, sl=None, tp=None, open_time=None):
		
		# Hidden Variable
		super().__setattr__('_broker', broker)

		# Dictionary Variables
		self.order_id = order_id
		self.account_id = account_id
		self.product = product
		self.direction = direction
		self.order_type = order_type
		self.entry_price = entry_price
		self.close_price = None
		self.sl = sl
		self.tp = tp

		if open_time:
			self.open_time = int(open_time)
		else:
			self.open_time = math.floor(time.time())


	@classmethod
	def fromDict(cls, broker, pos):
		# Hidden Variable
		res = cls(
			broker,
			pos['order_id'],
			pos['account_id'],
			pos['product'],
			pos['order_type'],
			pos['direction'],
			pos['lotsize']
		)
		# Dictionary Variables
		for k, v in pos.items():
			res.__setattr__(k, v)

		return res


	def __getattr__(self, key):
		if key != '_broker':
			try:
				return self[key]
			except Exception:
				pass
		
		super().__getattr__(key)


	def __setattr__(self, key, value):
		if key != '_broker':
			self[key] = value
		else:
			raise Exception('`_broker` is a protected variable.')


	def __str__(self):
		cpy = self.copy()
		cpy['open_time'] = int(cpy['open_time'])
		return json.dumps(cpy, indent=2)


	def cancel(self):
		order = self._broker.api.getOrderByID(self.order_id)
		if order is not None:
			result = self
			res = order.cancel(override=True)

			for ref_id, item in res.items():
				if item.get('accepted'):
					func = self._broker._get_trade_handler(item.get('type'))
					result = self._broker._wait(ref_id, func, (ref_id, item))
				else:
					return self

			return result

		else:
			return self


	def close(self):
		return self.cancel()


	def modify(self, 
		lotsize=None,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		order = self._broker.api.getOrderByID(self.order_id)
		if order is not None:
			res = order.modify(
				lotsize, entry_range, entry_price, 
				sl_range, tp_range, sl_price, tp_price, 
				override=True
			)

			for ref_id, item in res.items():
				if item.get('accepted'):
					func = self._broker._get_trade_handler(item.get('type'))
					wait_result = self._broker._wait(ref_id, func, (ref_id, item))
				else:
					return self

			return self

		else:
			return self


	def modifyEntry(self, entry_range=None, entry_price=None):
		return self.modify(entry_range=entry_range, entry_price=entry_price)


	def modifySL(self, sl_range=None, sl_price=None):
		return self.modify(sl_range=sl_range, sl_price=sl_price)


	def modifyTP(self, tp_range=None, tp_price=None):
		return self.modify(tp_range=tp_range, tp_price=tp_price)


	def isBacktest(self):
		return False




class BacktestOrder(Order):


	def cancel(self):
		return self._broker.backtester.deleteOrder(self)


	def modify(self,
		lotsize=None,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		return self._broker.backtester.modifyOrder(
			self, lotsize, entry_range, entry_price,
			sl_range, tp_range, sl_price, tp_price
		)


	def isBacktest(self):
		return True



'''
Imports
'''
from app import pythonsdk as tl
from app.pythonsdk.error import BrokerException



