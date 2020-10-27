import math
import time
import json
import requests


class Position(dict):

	def __init__(self, broker, order_id, account_id, product, order_type, direction, lotsize, entry_price=None, sl=None, tp=None, open_time=None):
		
		# Hidden Variable
		super().__setattr__('_broker', broker)

		# Dictionary Variables
		self.order_id = order_id
		self.account_id = account_id
		self.product = product
		self.order_type = order_type
		self.direction = direction
		self.lotsize = lotsize
		self.entry_price = entry_price
		self.close_price = None
		self.sl = sl
		self.tp = tp

		if open_time:
			self.open_time = int(open_time)
		else:
			self.open_time = math.floor(time.time())

		self.close_time = None

	@classmethod
	def fromDict(cls, broker, pos):
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

	@classmethod
	def fromOrder(cls, broker, order):
		if order.order_type == tl.LIMIT_ORDER:
			order_type = tl.LIMIT_ENTRY
		elif order.order_type == tl.STOP_ORDER:
			order_type = tl.STOP_ENTRY

		res = cls(
			broker,
			order['order_id'],
			order['account_id'],
			order['product'],
			order_type,
			order['direction'],
			order['lotsize']
		)
		# Dictionary Variables
		for k, v in pos.items():
			if k != 'order_type':
				res.__setattr__(k, v)

		return res

	def update(self, pos):
		for k, v in pos.items():
			self.__setattr__(k, v)

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
			raise BrokerException('`_broker` is a protected variable.')

	def __str__(self):
		return json.dumps(self, indent=2)


	# Broker Functions
	def close(self, lotsize=None):
		if not lotsize: lotsize = self.lotsize

		pos = self._broker.api.getPositionByID(self.order_id)
		if pos is not None:
			res = pos.close(lotsize, override=True)
		
			result = self
			for ref_id, item in res.items():
				if item.get('accepted'):
					func = self._broker._get_trade_handler(item.get('type'))
					result = self._broker._wait(ref_id, func, (ref_id, item))

			return result
		else:
			print('[CLOSE] POS IS NONE')
			return self


	def modify(self, 
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		pos = self._broker.api.getPositionByID(self.order_id)
		if pos is not None:
			res = pos.modify(
				sl_range, tp_range, sl_price, tp_price, override=True
			)

			for ref_id, item in res.items():
				if item.get('accepted'):
					func = self._broker._get_trade_handler(item.get('type'))
					wait_result = self._broker._wait(ref_id, func, (ref_id, item))

			return self
		else:
			print('[MODIFY] POS IS NONE')
			return self
	

	def modifySL(self, sl_range=None, sl_price=None):
		return self.modify(sl_range=sl_range, sl_price=sl_price)


	def modifyTP(self, tp_range=None, tp_price=None):
		return self.modify(tp_range=tp_range, tp_price=tp_price)


	def getProfit(self):

		if self.direction == tl.LONG:
			if self.close_price:
				return round(tl.utils.convertToPips(self.close_price - self.entry_price), 2)
			else:
				bid = self._broker.getBid(self.product)
				return round(tl.utils.convertToPips(bid - self.entry_price), 2)
		else:
			if self.close_price:
				return round(tl.utils.convertToPips(self.entry_price - self.close_price), 2)
			else:
				ask = self._broker.getAsk(self.product)
				return round(tl.utils.convertToPips(self.entry_price - ask), 2)


	def isBacktest(self):
		return False


class BacktestPosition(Position):

	def close(self, lotsize=None):
		if not lotsize: lotsize = self.lotsize
		
		return self._broker.backtester.deletePosition(
			self, lotsize
		)

	def modify(self, 
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None
	):
		return self._broker.backtester.modifyPosition(
			self, sl_range, tp_range, sl_price, tp_price
		)

	def isBacktest(self):
		return True


'''
Imports
'''
from app import pythonsdk as tl
from app.pythonsdk.error import BrokerException





