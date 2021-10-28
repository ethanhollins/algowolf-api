from app import tradelib as tl

class OrderManager(dict):

	def __init__(self, broker):
		self._broker = broker


	def close(self, order, override=False):
		return self.cancel(order, override=override)


	def cancel(self, order, override=False):
		# Call to broker
		return self._broker.deleteOrder(order, override=override)


	def modify(self, 
		order, lotsize=None,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None,
		override=False
	):
		if not lotsize:
			lotsize = order["lotsize"]

		# Convert to price
		if entry_range:
			if order["direction"] == tl.LONG:
				entry = round(order["entry_price"] + tl.utils.convertToPrice(entry_range), 5)
			else:
				entry = round(order["entry_price"] - tl.utils.convertToPrice(entry_range), 5)
		elif entry_price:
			entry = entry_price
		else:
			entry = order["entry_price"]

		if sl_range:
			if order["direction"] == tl.LONG:
				sl = round(order["entry_price"] - tl.utils.convertToPrice(sl_range), 5)
			else:
				sl = round(order["entry_price"] + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price:
			sl = sl_price
		else:
			sl = order["sl"]

		if tp_range:
			if order["direction"] == tl.LONG:
				tp = round(order["entry_price"] + tl.utils.convertToPrice(tp_range), 5)
			else:
				tp = round(order["entry_price"] - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price:
			tp = tp_price
		else:
			tp = order["tp"]

		# Check for min requirements

		# Call to broker
		return self._broker.modifyOrder(order, lotsize, entry, sl, tp, override=override)
		

	def modifyEntry(self, order, entry_range=None, entry_price=None):
		return self.modify(order, entry_range=entry_range, entry_price=entry_price)

	def modifySL(self, order, sl_range=None, sl_price=None):
		return self.modify(order, sl_range=sl_range, sl_price=sl_price)

	def modifyTP(self, order, tp_range=None, tp_price=None):
		return self.modify(order, tp_range=tp_range, tp_price=tp_price)
