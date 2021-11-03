from app import tradelib as tl


class PositionManager(object):

	def __init__(self, broker):
		
		# Hidden Variable
		self._broker = broker
		super().__setattr__('_broker', broker)


	def close(self, pos, lotsize=None, override=False):
		if not lotsize: lotsize = pos["lotsize"]

		# Lotsize validation
		lotsize = min(pos["lotsize"], lotsize)
		# Call to broker
		return self._broker.deletePosition(pos, lotsize, override=override)


	def modify(self,
		pos,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None,
		override=False
	):
		# Convert to price
		if sl_range is not None:
			if pos["direction"] == tl.LONG:
				sl = round(pos["entry_price"] - tl.utils.convertToPrice(sl_range), 5)
			else:
				sl = round(pos["entry_price"] + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price is not None:
			sl = sl_price
		else:
			sl = pos["sl"]

		if tp_range is not None:
			if pos["direction"] == tl.LONG:
				tp = round(pos["entry_price"] + tl.utils.convertToPrice(tp_range), 5)
			else:
				tp = round(pos["entry_price"] - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price is not None:
			tp = tp_price
		else:
			tp = pos["tp"]

		# Check for min requirements

		# Call to broker
		return self._broker.modifyPosition(pos, sl, tp, override=override)


	def getProfit(self, pos):
		ask = self._broker.getAsk(pos["product"])
		bid = self._broker.getBid(pos["product"])

		if pos["direction"] == tl.LONG:
			if pos["close"]:
				return round(tl.utils.convertToPips(pos["close"] - pos["entry_price"]), 2)
			else:
				return round(tl.utils.convertToPips(bid - pos["entry_price"]), 2)
		else:
			if pos["close"]:
				return round(tl.utils.convertToPips(pos["entry_price"] - pos["close"]), 2)
			else:
				return round(tl.utils.convertToPips(pos["entry_price"] - ask), 2)

