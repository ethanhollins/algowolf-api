'''

Scalpius
	Version: 1.0.0
	Strategy author: Lenny Keys
	Script author: Ethan Hollins

	licensed to Tymraft Pty. Ltd.
	
'''

from enum import Enum
import json


'''
Conditionals
'''

# Candlestick conditionals

def isHammer(chart, direction, reverse=False):
	if reverse:
		if direction == LONG:
			if chart[0, 3] > chart[0, 0]:
				wick = chart[0, 0] - chart[0, 2]
				body = chart[0, 3] - chart[0, 0]
			else:
				wick = chart[0, 3] - chart[0, 2]
				body = chart[0, 0] - chart[0, 3]
				
			mp = (chart[0, 1] - chart[0, 2])/2
			return (
				wick >= body * 2 and
				chart[0, 3] < mp
			)

		else:
			if chart[0, 0] > chart[0, 3]:
				wick = chart[0, 1] - chart[0, 0]
				body = chart[0, 0] - chart[0, 3]
			else:
				wick = chart[0, 1] - chart[0, 3]
				body = chart[0, 3] - chart[0, 0]

			mp = (chart[0, 1] - chart[0, 2])/2
			return (
				wick >= body * 2 and
				chart[0, 3] < mp
			)

	else:
		if direction == LONG:
			if chart[0, 0] > chart[0, 3]:
				wick = chart[0, 1] - chart[0, 0]
				body = chart[0, 0] - chart[0, 3]
			else:
				wick = chart[0, 1] - chart[0, 3]
				body = chart[0, 3] - chart[0, 0]

			mp = (chart[0, 1] - chart[0, 2])/2
			return (
				wick >= body * 2 and
				chart[0, 3] < mp
			)

		else:
			if chart[0, 3] > chart[0, 0]:
				wick = chart[0, 0] - chart[0, 2]
				body = chart[0, 3] - chart[0, 0]
			else:
				wick = chart[0, 3] - chart[0, 2]
				body = chart[0, 0] - chart[0, 3]
				
			mp = (chart[0, 1] - chart[0, 2])/2
			return (
				wick >= body * 2 and
				chart[0, 3] < mp
			)


def isBB(chart, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return chart[0, 0] - chart[0, 3] > 0
		else:
			return chart[0, 0] - chart[0, 3] < 0
	else:
		if direction == LONG:
			return chart[0, 0] - chart[0, 3] < 0
		else:
			return chart[0, 0] - chart[0, 3] > 0

# Misc conditionals

def isCrossed(x, y, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return x < y
		else:
			return x > y
	else:
		if direction == LONG:
			return x > y
		else:
			return x < y


def isTagged(x, y, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return x <= y
		else:
			return x >= y
	else:
		if direction == LONG:
			return x >= y
		else:
			return x <= y


'''
Utilities
'''

def getBollValue(boll, direction, offset=0, reverse=False):
	if reverse:
		if direction == LONG:
			return boll.bids[offset, 1]
		else:
			return boll.bids[offset, 0]
	else:
		if direction == LONG:
			return boll.bids[offset, 0]
		else:
			return boll.bids[offset, 1]


def getMaeValue(mae, direction, offset=0, reverse=False):
	if reverse:
		if direction == LONG:
			return mae.bids[offset, 1]
		else:
			return mae.bids[offset, 0]
	else:
		if direction == LONG:
			return mae.bids[offset, 0]
		else:
			return mae.bids[offset, 1]


def getHL(chart, direction, offset=0, reverse=False):
	if reverse:
		if direction == LONG:
			return chart[offset, 2]
		else:
			return chart[offset, 1]
	else:
		if direction == LONG:
			return chart[offset, 1]
		else:
			return chart[offset, 2]


'''
Confirmations
'''

# Retest Confirmations

# RTV Confirmations
def isRtvOneConf(chart, direction):
	boll_val = getBollValue(chart.indicators.boll_a, direction, reverse=True)
	hl = getHL(chart.bids[CHART_A], direction, reverse=True)

	return (
		isTagged(hl, boll_val, direction, reverse=True)
	)


def isRtvTwoConf(chart, direction):
	return (
		isBB(chart.bids[CHART_A], direction)
	)


def isRtvThreeConf(chart, direction):
	return (
		isBB(chart.bids[CHART_A], direction, reverse=True)
	)


# RTC Confirmations
def isRtcOneConf(chart, direction):
	donch_val = getDonchValue(chart.indicators.donch_one, direction, reverse=True)
	hl = getHL(chart.bids[CHART_A], direction, reverse=True)

	return (
		isTagged(hl, donch_val, direction, reverse=True)
	)


def isRtcTwoConf(chart, direction):
	return (
		isHammer(chart.bids[CHART_A], direction)
	)


# Retest Cancellations
def isRetestCancelConf(chart, trigger):
	hl = getHL(chart.bids[CHART_A], trigger.direction, reverse=True)

	return (
		isCrossed(hl, trigger.swing, trigger.direction, reverse=True)
	)


def isRtvBarCancelConf(trigger):
	return (
		trigger.bars_passed > 20
	)


def isRtcBarCancelConf(trigger):
	return (
		trigger.bars_passed > 5
	)


# Confirming Evidence Confirmations
def isBollingerTouchOneConf(period, chart, trigger):
	# Select Correct Chart Period
	if period == CHART_B:
		ohlc = chart.bids[CHART_B]
		boll = chart.indicators.boll_b
	else:
		ohlc = chart.bids[CHART_C]
		boll = chart.indicators.boll_c

	boll_val = getBollValue(boll, direction, reverse=True)
	hl = getHL(ohlc, direction, reverse=True)

	return (
		isTagged(hl, boll_val, direction, reverse=True)
	)


def isConfirmingEvidence(direction):
	if direction == LONG:
		return (
			bollinger_touch_long_trigger_b.state == BollingerTouchState.ACTIVE or
			bollinger_touch_long_trigger_c.state == BollingerTouchState.ACTIVE
		)

	else:
		return (
			bollinger_touch_short_trigger_b.state == BollingerTouchState.ACTIVE or
			bollinger_touch_short_trigger_c.state == BollingerTouchState.ACTIVE
		)


# Confirming Evidence Cancellations
def isBollingerTouchBarCancelConf(trigger):
	return (
		trigger.bars_passed > 2
	)


'''
Events
'''

# RTV/RTC
def onRtvSetup(chart, trigger):
	
	if trigger.state.value > RtvState.ONE:
		trigger.bars_passed += 1
		if isRtvBarCancelConf(trigger):
			trigger.reset()

	if trigger.state == RtvState.ONE:
		if isRtvOneConf(chart, trigger.direction):
			trigger.state = RtvState.TWO
			trigger.setSwing(getHL(chart.bids[CHART_A], trigger.direction, reverse=True))
			return onRtvSetup(chart, trigger)

	elif trigger.state == RtvState.TWO:
		if isRtvTwoConf(chart, trigger.direction):
			trigger.state = RtvState.THREE

	elif trigger.state == RtvState.THREE:
		if isRtvThreeConf(chart, trigger.direction):
			trigger.state = RtvState.FOUR

	elif trigger.state == RtvState.FOUR:
		if isConfirmingEvidence():
			trigger.state = RtvState.COMPLETE


def onRtcSetup(chart, trigger):

	if trigger.state.value > RtcState.ONE:
		trigger.bars_passed += 1
		if isRtcBarCancelConf(trigger):
			trigger.reset()

	if trigger.state == RtcState.ONE:
		if isRtcOneConf(chart, trigger.direction):
			trigger.state = RtcState.TWO
			trigger.setSwing(getHL(chart.bids[CHART_A], trigger.direction, reverse=True))
			return onRtvSetup(chart, trigger)

	elif trigger.state == RtcState.TWO:
		if isRtcTwoConf(chart, trigger.direction):
			trigger.state = RtcState.THREE

	elif trigger.state == RtcState.THREE:
		if isConfirmingEvidence():
			trigger.state = RtvState.COMPLETE


def onRetestCancelSetup(chart, trigger):
	
	if isRetestCancelConf(chart, trigger):
		trigger.reset()


# Confiming Evidence
def bollingerTouchBarEndSetup(chart, trigger):
	
	if trigger.state == BollingerTouchState.ACTIVE:
		trigger.bars_passed += 1
		if isBollingerTouchBarCancelConf(trigger):
			trigger.reset()


def bollingerTouchTickSetup(period, chart, trigger):

	if isBollingerTouchOneConf(period, chart, trigger):
		trigger.setActive()


def trendingWithPullbackSetup(chart, trigger):
	return


def bollingerWalkSetup(chart, trigger):
	return


def dvTrendingSetup(chart, trigger):
	return



def onTime(timestamp, chart):
	return

'''
TWO MINUTES
'''

# Bar End
def onBarEndA(timestamp, chart):
	
	# Check Decreasing volatility mode

	# Run RTV entry setup
	onRtvSetup(chart, rtv_long_trigger)
	onRtvSetup(chart, rtv_short_trigger)

	# Run RTC entry setup
	onRtcSetup(chart, rtc_long_trigger)
	onRtcSetup(chart, rtc_short_trigger)



# Tick
def onTickA(timestamp, chart):

	# Run RTV cancellation setup
	onRetestCancelSetup(chart, rtv_long_trigger)
	onRetestCancelSetup(chart, rtv_short_trigger)

	# Run RTC cancellation setup
	onRetestCancelSetup(chart, rtc_long_trigger)
	onRetestCancelSetup(chart, rtc_short_trigger)


'''
FIVE MINUTES
'''

# Bar End
def onBarEndB(timestamp, chart):
	return


# Tick
def onTickB(timestamp, chart):
	return

'''
TEN MINUTES
'''

# Bar End
def onBarEndC(timestamp, chart):
	return


# Tick
def onTickC(timestamp, chart):
	return


'''
Setup
'''

def setInputs():
	global CHART_A, CHART_B, CHART_C
	CHART_A = period.TWO_MINUTES
	CHART_B = period.FIVE_MINUTES
	CHART_C = period.TEN_MINUTES


def setGlobals():
	globals rtv_long_trigger, rtv_short_trigger
	rtv_long_trigger = RtvTrigger(LONG)
	rtv_short_trigger = RtvTrigger(SHORT)

	globals rtc_long_trigger, rtc_short_trigger
	rtc_long_trigger = RtcTrigger(LONG)
	rtc_short_trigger = RtcTrigger(SHORT)

	globals bollinger_touch_long_trigger_b, bollinger_touch_short_trigger_b
	bollinger_touch_long_trigger_b = BollingerTouchTrigger(LONG)
	bollinger_touch_short_trigger_b = BollingerTouchTrigger(SHORT)

	globals bollinger_touch_long_trigger_c, bollinger_touch_short_trigger_c
	bollinger_touch_long_trigger_c = BollingerTouchTrigger(LONG)
	bollinger_touch_short_trigger_c = BollingerTouchTrigger(SHORT)


def report(tick):
	return


'''
Hook functions
'''

def init():
	
	# Set Inputs
	setInputs()

	# Charts
	chart = strategy.getChart(product.EURUSD, CHART_A, CHART_B, CHART_C)

	# Indicators
	mae = indicator.MAE(21, type='EMA')
	
	chart.addIndicator('boll_a', CHART_A, indicator.BOLL(20, 2))
	chart.addIndicator('boll_b', CHART_B, indicator.BOLL(20, 2))
	chart.addIndicator('boll_c', CHART_C, indicator.BOLL(20, 2))

	chart.addIndicator('ema_slow_a', CHART_A, indicator.EMA(8))
	chart.addIndicator('ema_slow_b', CHART_B, indicator.EMA(8))
	chart.addIndicator('ema_slow_c', CHART_C, indicator.EMA(8))

	chart.addIndicator('ema_fast_a', CHART_A, indicator.EMA(21))
	chart.addIndicator('ema_fast_b', CHART_B, indicator.EMA(21))
	chart.addIndicator('ema_fast_c', CHART_C, indicator.EMA(21))

	chart.addIndicator('atr_a', CHART_A, indicator.ATR(14))
	chart.addIndicator('atr_b', CHART_B, indicator.ATR(14))
	chart.addIndicator('atr_c', CHART_C, indicator.ATR(14))




def onStart():
	# Clear any backtest positions if real positions exist
	if any([not pos.isBacktest() for pos in strategy.positions]):
		strategy.clearBacktestTrades()


def onTrade(trade):
	return


def onTick(tick):

	# Tick Handlers
	if tick.period == CHART_A:
		onTickA(tick.timestamp, tick.chart)

	elif tick.period == CHART_B:
		onTickB(tick.timestamp, tick.chart)

	elif tick.period == CHART_C:
		onTickC(tick.timestamp, tick.chart)

	# Bar End Handlers
	if tick.bar_end:
		if tick.period == CHART_A:
			onBarEndA(tick.timestamp, tick.chart)

		elif tick.period == CHART_B:
			onBarEndB(tick.timestamp, tick.chart)

		elif tick.period == CHART_C:
			onBarEndC(tick.timestamp, tick.chart)

		report(tick)


class BetterDict(dict):

	def __getattr__(self, key):
		return self[key]

	def __setattr__(self, key, value):
		self[key] = value


class RtvState(Enum):
	ONE = 1
	TWO = 2
	THREE = 3
	FOUR = 4
	COMPLETE = 5


class RtvTrigger(BetterDict):
	
	def __init__(self, direction):
		self.direction = direction
		self.state = RtvState.ONE

		# Cancel Vars
		self.swing = None
		self.bars_passed = 0


	def setSwing(self, x):
		self.swing = x
		self.bars_passed = 0


	def reset(self):
		self.state = RtvState.ONE
		self.swing = None
		self.bars_passed = 0



class RtcState(Enum):
	ONE = 1
	TWO = 2
	THREE = 3
	COMPLETE = 4


class RtcTrigger(BetterDict):
	
	def __init__(self, direction):
		self.direction = direction
		self.state = RtcState.ONE

		# Cancel Vars
		self.swing = None
		self.bars_passed = 0


	def setSwing(self, x):
		self.swing = x
		self.bars_passed = 0


	def reset(self):
		self.state = RtvState.ONE
		self.swing = None
		self.bars_passed = 0


class BollingerTouchState(Enum):
	ONE = 1
	ACTIVE = 2


class BollingerTouchTrigger(BetterDict):

	def __init__(self, direction):
		self.direction = direction
		self.state = BollingerTouchState.ONE

		# Cancel Vars
		self.bars_passed = 0


	def setActive(self):
		self.state = BollingerTouchState.ACTIVE
		self.bars_passed = 0


	def reset(self):
		self.state = BollingerTouchState.ONE
		self.bars_passed = 0


class TrendState(Enum):
	ONE = 1
	COMPLETE = 2


class TrendTrigger(BetterDict):

	def __init__(self, direction):
		self.direction = direction
		self.state = TrendState.ONE

		# Cancel Vars
		self.bars_passed = 0


	def reset(self):
		self.state = TrendState.ONE
		self.bars_passed = 0



