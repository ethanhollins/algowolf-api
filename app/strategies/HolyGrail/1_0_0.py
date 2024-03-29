'''

Scalpius
	Version: 1.0.0
	Strategy author: Lenny Keys
	Script author: Ethan Hollins

	licensed to Tymraft Pty. Ltd.
	
'''

from enum import Enum
import json
import math


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

def getIndName(period, chart, ind_name):
	if period == CHART_A:
		return ind_name + '_a'
	elif period == CHART_B:
		return ind_name + '_b'
	elif period == CHART_C:
		return ind_name + '_c'


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


def getMaValue(ma, offset=0):
	return ma.bids[offset, 0]


def getAtrValue(atr, offset=0):
	return atr.bids[offset, 0]


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


def getRoundedPrice(x, direction, reverse=False):
	if reverse:
		if direction == LONG:
			x = utils.convertToPips(x) - .5
			x += x % .5
			return utils.convertToPrice(x)

		else:
			x = utils.convertToPips(x) + .5
			x -= x % .5
			return utils.convertToPrice(x)
		
	else:
		if direction == LONG:
			x = utils.convertToPips(x) + .5
			x -= x % .5
			return utils.convertToPrice(x)

		else:
			x = utils.convertToPips(x) - .5
			x += x % .5
			return utils.convertToPrice(x)


def getTargetPrice(price, dist, direction, reverse=False):
	if reverse:
		if direction == LONG:
			dist = max(dist, utils.convertToPrice(3.0))
			return getRoundedPrice(price - (dist * 1.1), SHORT)

		else:
			dist = max(dist, utils.convertToPrice(3.0))
			return getRoundedPrice(price + (dist * 1.1), LONG)

	else:
		if direction == LONG:
			dist = max(dist, utils.convertToPrice(3.0))
			return getRoundedPrice(price + (dist * 1.1), LONG)

		else:
			dist = max(dist, utils.convertToPrice(3.0))
			return getRoundedPrice(price - (dist * 1.1), SHORT)


def getPeriodHL(period, chart, bars, direction, reverse=False):
	ohlc = chart.bids[period]

	if reverse:
		if direction == LONG:
			return min(ohlc[:bars])
		else:
			return max(ohlc[:bars])
	else:
		if direction == LONG:
			return max(ohlc[:bars])
		else:
			return min(ohlc[:bars])


def getPositionDirection():
	for pos in strategy.positions:
		return pos.direction

	return None


def getOrderDirection():
	for order in strategy.orders:
		return order.direction

	return None


def getSessionTimes(now):
	'''Calculate sessions times (assumes a less than 24 hour session)'''
	converted_time = utils.convertTimezone(now, TZ)

	# End Time
	end_time = converted_time.replace(
		hour=END_TIME[0], minute=END_TIME[1], second=0, microsecond=0
	) - timedelta(minutes=1)

	if time_state == TimeState.WAIT and end_time < converted_time:
		end_time += timedelta(days=1)

	# Start Time
	start_time = converted_time.replace(
		hour=START_TIME[0], minute=START_TIME[1], second=0, microsecond=0
	) - timedelta(minutes=1)
	if start_time > end_time:
		start_time -= timedelta(days=1)
	elif time_state == TimeState.WAIT and start_time < converted_time:
		start_time += timedelta(days=1)

	return start_time, end_time


def addOffset(x, y, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return y - x
		else:
			return y + x
	else:
		if direction == LONG:
			return y + x
		else:
			return y - x


def resetTriggers():
	return


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


def isRetestBarCancelConf(trigger):
	return (
		trigger.bars_passed > 5
	)


def isDVCancel(trigger):
	if trigger.direction == LONG:
		dv_trigger = dv_short_trigger_a
	else:
		dv_trigger = dv_long_trigger_a

	return (
		not isTrendingConfirmingEvidence(trigger.direction) and
		dv_trigger.state == DVState.ACTIVE
	)


# Confirming Evidence Confirmations
def isBollingerTouchOneConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	boll = chart.indicators[getIndName(period, chart, 'boll')]

	boll_val = getBollValue(boll, trigger.direction, reverse=True)
	hl = getHL(ohlc, trigger.direction, reverse=True)

	return (
		isTagged(hl, boll_val, trigger.direction, reverse=True)
	)


def isTrendingWithPullbackOneConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	ema_fast = chart.indicators[getIndName(period, chart, 'ema_fast')]

	ema_fast_val = getMaValue(ema_fast)
	hl = getHL(ohlc, trigger.direction, reverse=True)

	return (
		isTagged(hl, ema_fast_val, trigger.direction, reverse=True)
	)


def isTrendingWithPullbackIntraConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	boll = chart.indicators[getIndName(period, chart, 'boll')]
	ema_fast = chart.indicators[getIndName(period, chart, 'ema_fast')]

	boll_val = getBollValue(boll, trigger.direction, reverse=True)
	ema_fast_val = getMaValue(ema_fast)
	hl = getHL(ohlc, trigger.direction, reverse=True)
	off = utils.convertToPrice(1.0)

	return (
		isTagged(hl, addOffset(off, ema_fast_val), trigger.direction, reverse=True) and
		not isTagged(hl, boll_val, trigger.direction, reverse=True)
	)


def isBollingerWalkOneConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	boll = chart.indicators[getIndName(period, chart, 'boll')]

	boll_val = getBollValue(boll, trigger.direction)

	return (
		isCrossed(ohlc[0, 3], boll_val, trigger.direction)
	)


def isDvTrendingOneConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	atr = chart.indicators[getIndName(period, chart, 'atr')]

	curr_atr_val = getAtrValue(atr)
	prev_atr_val = getAtrValue(atr, offset=1)

	return (
		prev_atr_val < curr_atr_val and
		trigger.close is None or isCrossed(ohlc[0, 3], trigger.close, trigger.direction)
	)


def isDvPivotOneConf(period, chart, trigger):
	atr = chart.indicators[getIndName(period, chart, 'atr')]

	curr_atr_val = getAtrValue(atr)
	prev_atr_val = getAtrValue(atr, offset=1)

	return (
		prev_atr_val > curr_atr_val
	)


def isDvPivotTwoConf(period, chart, trigger):
	atr = chart.indicators[getIndName(period, chart, 'atr')]

	curr_atr_val = getAtrValue(atr)
	prev_atr_val = getAtrValue(atr, offset=1)

	return (
		prev_atr_val < curr_atr_val
	)


def isDvNewHighCancelConf(period, chart, trigger):
	atr = chart.indicators[getIndName(period, chart, 'atr')]
	atr_val = getAtrValue(atr)

	return (
		atr_val > trigger.high
	)


def isDvPivotCancelConf(period, chart, trigger):
	atr = chart.indicators[getIndName(period, chart, 'atr')]
	atr_val = getAtrValue(atr)

	return (
		trigger.pivot is not None and
		atr_val > trigger.pivot
	)


def isDvEntryOneConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	ema_slow = chart.indicators[getIndName(period, chart, 'ema_slow')]
	ema_fast = chart.indicators[getIndName(period, chart, 'ema_fast')]

	ema_slow_val = getMaValue(ema_slow)
	ema_fast_val = getMaValue(ema_fast)
	hl = getHL(ohlc, trigger.direction)

	return (
		(isTagged(hl, ema_slow_val, trigger.direction) or
				isTagged(hl, ema_fast_val, trigger.direction)) and
		isHammer(ohlc, trigger.direction)
	)


def isTrendingConfirmingEvidence(direction):
	if direction == LONG:
		return (
			(trending_with_pullback_long_trigger_b.state == TrendingWithPullbackState.ACTIVE or
				bollinger_walk_long_trigger_b.state == BollingerWalkState.ACTIVE or
				dv_long_trigger_b.state == DVState.ACTIVE) and
			(trending_with_pullback_long_trigger_c.state == TrendingWithPullbackState.ACTIVE or
				bollinger_walk_long_trigger_c.state == BollingerWalkState.ACTIVE or
				dv_long_trigger_c.state == DVState.ACTIVE)
		)

	else:
		return (
			(trending_with_pullback_short_trigger_b.state == TrendingWithPullbackState.ACTIVE or
				bollinger_walk_short_trigger_b.state == BollingerWalkState.ACTIVE or
				dv_short_trigger_b.state == DVState.ACTIVE) and
			(trending_with_pullback_short_trigger_c.state == TrendingWithPullbackState.ACTIVE or
				bollinger_walk_short_trigger_c.state == BollingerWalkState.ACTIVE or
				dv_short_trigger_c.state == DVState.ACTIVE)
		)


def isBBConfirmingEvidence(direction):
	if direction == LONG:
		return (
			bollinger_touch_long_trigger_b.state == BollingerTouchState.ACTIVE and
			bollinger_touch_long_trigger_c.state == BollingerTouchState.ACTIVE
		)

	else:
		return (
			bollinger_touch_short_trigger_b.state == BollingerTouchState.ACTIVE and
			bollinger_touch_short_trigger_c.state == BollingerTouchState.ACTIVE
		)



# Confirming Evidence Cancellations
def isConfirmingEvidenceBarCancelConf(trigger):
	return (
		trigger.bars_passed > 2
	)


def isBollingerWalkCancelConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	boll = chart.indicators[getIndName(period, chart, 'boll')]

	boll_val = getBollValue(boll, trigger.direction)
	hl = getHL(ohlc, trigger.direction)

	return (
		isTagged(hl, boll_val, trigger.direction, reverse=True)
	)


# Trend Confirmations
def isTrendOneConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	boll = chart.indicators[getIndName(period, chart, 'boll')]

	boll_val = getBollValue(boll, trigger.direction)
	hl = getHL(ohlc, trigger.direction)

	return (
		isTagged(hl, boll_val, trigger.direction)
	)


def isTrendTwoConf(period, chart, trigger):
	ohlc = chart.bids[period]
	hl = getHL(ohlc, trigger.direction)
	close = ohlc[0, 3]

	return (
		isCrossed(hl, trigger.hl, trigger.direction) and
		isCrossed(close, trigger.close, trigger.direction)
	)


def isTrending(direction, reverse=False):
	if reverse:
		if direction == LONG:
			return (
				trend_short_trigger_b.state == TrendState.ACTIVE and
				trend_short_trigger_c.state == TrendState.ACTIVE
			)

		else:
			return (
				trend_long_trigger_b.state == TrendState.ACTIVE and
				trend_long_trigger_c.state == TrendState.ACTIVE
			)

	else:
		if direction == LONG:
			return (
				trend_long_trigger_b.state == TrendState.ACTIVE and
				trend_long_trigger_c.state == TrendState.ACTIVE
			)

		else:
			return (
				trend_short_trigger_b.state == TrendState.ACTIVE and
				trend_short_trigger_c.state == TrendState.ACTIVE
			)

# Trend Cancellations


# Bollinger Cancel Confirmation
def isTrendBollingerCancelConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	boll = chart.indicators[getIndName(period, chart, 'boll')]

	boll_val = getBollValue(boll, trigger.direction, reverse=True)
	hl = getHL(ohlc, trigger.direction, reverse=True)

	return (
		isTagged(hl, boll_val, trigger.direction, reverse=True)
	)

# Bars Passed Cancel Confirmation
def isTrendBarsPassedCancelConf(period, chart, trigger):
	return (
		trigger.bars_passed > 20
	)

# Swing Cancel Confirmations
def isTrendSwingOneCancelConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	ema_slow = chart.indicators[getIndName(period, chart, 'ema_slow')]
	ema_fast = chart.indicators[getIndName(period, chart, 'ema_fast')]

	ema_slow_val = getMaValue(ema_slow)
	ema_fast_val = getMaValue(ema_fast)

	return (
		isCrossed(ohlc[0, 3], ema_slow_val, trigger.direction, reverse=True) and
		isCrossed(ohlc[0, 3], ema_fast_val, trigger.direction, reverse=True)
	)


def isTrendSwingTwoCancelConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	ema_slow = chart.indicators[getIndName(period, chart, 'ema_slow')]
	ema_fast = chart.indicators[getIndName(period, chart, 'ema_fast')]

	ema_slow_val = getMaValue(ema_slow)
	ema_fast_val = getMaValue(ema_fast)

	return (
		isCrossed(ohlc[0, 3], ema_slow_val, trigger.direction) and
		isCrossed(ohlc[0, 3], ema_fast_val, trigger.direction)
	)


def isTrendSwingThreeCancelConf(period, chart, trigger):
	# Select Correct Chart Period
	ohlc = chart.bids[period]
	ema_slow = chart.indicators[getIndName(period, chart, 'ema_slow')]
	ema_fast = chart.indicators[getIndName(period, chart, 'ema_fast')]

	ema_slow_val = getMaValue(ema_slow)
	ema_fast_val = getMaValue(ema_fast)

	return (
		isCrossed(ohlc[0, 3], ema_slow_val, trigger.direction, reverse=True) and
		isCrossed(ohlc[0, 3], ema_fast_val, trigger.direction, reverse=True)
	)

# High and Tight Confirmations

def isTrendingHighAndTightConf(period, chart, trigger, target):
	ohlc = chart.bids[period]
	period_hl = getPeriodHL(period, chart, 25, trigger.direction)
	period_hl = addOffset(period_hl, 1.5, trigger.direction)

	return (
		not isCrossed(target, period_hl, trigger.direction)
	)


def isOppTrendingHighAndTightConf(period, chart, trigger, target):
	ohlc = chart.bids[period]
	mae = chart.indicators[getIndName(period, chart, 'mae')]

	mae_val = getMaeValue(mae, trigger.direction)

	return (
		not isCrossed(target, mae_val, trigger.direction)
	)


def isNoTrendHighAndTightConf(period, chart, trigger, target):
	ohlc = chart.bids[period]
	period_hl = getPeriodHL(period, ohlc, 25, trigger.direction)

	return (
		not isCrossed(target, period_hl, trigger.direction)
	)

# Order Confirmations
def isBetterEntryConf(pending_order, trigger):
	return (
		trigger.pending_order is None or
		isCrossed(
			pending_order.entry_price,
			trigger.pending_order.entry_price,
			trigger.direction, reverse=True
		)
	)


'''
Events
'''

def confirmation(pending_order):
	# Check current position conditions
	if time_state == TimeState.TRADING:
		if getPositionDirection() != pending_order.direction:

			# Check current order conditions
			order_direction = getOrderDirection()
			if order_direction == pending_order.direction:
				# Modify existing order
				for order in strategy.orders:
					order.modify(
						entry_price=pending_order.entry_price,
						sl_price=pending_order.sl_price,
						tp_price=pending_order.tp_price
					)

			else:
				# Cancel opposite orders
				if order_direction != pending_order.direction:
					for order in strategy.orders:
						order.cancel()

				# Place new order
				if pending_order.direction == LONG:
					result = strategy.buy(
						product.EURUSD, 1.0, order_type=STOP_ORDER,
						entry_price=pending_order.entry_price,
						sl_price=pending_order.sl_price,
						tp_price=pending_order.tp_price
					)

				else:
					result = strategy.sell(
						product.EURUSD, 1.0, order_type=STOP_ORDER,
						entry_price=pending_order.entry_price,
						sl_price=pending_order.sl_price,
						tp_price=pending_order.tp_price
					)


def cancelOrders():
	for order in strategy.orders:
		order.cancel()


# RTV/RTC
def onRtvSetup(chart, trigger):
	ohlc = chart.bids[CHART_A]

	if trigger.state.value > RtvState.ONE:
		trigger.bars_passed += 1
		if isRtvBarCancelConf(trigger):
			trigger.reset()

	if trigger.state == RtvState.ONE:
		if isRtvOneConf(chart, trigger.direction):
			trigger.state = RtvState.TWO
			trigger.setSwing(
				getRoundedPrice(getHL(ohlc, trigger.direction, reverse=True), reverse=True)
			)
			return onRtvSetup(chart, trigger)

	elif trigger.state == RtvState.TWO:
		if isRtvTwoConf(chart, trigger.direction):
			trigger.state = RtvState.THREE

	elif trigger.state == RtvState.THREE:
		if isRtvThreeConf(chart, trigger.direction):
			trigger.state = RtvState.FOUR

	elif trigger.state == RtvState.FOUR:
		# Calc order prices
		entry_price = getRoundedPrice(getHL(chart.bids[CHART_A], trigger.direction), trigger.direction)
		sl_price = trigger.swing
		tp_price = getTargetPrice(entry_price, abs(entry_price - trigger.swing), trigger.direction)
		pending_order = PendingOrder(trigger.direction, entry_price, sl_price, tp_price)

		if isBetterEntryConf(pending_order, trigger):
			if not isDVCancel(trigger):
				trigger.pending_order = pending_order
				# Is Trending in Entry Direction
				if isTrending(trigger.direction):
					if isTrendingConfirmingEvidence(trigger.direction):
						if isTrendingHighAndTightConf(period, chart, trigger, tp_price):
							confirmation(trigger.pending_order)

				# Is Trending opposite to Entry Direction
				elif isTrending(trigger.direction, reverse=True):
					if isBBConfirmingEvidence(trigger.direction):
						if isOppTrendingHighAndTightConf(period, chart, trigger, tp_price):
							confirmation(trigger.pending_order)

				# Is not Trending
				else:
					if isBBConfirmingEvidence(trigger.direction):
						if isNoTrendHighAndTightConf(period, chart, trigger, tp_price):
							confirmation(trigger.pending_order)


def onRtcSetup(chart, trigger):

	if trigger.state.value > RtcState.ONE:
		trigger.bars_passed += 1
		if isRetestBarCancelConf(trigger):
			if trigger.pending_order is not None:
				cancelOrders()
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
		# Calc order prices
		entry_price = getRoundedPrice(getHL(chart.bids[CHART_A], trigger.direction), trigger.direction)
		sl_price = trigger.swing
		tp_price = getTargetPrice(entry_price, abs(entry_price - trigger.swing), trigger.direction)
		pending_order = PendingOrder(trigger.direction, entry_price, sl_price, tp_price)

		if isBetterEntryConf(pending_order, trigger):
			if not isDVCancel(trigger):
				trigger.pending_order = pending_order
				# Is Trending in Entry Direction
				if isTrending(trigger.direction):
					if isTrendingConfirmingEvidence(trigger.direction):
						if isTrendingHighAndTightConf(period, chart, trigger, tp_price):
							confirmation(trigger.pending_order)

				# Is Trending opposite to Entry Direction
				elif isTrending(trigger.direction, reverse=True):
					if isBBConfirmingEvidence(trigger.direction):
						if isOppTrendingHighAndTightConf(period, chart, trigger, tp_price):
							confirmation(trigger.pending_order)

				# Is not Trending
				else:
					if isBBConfirmingEvidence(trigger.direction):
						if isNoTrendHighAndTightConf(period, chart, trigger, tp_price):
							confirmation(trigger.pending_order)


def onRetestCancelSetup(chart, trigger):
	
	if isRetestCancelConf(chart, trigger):
		if trigger.pending_order is not None:
			cancelOrders()
		trigger.reset()


# Trend
def trendBarEndCancelSetup(period, chart, trigger):

	if trigger.state == TrendState.ACTIVE:
		# Bollinger Cancel
		if isTrendBollingerCancelConf(period, chart, trigger):
			trigger.turnOff()

		# Bars Passed Cancel
		elif isTrendBarsPassedCancelConf(period, chart, trigger):
			trigger.turnOff()

		# Swing Cancel 
		elif trigger.swing_cancel_state == TrendSwingCancelState.ONE:
			if isTrendSwingOneCancelConf(period, chart, trigger):
				trigger.swing_cancel_state = TrendSwingCancelState.TWO

		elif trigger.swing_cancel_state == TrendSwingCancelState.TWO:
			if isTrendSwingTwoCancelConf(period, chart, trigger):
				trigger.swing_cancel_state = TrendSwingCancelState.THREE

		elif trigger.swing_cancel_state == TrendSwingCancelState.THREE:
			if isTrendSwingThreeCancelConf(period, chart, trigger):
				trigger.swing_cancel_state = TrendSwingCancelState.ONE
				trigger.turnOff()


def trendBarEndSetup(period, chart, trigger):
	ohlc = chart.bids[period]

	if trigger.state == TrendState.ONE:
		if isTrendOneConf(period, chart, trigger):
			trigger.state = TrendState.TWO
			trigger.setHL(getHL(ohlc, trigger.direction))
			trigger.setClose(ohlc[0,3])

	elif trigger.state == TrendState.TWO:
		if isTrendTwoConf(period, chart, trigger):
			trigger.state = TrendState.ACTIVE

		else:
			trigger.setHL(getHL(ohlc, trigger.direction))
			trigger.setClose(ohlc[0,3])


# Confiming Evidence
def bollingerTouchBarEndSetup(chart, trigger):
	
	if trigger.state == BollingerTouchState.ACTIVE:
		trigger.bars_passed += 1
		if isConfirmingEvidenceBarCancelConf(trigger):
			trigger.reset()


def bollingerTouchTickSetup(period, chart, trigger):

	if trigger.state == BollingerTouchState.ONE:
		if isBollingerTouchOneConf(period, chart, trigger):
			trigger.setActive()


def trendingWithPullbackTickSetup(period, chart, trigger, trend_trigger):

	if trend_trigger.state == TrendState.ACTIVE:
		if isTrendingWithPullbackIntraConf(period, chart, trigger):
			trigger.intra_state = TrendingWithPullbackState.ACTIVE

		else:
			trigger.intra_state = TrendingWithPullbackState.ONE


def trendingWithPullbackBarEndSetup(period, chart, trigger, trend_trigger):
	
	if trend_trigger.state == TrendState.ACTIVE:
		if trigger.state == TrendingWithPullbackState.ONE:
			if isTrendingWithPullbackOneConf(period, chart, trigger):
				trigger.state = TrendingWithPullbackState.ACTIVE

		elif trigger.state == TrendingWithPullbackState.ACTIVE:
			trigger.bars_passed += 1
			if isConfirmingEvidenceBarCancelConf(period, chart, trigger):
				trigger.state = TrendingWithPullbackState.ONE


def bollingerWalkSetup(period, chart, trigger):
	
	if trigger.state == BollingerWalkState.ONE:
		if isBollingerWalkOneConf(period, chart, trigger):
			trigger.state = BollingWalkState.ACTIVE

	elif trigger.state == BollingerWalkState.ACTIVE:
		trigger.bars_passed += 1
		if isBollingerWalkCancelConf(period, chart, trigger):
			trigger.state = BollingWalkState.ONE

		elif isConfirmingEvidenceBarCancelConf(period, chart, trigger):
			trigger.state = BollingWalkState.ONE


def dvTrendingSetup(period, chart, trigger, trend_trigger):
	
	ohlc = chart[period]
	atr = chart.indicators[getIndName(period, chart, 'atr')]
	# DV Handling
	if trigger.state == DVState.ACTIVE:
		
		# Pivot Setup
		if trigger.pivot_state == DVPivotState.ONE:
			if isDvPivotOneConf(period, chart, trigger):
				trigger.pivot_count += 1
				if trigger.pivot_count == 2:
					trigger.new_pivot = getAtrValue(atr)
					trigger.pivot_state = DVPivotState.TWO
					trigger.pivot_count = 0
			else:
				trigger.pivot_count = 0

		elif trigger.pivot_state == DVPivotState.TWO:
			if isDvPivotTwoConf(period, chart, trigger):
				trigger.pivot_count += 1
				if trigger.pivot_count == 2:
					trigger.pivot = trigger.new_pivot
					trigger.pivot_state = DVPivotState.ONE
			else:
				trigger.pivot_state = DVPivotState.ONE
				trigger.pivot_count = 0

		# New High Cancel
		if isDvNewHighCancelConf(period, chart, trigger):
			trigger.state = DVState.ONE

		# Pivot Cancel
		if isDvPivotCancelConf(period, chart, trigger):
			trigger.state = DVState.ONE

	# DV Activation
	if trend_trigger.state == TrendState.ACTIVE:
		trigger.setHigh(getAtrValue(atr))
		if trigger.state == DVState.ONE:
			if isDvTrendingOneConf(period, chart, trigger):
				trigger.state = DVState.ACTIVE

		trigger.setClose(ohlc.bids[0, 3])

	elif trigger.state == DVState.ONE:
		trigger.reset()


def dvTrendingEntrySetup(period, chart, trigger):
	ohlc = chart[period]

	if trigger.state == DVState.ACTIVE:
		entry_price = getRoundedPrice(ohlc[0, 3], trigger.direction)
		sl_price = getRoundedPrice(getHL(ohlc, trigger.direction, reverse=True), trigger.direction)
		tp_price = getTargetPrice(entry_price, abs(entry_price - sl_price), trigger.direction)
		pending_order = PendingOrder(trigger.direction, entry_price, sl_price, tp_price)

		if trigger.entry_state == DVEntryState.ONE:

			if isDvEntryOneConf(period, chart, trigger):
				if isTrendingConfirmingEvidence(trigger.direction):
					trigger.pending_order = pending_order
					# Calc order prices
					confirmation(trigger.pending_order)

			elif trigger.pending_order is not None:
				if entry_price != trigger.pending_order.entry_price:
					cancelOrders()
					trigger.pending_order = None

				elif isCrossed(
					sl_price, trigger.pending_order.sl_price, 
					trigger.direction, reverse=True
				):
					trigger.pending_order = pending_order
					confirmation(trigger.pending_order)


def onTime(timestamp, chart):
	global time_state, session, bank
	# Get session times
	now = utils.convertTimezone(utils.convertTimestampToTime(timestamp), TZ)
	start_time, end_time = getSessionTimes(now)

	# Set time state
	if time_state == TimeState.WAIT:
		if now >= start_time:
			# Reset globals
			time_state = TimeState.TRADING
			bank = strategy.getBalance()
			session = []
			resetTriggers()

			# Draw session line
			# drawSessionStartLine(chart)

	elif time_state == TimeState.TRADING:
		if now > end_time:
			time_state = TimeState.WAIT
			# Draw session line
			# drawSessionEndLine(chart)


'''
TWO MINUTES
'''

# Bar End
def onBarEndA(timestamp, chart):
	
	# Run DV Trending setup
	dvTrendingSetup(CHART_A, chart, dv_long_trigger_a)
	dvTrendingSetup(CHART_A, chart, dv_short_trigger_a)

	# Run DV Trending Entry setup
	dvTrendingEntrySetup(CHART_A, chart, dv_long_trigger_a)
	dvTrendingEntrySetup(CHART_A, chart, dv_short_trigger_a)

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
	
	# Run Bollinger Touch setup
	bollingerTouchBarEndSetup(CHART_B, chart, bollinger_touch_long_trigger_b)
	bollingerTouchBarEndSetup(CHART_B, chart, bollinger_touch_short_trigger_b)

	# Run Trending with Pullback setup
	trendingWithPullbackBarEndSetup(CHART_B, chart, trending_with_pullback_long_trigger_b, trend_long_trigger_b)
	trendingWithPullbackBarEndSetup(CHART_B, chart, trending_with_pullback_short_trigger_b, trend_long_trigger_b)

	# Run Bollinger Walk setup
	bollingerWalkSetup(CHART_B, chart, bollinger_walk_long_trigger_b)
	bollingerWalkSetup(CHART_B, chart, bollinger_walk_short_trigger_b)

	# Run DV Trending setup
	dvTrendingSetup(CHART_B, chart, dv_long_trigger_b)
	dvTrendingSetup(CHART_B, chart, dv_short_trigger_b)


# Tick
def onTickB(timestamp, chart):
	
	# Run Bollinger Touch setup
	bollingerTouchTickSetup(CHART_B, chart, bollinger_touch_long_trigger_b)
	bollingerTouchTickSetup(CHART_B, chart, bollinger_touch_short_trigger_b)

	# Run Trending with Pullback setup
	trendingWithPullbackTickSetup(CHART_B, chart, trending_with_pullback_long_trigger_b, trend_long_trigger_b)
	trendingWithPullbackTickSetup(CHART_B, chart, trending_with_pullback_short_trigger_b, trend_long_trigger_b)


'''
TEN MINUTES
'''

# Bar End
def onBarEndC(timestamp, chart):
	
	# Run Bollinger Touch setup
	bollingerTouchBarEndSetup(CHART_C, chart, bollinger_touch_long_trigger_c)
	bollingerTouchBarEndSetup(CHART_C, chart, bollinger_touch_short_trigger_c)

	# Run Trending with Pullback setup
	trendingWithPullbackBarEndSetup(CHART_C, chart, trending_with_pullback_long_trigger_c, trend_long_trigger_c)
	trendingWithPullbackBarEndSetup(CHART_C, chart, trending_with_pullback_short_trigger_c, trend_long_trigger_c)

	# Run Bollinger Walk setup
	bollingerWalkSetup(CHART_C, chart, bollinger_walk_long_trigger_c)
	bollingerWalkSetup(CHART_C, chart, bollinger_walk_short_trigger_c)

	# Run DV Trending setup
	dvTrendingSetup(CHART_C, chart, dv_long_trigger_c)
	dvTrendingSetup(CHART_C, chart, dv_short_trigger_c)


# Tick
def onTickC(timestamp, chart):
	
	# Run Bollinger Touch setup
	bollingerTouchTickSetup(CHART_C, chart, bollinger_touch_long_trigger_c)
	bollingerTouchTickSetup(CHART_C, chart, bollinger_touch_short_trigger_c)

	# Run Trending with Pullback setup
	trendingWithPullbackTickSetup(CHART_C, chart, trending_with_pullback_long_trigger_c, trend_long_trigger_c)
	trendingWithPullbackTickSetup(CHART_C, chart, trending_with_pullback_short_trigger_c, trend_long_trigger_c)


'''
Setup
'''

def setInputs():
	global CHART_A, CHART_B, CHART_C
	CHART_A = period.TWO_MINUTES
	CHART_B = period.FIVE_MINUTES
	CHART_C = period.TEN_MINUTES


def setGlobals():
	global session, time_state, bank
	session = []
	time_state = TimeState.WAIT
	bank = 0

	global START_TIME, END_TIME
	START_TIME = [7, 0]
	END_TIME = [13, 0]

	globals rtv_long_trigger, rtv_short_trigger
	rtv_long_trigger = RtvTrigger(LONG)
	rtv_short_trigger = RtvTrigger(SHORT)

	globals rtc_long_trigger, rtc_short_trigger
	rtc_long_trigger = RtcTrigger(LONG)
	rtc_short_trigger = RtcTrigger(SHORT)

	globals trend_long_trigger_b, trend_short_trigger_b
	trend_long_trigger_b = TrendTrigger(LONG)
	trend_short_trigger_b = TrendTrigger(SHORT)

	globals trend_long_trigger_c, trend_short_trigger_c
	trend_long_trigger_c = TrendTrigger(LONG)
	trend_short_trigger_c = TrendTrigger(SHORT)

	globals bollinger_touch_long_trigger_b, bollinger_touch_short_trigger_b
	bollinger_touch_long_trigger_b = BollingerTouchTrigger(LONG)
	bollinger_touch_short_trigger_b = BollingerTouchTrigger(SHORT)

	globals bollinger_touch_long_trigger_c, bollinger_touch_short_trigger_c
	bollinger_touch_long_trigger_c = BollingerTouchTrigger(LONG)
	bollinger_touch_short_trigger_c = BollingerTouchTrigger(SHORT)

	globals bollinger_walk_long_trigger_b, bollinger_walk_short_trigger_b
	bollinger_walk_long_trigger_b = BollingerWalkTrigger(LONG)
	bollinger_walk_short_trigger_b = BollingerWalkTrigger(SHORT)

	globals bollinger_walk_long_trigger_c, bollinger_walk_short_trigger_c
	bollinger_walk_long_trigger_c = BollingerWalkTrigger(LONG)
	bollinger_walk_short_trigger_c = BollingerWalkTrigger(SHORT)

	globals trending_with_pullback_long_trigger_b, trending_with_pullback_short_trigger_b
	trending_with_pullback_long_trigger_b = TrendingWithPullbackTrigger(LONG)
	trending_with_pullback_short_trigger_b = TrendingWithPullbackTrigger(SHORT)

	globals trending_with_pullback_long_trigger_c, trending_with_pullback_short_trigger_c
	trending_with_pullback_long_trigger_c = TrendingWithPullbackTrigger(LONG)
	trending_with_pullback_short_trigger_c = TrendingWithPullbackTrigger(SHORT)

	globals dv_long_trigger_a, dv_short_trigger_a
	dv_long_trigger_a = DVTrigger(LONG)
	dv_short_trigger_a = DVTrigger(SHORT)

	globals dv_long_trigger_b, dv_short_trigger_b
	dv_long_trigger_b = DVTrigger(LONG)
	dv_short_trigger_b = DVTrigger(SHORT)

	globals dv_long_trigger_c, dv_short_trigger_c
	dv_long_trigger_c = DVTrigger(LONG)
	dv_short_trigger_c = DVTrigger(SHORT)


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
	
	if trade.type in (MARKET_ENTRY, STOP_ENTRY, LIMIT_ENTRY):
		global session
		session.append(trade.item)


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


class PendingOrder(BetterDict):

	def __init__(self, direction, entry_price, sl_price, tp_price):
		self.direction = direction
		self.entry_price = entry_price
		self.sl_price = sl_price
		self.tp_price = tp_price


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
		self.pending_order = None

		# Cancel Vars
		self.entry = None
		self.swing = None
		self.bars_passed = 0


	def setSwing(self, x):
		self.swing = x
		self.bars_passed = 0


	def reset(self):
		self.state = RtvState.ONE
		self.swing = None
		self.bars_passed = 0
		self.pending_order = None


class RtcState(Enum):
	ONE = 1
	TWO = 2
	THREE = 3
	COMPLETE = 4


class RtcTrigger(BetterDict):
	
	def __init__(self, direction):
		self.direction = direction
		self.state = RtcState.ONE
		self.pending_order = None

		# Cancel Vars
		self.entry = None
		self.swing = None
		self.bars_passed = 0


	def setSwing(self, x):
		self.swing = x
		self.bars_passed = 0


	def reset(self):
		self.state = RtvState.ONE
		self.swing = None
		self.bars_passed = 0
		self.pending_order = None


class TrendState(Enum):
	ONE = 1
	TWO = 2
	ACTIVE = 3


class TrendSwingCancelState(Enum):
	ONE = 1
	TWO = 2
	COMPLETE = 3


class TrendTrigger(BetterDict):

	def __init__(self, direction):
		self.direction = direction
		self.state = TrendState.ONE
		self.swing_cancel_state = TrendSwingCancelState.ONE

		self.hl = None
		self.close = None

		# Cancel Vars
		self.bars_passed = 0

		# Spike High
		self.spike_high = None
		self.spike_bars_passed = 0


	def setHL(self, x):
		if self.direction == LONG:
			if self.hl is None or x > self.hl:
				self.hl = x

		else:
			if self.hl is None or x < self.hl:
				self.hl = x


	def setClose(self, x):
		if self.direction == LONG:
			if self.close is None or x > self.close:
				self.close = x

		else:
			if self.close is None or x < self.close:
				self.close = x


	def turnOff(self):
		self.state = TrendState.TWO
		self.swing_cancel_state = TrendSwingCancelState.ONE
		self.bars_passed = 0


	def reset(self):
		self.state = TrendState.ONE
		self.swing_cancel_state = TrendSwingCancelState.ONE
		self.bars_passed = 0
		self.hl = None
		self.close = None


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


class TrendingWithPullbackState(Enum):
	ONE = 1
	ACTIVE = 2


class TrendingWithPullbackTrigger(BetterDict):

	def __init__(self, direction):
		self.direction = direction
		self.state = TrendingWithPullbackState.ONE
		self.intra_state = TrendingWithPullbackState.ONE

		# Cancel Vars 
		self.bars_passed = 0


	def reset(self):
		self.state = TrendingWithPullbackState.ONE
		self.bars_passed = 0


class BollingerWalkState(Enum):
	ONE = 1
	ACTIVE = 2


class BollingerWalkTrigger(BetterDict):
	
	def __init__(self, direction):
		self.direction = direction
		self.state = BollingerWalkState.ONE

		# Cancel Vars 
		self.bars_passed = 0

	def reset(self):
		self.state = BollingerWalkState.ONE
		self.bars_passed = 0


class DVState(Enum):
	ONE = 1
	ACTIVE = 2


class DVEntryState(Enum):
	ONE = 1
	TWO = 2
	COMPLETE = 3


class DVTrigger(BetterDict):

	def __init__(self, direction):
		self.direction = direction
		self.state = DVState.ONE
		self.entry_state = DVEntryState.ONE
		self.pending_order = None

		# Setup Vars
		self.close = None

		# ATR Vars
		self.high = None
		self.pivot = None

		self.new_pivot = None
		self.pivot_count = 0


	def setClose(self, x):
		if self.direction == LONG:
			if self.close is None or x > self.close:
				self.close = x
		else:
			if self.close is None or x < self.close:
				self.close = x


	def setHigh(self, x):
		if self.high is None or x > self.high:
			self.high = x=


	def reset(self):
		self.state = DVState.ONE

		self.close = None
		self.high = None
		self.pivot = None

		self.new_pivot = None
		self.pivot_count = 0
		self.pending_order = None


class TimeState(Enum):
	WAIT = 1
	TRADING = 2

