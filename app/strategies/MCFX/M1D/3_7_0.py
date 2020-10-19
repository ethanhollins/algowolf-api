'''

Midas Code FX
	Version: 3.7.0
	Strategy author: Lenny Keys
	Script author: Ethan Hollins

	licensed to Tymraft Pty. Ltd.

DEFINITIONS:
	A/B (AB): 	Above (LONG) / Below (SHORT) 
	BB: 		Bullish / Bearish
	BL:			Baseline
	CT: 		Counter-Trend
	Doji: 		Candle body less than `x` size
	H/L:		High / Low
	Rev:		Reverse
	RL:			Reverse Line
	T: 			Trend
	Tag: 		Has touched or is AB
	X:			Cross

'''

from datetime import datetime, timedelta
from enum import Enum
from copy import copy
import json

'''
Conditionals
'''

# Donch functions
def isDonchFlat(chart, direction, length, offset=0, reverse=False):
	if reverse:
		if direction == LONG:
			vals = chart.indicators.donch.bids[offset:length+1+offset, 1]
			return all([i == vals[0] for i in vals])
		else:
			vals = chart.indicators.donch.bids[offset:length+1+offset, 0]
			return all([i == vals[0] for i in vals])
	else:
		if direction == LONG:
			vals = chart.indicators.donch.bids[offset:length+1+offset, 0]
			return all([i == vals[0] for i in vals])
		else:
			vals = chart.indicators.donch.bids[offset:length+1+offset, 1]
			return all([i == vals[0] for i in vals])


def isDonchTag(chart, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0,2] <= chart.indicators.donch.bids[0,1]
		else:
			return chart.bids.ONE_MINUTE[0,1] >= chart.indicators.donch.bids[0,0]
	else:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0,1] >= chart.indicators.donch.bids[0,0]
		else:
			return chart.bids.ONE_MINUTE[0,2] <= chart.indicators.donch.bids[0,1]


# Candlestick functions
def isBB(chart, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0, 0] - chart.bids.ONE_MINUTE[0, 3] > 0
		else:
			return chart.bids.ONE_MINUTE[0, 0] - chart.bids.ONE_MINUTE[0, 3] < 0
	else:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0, 0] - chart.bids.ONE_MINUTE[0, 3] < 0
		else:
			return chart.bids.ONE_MINUTE[0, 0] - chart.bids.ONE_MINUTE[0, 3] > 0


def isDoji(chart, size):
	return abs(utils.convertToPips(chart.bids.ONE_MINUTE[0, 0] - chart.bids.ONE_MINUTE[0, 3])) < size

# Misc functions
def isMinDist(x, y, dist):
	return utils.convertToPips(abs(x - y)) > dist

def isMinEqualDist(x, y, dist):
	return utils.convertToPips(abs(x - y)) >= dist


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


def isSessionLoss():
	profit = getSessionProfit()
	return profit / STOP_RANGE * RISK <= -(RISK * D_FIVE)


'''
Utilities
'''

def getPositionProfit():
	profit = 0
	for pos in strategy.positions:
		profit += pos.getProfit()

	return profit


def getSessionProfit():
	profit = 0
	for pos in session:
		profit += pos.getProfit()

	return profit


def getSessionPotentialProfit(chart):
	profit = 0
	for pos in session:
		if pos.close_price is None:
			if pos.direction == LONG:
				profit += utils.convertToPips(chart.bids.ONE_MINUTE[0, 1] - pos.entry_price)
			else:
				profit += utils.convertToPips(pos.entry_price - chart.bids.ONE_MINUTE[0, 2])
		else:
			profit += pos.getProfit()

	return profit


def getDonchValue(chart, direction, offset=0, reverse=False):
	if reverse:
		if direction == LONG:
			return chart.indicators.donch.bids[offset, 1]
		else:
			return chart.indicators.donch.bids[offset, 0]
	else:
		if direction == LONG:
			return chart.indicators.donch.bids[offset, 0]
		else:
			return chart.indicators.donch.bids[offset, 1]


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


def getExitTime(now, time):
	start_time, _ = getSessionTimes(now)

	exit_time = start_time.replace(hour=time[0], minute=time[1])
	if exit_time < start_time:
		exit_time += timedelta(days=1)

	return exit_time


def getHL(chart, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0, 2]
		else:
			return chart.bids.ONE_MINUTE[0, 1]
	else:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0, 1]
		else:
			return chart.bids.ONE_MINUTE[0, 2]


def getTrigger(direction, reverse=False):
	if reverse:
		if direction == LONG:
			return short_trigger
		else:
			return long_trigger
	else:
		if direction == LONG:
			return long_trigger
		else:
			return short_trigger


def getOppDirection(direction):
	if direction == LONG:
		return SHORT
	else:
		return LONG


def getCurrentPositionDirection():
	for pos in strategy.positions:
		return pos.direction

	return None

def getIGLotsize():
	global bank
	usable_bank = min(bank + EXTERNAL_BANK, MAXIMUM_BANK)
	aud_usd = strategy.getBid(product.AUD_USD)
	return max(round((usable_bank * (RISK / 100) / STOP_RANGE) * aud_usd, 2), 1.0)


'''
Confirmations
'''

# T Reverse Confirmations
def tRevFlatConf(chart, trigger):
	donch_val = getDonchValue(chart, trigger.direction)
	return (
		isCrossed(donch_val, trigger.reverse_line, trigger.direction) and
		isDonchFlat(chart, trigger.direction, B_FOUR)
	)


def tRevFlatCancelConf(chart, trigger):
	curr_donch = getDonchValue(chart, trigger.direction)
	prev_donch = getDonchValue(chart, trigger.direction, offset=1)

	return isCrossed(curr_donch, prev_donch, trigger.direction, reverse=True)


def tRevRLCrossCancelConf(chart, trigger):
	donch_val = getDonchValue(chart, trigger.direction)

	return isCrossed(donch_val, trigger.reverse_line, trigger.direction, reverse=True)


def tRevCtDonchCancelConf(chart, trigger):
	curr_donch = getDonchValue(chart, trigger.direction, reverse=True)
	prev_donch = getDonchValue(chart, trigger.direction, reverse=True, offset=1)

	return isCrossed(curr_donch, prev_donch, trigger.direction, reverse=True)


def tRevOneConf(chart, trigger):
	donch_val = getDonchValue(chart, trigger.direction)
	return (
		isCrossed(donch_val, trigger.reverse_line, trigger.direction) and
		isMinEqualDist(donch_val, trigger.reverse_line, B_ONE)
	)


def tRevTwoConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isCrossed(close, trigger.reverse_line, trigger.direction) and
		(trigger.t_rev_flat is None or isCrossed(close, trigger.t_rev_flat, trigger.direction)) and
		isBB(chart, trigger.direction) and not isDoji(chart, B_TWO) and
		isMinDist(trigger.reverse_line, close, B_THREE)
	)


def tRevThreeConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]
	donch_val = getDonchValue(chart, trigger.direction)
	hl = getHL(chart, trigger.direction)
	return (
		isCrossed(close, trigger.reverse_line, trigger.direction) and
		isMinDist(close, trigger.reverse_line, B_SIX) and
		isBB(chart, trigger.direction) and not isDoji(chart, B_FIVE) and
		isCrossed(hl, trigger.t_rev_hl, trigger.direction)
	)


def tTwoRevConf(chart, trigger):
	rev_donch_val = getDonchValue(chart, trigger.direction, reverse=True)
	ema_fast_val = chart.indicators.ema_fast.bids[0]
	sma_fast_val = chart.indicators.sma_fast.bids[0]
	sma_slow_val = chart.indicators.sma_slow.bids[0]

	return (
		isCrossed(rev_donch_val, trigger.baseline, trigger.direction) and
		isCrossed(ema_fast_val, sma_slow_val, trigger.direction) and
		isCrossed(sma_fast_val, sma_slow_val, trigger.direction)
	)



# Entry Confirmations
def goldCrossVariationConf(chart, direction):
	ema_fast_val = chart.indicators.ema_fast.bids[0]
	sma_fast_val = chart.indicators.sma_fast.bids[0]
	sma_slow_val = chart.indicators.sma_slow.bids[0]

	return (
		isCrossed(ema_fast_val, sma_slow_val, direction, reverse=True) and
		isCrossed(sma_fast_val, sma_slow_val, direction, reverse=True)
	)


def goldCrossOneConf(chart, direction, reverse=False):
	ema_fast_val = chart.indicators.ema_fast.bids[0]
	sma_fast_val = chart.indicators.sma_fast.bids[0]
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isCrossed(ema_fast_val, sma_fast_val, direction, reverse=not reverse) and
		isCrossed(close, sma_fast_val, direction, reverse=not reverse) and
		isCrossed(close, ema_fast_val, direction, reverse=not reverse)
	)


def goldCrossTwoConf(chart, direction, reverse=False):
	ema_fast_val = chart.indicators.ema_fast.bids[0]
	sma_fast_val = chart.indicators.sma_fast.bids[0]
	donch_val = getDonchValue(chart, direction)
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isTagged(ema_fast_val, sma_fast_val, direction, reverse=reverse) and
		isBB(chart, direction, reverse=not reverse) and not isDoji(chart, C_ONE) and
		isCrossed(close, sma_fast_val, direction, reverse=reverse) and
		isCrossed(close, ema_fast_val, direction, reverse=reverse)
	)


def goldCrossCancelConf(chart, direction, reverse=False):
	donch_val = getDonchValue(chart, direction)
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isDonchTag(chart, direction, reverse=reverse) or
		not isMinDist(close, donch_val, C_TWO)
	)


def isReEntryTDonchConf(chart, direction):
	donch_val = getDonchValue(chart, direction)
	hl = getHL(chart, trigger.direction)

	return isTagged(hl, donch_val, direction)


# Stop Line Confirmations
def stopLineOneConf(chart, direction):
	ema_fast_val = chart.indicators.ema_fast.bids[0]
	sma_slow_val = chart.indicators.sma_slow.bids[0]

	return isCrossed(ema_fast_val, sma_slow_val, direction, reverse=True)


def stopLineTwoConf(chart, direction):
	sma_fast_val = chart.indicators.sma_fast.bids[0]
	sma_slow_val = chart.indicators.sma_slow.bids[0]

	return isCrossed(sma_fast_val, sma_slow_val, direction)


def stopLineActiveOneConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isCrossed(close, trigger.stop_line, trigger.direction, reverse=True) and
		isBB(chart, trigger.direction, reverse=True) and not isDoji(chart, D_ONE) and
		isMinDist(close, trigger.stop_line, D_TWO)
	)


def stopLineActiveTwoConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]
	opp_trigger = getTrigger(trigger.direction, reverse=True)
	hl = getHL(chart, opp_trigger.direction)

	return (
		opp_trigger.reverse_line is None or
		not isTagged(hl, opp_trigger.reverse_line, opp_trigger.direction)
	)


# Exit Confirmations
def exitOneConf(chart):
	return getSessionPotentialProfit(chart) >= E_ONE


def exitTwoConf(chart):
	profit = getSessionPotentialProfit(chart)
	return (
		profit >= E_TWO
	)


def exitThreeConf(chart):
	profit = getSessionPotentialProfit(chart)
	return (
		profit >= E_THREE or
		profit / STOP_RANGE * RISK >= E_FOUR
	)


def exitOverrideOneConf(chart):
	profit = getSessionPotentialProfit(chart)
	return (
		profit / STOP_RANGE <= -D_THREE
	)


def exitOverrideTwoConf(chart):
	profit = getSessionPotentialProfit(chart)
	return (
		profit >= D_FOUR or
		profit / STOP_RANGE * RISK >= D_FOURTEEN
	)


'''
Events
'''

def confirmation(chart, trigger, reverse=False):
	'''On entry confirmation, enter position'''
	global time_state

	if time_state == TimeState.TRADING:

		if isSessionLoss():
			time_state = TimeState.NO_NEW_ENTRIES
			# Draw session line
			drawSessionLine(chart)
			return False

		if reverse:
			direction = getOppDirection(trigger.direction)
		else:
			direction = trigger.direction

		if len(strategy.positions) == 0:
			if direction == LONG:
				result = strategy.buy(
					product.GBPUSD, getIGLotsize(), sl_range=STOP_RANGE, tp_range=LIMIT_RANGE
				)

				strategy.draw(
					'arrowAltCircleUpRegular', 'arrows', product.GBPUSD,
					chart.bids.ONE_MINUTE[0, 1] + utils.convertToPrice(2.0), 
					chart.timestamps.ONE_MINUTE[0],
					color='#3498db', scale=7.0, rotation=0
				)
			else:
				result = strategy.sell(
					product.GBPUSD, getIGLotsize(), sl_range=STOP_RANGE, tp_range=LIMIT_RANGE
				)

				strategy.draw(
					'arrowAltCircleUpRegular', 'arrows', product.GBPUSD,
					chart.bids.ONE_MINUTE[0, 2] - utils.convertToPrice(2.0), 
					chart.timestamps.ONE_MINUTE[0],
					color='#f1c40f', scale=7.0, rotation=180
				)

			global session
			if isinstance(result, list):
				session += result
				return True

	return False


def exit(direction, re_enter=False, reverse=False):
	'''Exit all trades'''
	if reverse:
		direction = getOppDirection(direction)

	close_positions = [i for i in strategy.positions if i.direction == direction]
	if len(close_positions):
		strategy.closeAllPositions(close_positions)

		if re_enter:
			if direction == LONG:
				long_trigger.requires_variation = True
				long_trigger.stop_line_state = StopLineState.NONE
			else:
				short_trigger.requires_variation = True
				short_trigger.stop_line_state = StopLineState.NONE
				


def baselineSetup(chart, trigger):
	# Reverse Line (a)
	value = getDonchValue(chart, trigger.direction)
	if value == trigger.current_baseline:
		trigger.current_baseline_count += 1
	else:
		if trigger.current_baseline_count in range(3, A_ONE):
			trigger.preceding_baseline = value

		if trigger.baseline_state == BaselineState.COMPLETE_A:
			if (
				trigger.current_baseline_count in range(2, A_ONE) and
				isCrossed(trigger.current_baseline, trigger.baseline, 
							trigger.direction, reverse=True)
			):
				trigger.baseline_state = BaselineState.COMPLETE_B

		trigger.current_baseline = value
		trigger.current_baseline_count = 0

	if isDonchFlat(chart, trigger.direction, A_ONE):
		if trigger.isValidBaseline(value):
			trigger.setBaseline(value)

			if (
				trigger.preceding_baseline is not None and 
				isCrossed(trigger.preceding_baseline, trigger.baseline, 
							trigger.direction, reverse=True)
			):
				trigger.baseline_state = BaselineState.COMPLETE_B
			else:
				trigger.baseline_state = BaselineState.COMPLETE_A

			trigger.preceding_baseline = None

	# Handle Start Baseline
	if isDonchFlat(chart, trigger.direction, A_FOUR):
		trigger.start_baseline = value



def reverseLineSetup(chart, trigger):

	if trigger.baseline is not None and trigger.baseline_state != BaselineState.WAIT:
		# Check donch is A/B current baseline
		value = getDonchValue(chart, trigger.direction)

		# Reverse Line (b) (i)
		if isCrossed(value, trigger.baseline, trigger.direction):
			if isDonchFlat(chart, trigger.direction, A_TWO):

				if (trigger.baseline_state == BaselineState.COMPLETE_B or 
						isMinDist(trigger.baseline, value, A_THREE)):
					trigger.setReverseLine(value)
					trigger.baseline_state = BaselineState.WAIT


def tRevSetup(chart, trigger):
	
	if trigger.reverse_line is not None:

		if tRevFlatConf(chart, trigger):
			trigger.t_rev_flat = getDonchValue(chart, trigger.direction)

		if tRevFlatCancelConf(chart, trigger):
			trigger.t_rev_flat = None


		# Cancel on CT Donch tick
		if tRevCtDonchCancelConf(chart, trigger):
			trigger.resetReverseLine()
			trigger.resetBaseline()
			trigger.baseline_state = BaselineState.COMPLETE_A
			return

		# T2-Rev
		if trigger.t_rev_state != TRevState.COMPLETE and tTwoRevConf(chart, trigger):
			trigger.t_rev_state = TRevState.FOUR

		# T1-Rev
		if trigger.t_rev_state == TRevState.ONE:
			if tRevOneConf(chart, trigger):
				trigger.t_rev_state = TRevState.TWO
				return tRevSetup(chart, trigger)

		elif trigger.t_rev_state == TRevState.TWO:
			if tRevTwoConf(chart, trigger):
				trigger.t_rev_state = TRevState.THREE
				trigger.t_rev_hl = None
				trigger.setTRevHL(getHL(chart, trigger.direction))

		elif trigger.t_rev_state == TRevState.THREE:
			if tRevThreeConf(chart, trigger):
				trigger.t_rev_state = TRevState.FOUR
				return tRevSetup(chart, trigger)
			else:
				trigger.setTRevHL(getHL(chart, trigger.direction))

		elif trigger.t_rev_state == TRevState.FOUR:

			# Set Current Trigger
			trigger.t_rev_state = TRevState.COMPLETE
			trigger.entry_state = EntryState.ONE
			trigger.resetStopLine()
			# Set Opp Trigger
			opp_trigger = getTrigger(trigger.direction, reverse=True)
			opp_trigger.t_rev_state = TRevState.ONE

			opp_trigger.entry_state = EntryState.IDLE
			opp_trigger.requires_variation = False
			opp_trigger.is_re_entry = False
			opp_trigger.is_stop_point = False
			opp_trigger.resetBaseline()
			opp_trigger.resetReverseLine()
			# Exit Opposite trades
			exit(trigger.direction, reverse=True)

			if trigger.direction == LONG:
				strategy.draw(
					'caretSquareUpSolid', 'arrows', product.GBPUSD,
					chart.bids.ONE_MINUTE[0, 1] + utils.convertToPrice(2.0), 
					chart.timestamps.ONE_MINUTE[0],
					color='#3498db', scale=7.0, rotation=0
				)
			else:
				strategy.draw(
					'caretSquareUpSolid', 'arrows', product.GBPUSD,
					chart.bids.ONE_MINUTE[0, 2] - utils.convertToPrice(2.0), 
					chart.timestamps.ONE_MINUTE[0],
					color='#f1c40f', scale=7.0, rotation=180
				)


def entrySetup(chart, trigger):
	
	if trigger.entry_state != EntryState.IDLE:

		# Set Stop Line
		if (trigger.entry_state.value >= EntryState.TWO.value):
			trigger.setPendingStopLine(getHL(chart, trigger.direction, reverse=True))

		# Re-Entry T Donch variation
		if trigger.is_re_entry:
			if isReEntryTDonchConf(chart, trigger.direction):
				trigger.is_re_entry = False

		# Main Entry
		if trigger.entry_state == EntryState.ONE:
			if goldCrossOneConf(chart, trigger.direction):
				if trigger.requires_variation:
					if goldCrossVariationConf(chart, trigger.direction):
						trigger.entry_state = EntryState.TWO
				else:
					trigger.entry_state = EntryState.TWO

		elif trigger.entry_state == EntryState.TWO:
			if goldCrossTwoConf(chart, trigger.direction):
				trigger.entry_state = EntryState.THREE
				return entrySetup(chart, trigger)

		elif trigger.entry_state == EntryState.THREE:
			if goldCrossCancelConf(chart, trigger.direction):
				trigger.entry_state = EntryState.ONE
				trigger.pending_stop_line = None
				return entrySetup(chart, trigger)

			else:
				opp_trigger = getTrigger(trigger.direction, reverse=True)

				# Check re entry case
				if trigger.is_re_entry:
					if not opp_trigger.reverse_line:
						trigger.entry_state = EntryState.ONE
						trigger.pending_stop_line = None
						return entrySetup(chart, trigger)

				# Reset re entry conditions
				trigger.is_re_entry = False
				trigger.is_rl_tagged = False
				trigger.requires_variation = False

				# Enter Position
				trigger.entry_state = EntryState.ONE

				global last_direction
				last_direction = trigger.direction

				if confirmation(chart, trigger):

					# Check which stop state is current
					ema_fast_val = chart.indicators.ema_fast.bids[0]
					sma_slow_val = chart.indicators.sma_slow.bids[0]
					if isTagged(ema_fast_val, sma_slow_val, trigger.direction):
						trigger.stop_line_state = StopLineState.ONE
					else:
						trigger.stop_line_state = StopLineState.TWO

					trigger.stop_line = trigger.pending_stop_line
					trigger.is_stop_point = False

					# Reset exit state
					global exit_state
					exit_state = ExitState.NONE

				trigger.pending_stop_line = None
				

def stopLineSetup(chart, trigger):

	if trigger.stop_line_state.value < StopLineState.ACTIVE_ONE.value:
		trigger.setStopLine(getHL(chart, trigger.direction, reverse=True))

	if trigger.stop_line_state == StopLineState.ONE:
		if stopLineOneConf(chart, trigger.direction):
			trigger.stop_line_state = StopLineState.ACTIVE_ONE
			return stopLineSetup(chart, trigger)

	elif trigger.stop_line_state == StopLineState.TWO:
		if stopLineTwoConf(chart, trigger.direction):
			trigger.stop_line_state = StopLineState.ACTIVE_ONE
			return stopLineSetup(chart, trigger)

	elif trigger.stop_line_state == StopLineState.ACTIVE_ONE:
		if stopLineActiveOneConf(chart, trigger):
			trigger.stop_line_state = StopLineState.ACTIVE_TWO
			return stopLineSetup(chart, trigger)

	elif trigger.stop_line_state == StopLineState.ACTIVE_TWO:
		if stopLineActiveTwoConf(chart, trigger):
			exit(trigger.direction, re_enter=True)
		else:
			trigger.stop_line_state = StopLineState.RESET

	elif trigger.stop_line_state == StopLineState.RESET:
		opp_trigger = getTrigger(trigger.direction, reverse=True)
		if opp_trigger.reverse_line is None:
			trigger.setStopLine(opp_trigger.t_rev_hl)
			trigger.stop_line_state = StopLineState.ACTIVE_ONE
			return stopLineSetup(chart, trigger)


def exitSetup(chart):
	global exit_state, time_state, is_exit_override
	direction = getCurrentPositionDirection()

	if exitOverrideOneConf(chart):
		is_exit_override = True
	if is_exit_override and exitOverrideTwoConf(chart):
		exit_state = ExitState.ACTIVE

	if exit_state == ExitState.ACTIVE:
		if goldCrossTwoConf(chart, direction, reverse=True):
			if (time_state == TimeState.TRADING or
					getSessionProfit() >= 0 or getPositionProfit() >= 0):
				exit_state = ExitState.COMPLETE
				time_state = TimeState.NO_NEW_ENTRIES
				exit(direction)
				# Draw session line
				drawSessionLine(chart)


def stopPoints(chart):
	if len(strategy.positions) > 0:
		for pos in strategy.positions:
			for i in range(len(STOP_LEVELS)-1,-1,-1):
				level = STOP_LEVELS[i]
				point = STOP_POINTS[i]

				sl_range = None
				if pos.direction == LONG:
					if pos.sl is not None:
						sl_range = utils.convertToPips(pos.entry_price - pos.sl)
					profit = utils.convertToPips(chart.bids.ONE_MINUTE[0, 1] - pos.entry_price)
				else:
					if pos.sl is not None:
						sl_range = utils.convertToPips(pos.sl - pos.entry_price)
					profit = utils.convertToPips(pos.entry_price - chart.bids.ONE_MINUTE[0, 2])

				if sl_range is None or sl_range > -point:
					if profit >= level:
						trigger = getTrigger(pos.direction)
						trigger.is_stop_point = True
						print(f'MODIFY: {-point}')
						pos.modify(sl_range=-point)


def onTime(timestamp, chart):
	global time_state, exit_state, session, bank, no_more_entries, is_exit_override
	# Get session times
	now = utils.convertTimezone(utils.convertTimestampToTime(timestamp), TZ)
	start_time, end_time = getSessionTimes(now)

	# Set time state
	if time_state == TimeState.WAIT:
		if now > end_time:
			if len(strategy.positions) > 0:
				time_state = TimeState.NO_NEW_ENTRIES

		elif now >= start_time:
			# Reset globals
			time_state = TimeState.TRADING
			session = []
			exit_state = ExitState.NONE
			no_more_entries = False
			is_exit_override = False
			long_trigger.resetReverseLine()
			long_trigger.resetBaseline()
			long_trigger.baseline_state = BaselineState.COMPLETE_A
			short_trigger.resetReverseLine()
			short_trigger.resetBaseline()
			short_trigger.baseline_state = BaselineState.COMPLETE_A

			bank = strategy.getBalance()

			if last_direction == LONG and long_trigger.entry_state != EntryState.IDLE:
				long_trigger.requires_variation = True
			if last_direction == SHORT and short_trigger.entry_state != EntryState.IDLE:
				short_trigger.requires_variation = True

			# Draw session line
			drawSessionLine(chart)

	elif time_state == TimeState.NO_NEW_ENTRIES:
		if len(strategy.positions) == 0 and now > end_time:
			time_state = TimeState.WAIT

	elif time_state == TimeState.TRADING:
		if now > end_time:
			if len(strategy.positions) > 0:
				time_state = TimeState.NO_NEW_ENTRIES
				exit_state = ExitState.ACTIVE
			else:
				time_state = TimeState.WAIT
				# Draw session line
				drawSessionLine(chart)

		elif exit_state == ExitState.NONE:
			# Exit FOUR
			if now > getExitTime(now, EXIT_FOUR):
				exit_state = ExitState.ACTIVE
				time_state = TimeState.NO_NEW_ENTRIES	
			# Exit THREE
			elif now > getExitTime(now, EXIT_THREE):
				if exitThreeConf(chart):
					exit_state = ExitState.ACTIVE
			# Exit TWO
			elif now > getExitTime(now, EXIT_TWO):
				if exitTwoConf(chart):
					exit_state = ExitState.ACTIVE
			# Exit ONE
			elif now < getExitTime(now, EXIT_ONE):
				if exitOneConf(chart):
					exit_state = ExitState.ACTIVE


def onEventLoop(timestamp, chart):
	# Check Time
	onTime(timestamp, chart)

	# Baseline
	baselineSetup(chart, long_trigger)
	baselineSetup(chart, short_trigger)
	
	# Reverse Line
	reverseLineSetup(chart, long_trigger)
	reverseLineSetup(chart, short_trigger)

	# T Reverse Setup
	tRevSetup(chart, long_trigger)
	tRevSetup(chart, short_trigger)

	# Entry Setup
	entrySetup(chart, long_trigger)
	entrySetup(chart, short_trigger)

	# Stop Line Setup
	stopLineSetup(chart, long_trigger)
	stopLineSetup(chart, short_trigger)

	# Exit Setup
	exitSetup(chart)


'''
GUI
'''

def drawSessionLine(chart):
	strategy.draw(
		'verticalLine', 'arrows', product.GBPUSD,
		None, chart.timestamps.ONE_MINUTE[0],
		color='#000', scale=2.0
	)


'''
Setup
'''

def setInputs():
	global MAXIMUM_BANK, EXTERNAL_BANK, RISK, STOP_RANGE, LIMIT_RANGE
	strategy.setInputVariable('Risk Management', HEADER)
	MAXIMUM_BANK = strategy.setInputVariable('Maximum Bank', float, default=30000)
	EXTERNAL_BANK = strategy.setInputVariable('External Bank', float, default=0)
	RISK = strategy.setInputVariable('Risk', PERCENTAGE, default=0.5, properties={'min': 0.0, 'max': 1.0})
	STOP_RANGE = strategy.setInputVariable('Stop Range', float, default=12.0)
	LIMIT_RANGE = strategy.setInputVariable('Take Profit', float, default=84.0)

	global DONCH_PERIOD, SMA_SLOW_PERIOD, SMA_FAST_PERIOD, EMA_FAST_PERIOD
	strategy.setInputVariable('Indicators', HEADER)
	DONCH_PERIOD = strategy.setInputVariable('Donch', int, default=30)
	SMA_SLOW_PERIOD = strategy.setInputVariable('Sma Slow', int, default=15)
	SMA_FAST_PERIOD = strategy.setInputVariable('Sma Fast', int, default=5)
	EMA_FAST_PERIOD = strategy.setInputVariable('Ema Fast', int, default=3)

	global A_ONE, A_TWO, A_THREE, A_FOUR
	strategy.setInputVariable('Reverse Line', HEADER)
	A_ONE = strategy.setInputVariable('a) 1.', int, default=3)
	A_TWO = strategy.setInputVariable('a) 2.', int, default=1)
	A_THREE = strategy.setInputVariable('a) 3.', float, default=1.0)
	A_FOUR = strategy.setInputVariable('a) 4.', int, default=1)

	global B_ONE, B_TWO, B_THREE, B_FOUR, B_FIVE, B_SIX
	strategy.setInputVariable('T Reverse', HEADER)
	B_ONE = strategy.setInputVariable('b) 1.', float, default=0.1)
	B_TWO = strategy.setInputVariable('b) 2.', float, default=0.2)
	B_THREE = strategy.setInputVariable('b) 3.', float, default=0.2)
	B_FOUR = strategy.setInputVariable('b) 4.', int, default=1)
	B_FIVE = strategy.setInputVariable('b) 5.', float, default=0.2)
	B_SIX = strategy.setInputVariable('b) 6.', float, default=0.2)

	global C_ONE, C_TWO
	strategy.setInputVariable('Golden X', HEADER)
	C_ONE = strategy.setInputVariable('c) 1.', float, default=0.2)
	C_TWO = strategy.setInputVariable('c) 2.', float, default=2.0)

	global D_ONE, D_TWO, D_THREE, D_FOUR, D_FIVE
	strategy.setInputVariable('Exit', HEADER)
	D_ONE = strategy.setInputVariable('d) 1.', float, default=0.2)
	D_TWO = strategy.setInputVariable('d) 2.', float, default=0.2)
	D_THREE = strategy.setInputVariable('d) 3.', float, default=3.0)
	D_FOUR = strategy.setInputVariable('d) 4.', float, default=24.0)
	D_FIVE = strategy.setInputVariable('d) 5.', float, default=5.0)
	D_SIX = strategy.setInputVariable('d) 6.', float, default=24.0)
	D_SEVEN = strategy.setInputVariable('d) 7.', float, default=0.0)
	D_EIGHT = strategy.setInputVariable('d) 8.', float, default=48.0)
	D_NINE = strategy.setInputVariable('d) 9.', float, default=24.0)
	D_TEN = strategy.setInputVariable('d) 10.', float, default=60.0)
	D_ELEVEN = strategy.setInputVariable('d) 11.', float, default=36.0)
	D_TWELVE = strategy.setInputVariable('d) 12.', float, default=72.0)
	D_THIRTEEN = strategy.setInputVariable('d) 13.', float, default=48.0)
	D_FOURTEEN = strategy.setInputVariable('d) 14.', float, default=1.0)

	global STOP_POINTS, STOP_LEVELS
	STOP_POINTS = [D_SEVEN, D_NINE, D_ELEVEN, D_THIRTEEN]
	STOP_LEVELS = [D_SIX, D_EIGHT, D_TEN, D_TWELVE]

	global E_ONE, E_TWO, E_THREE, E_FOUR
	strategy.setInputVariable('Time Exits', HEADER)
	E_ONE = strategy.setInputVariable('e) 1.', float, default=60.0)
	E_TWO = strategy.setInputVariable('e) 2.', float, default=48.0)
	E_THREE = strategy.setInputVariable('e) 3.', float, default=24.0)
	E_FOUR = strategy.setInputVariable('e) 4.', PERCENTAGE, default=1.0)


def setGlobals():
	'''Set global variables'''
	global long_trigger, short_trigger, last_direction
	long_trigger = Trigger(LONG)
	short_trigger = Trigger(SHORT)
	last_direction = None

	global session
	session = []

	global time_state, exit_state
	time_state = TimeState.WAIT
	exit_state = ExitState.NONE

	global is_exit_override
	is_exit_override = False

	global bank
	bank = 0


def report(tick):
	log = ''

	utc_time = utils.convertTimestampToTime(tick.timestamp)
	aus_time = utils.convertTimezone(utc_time, AUS_TZ)
	london_time = utils.convertTimezone(utc_time, TZ)

	log += f'\nAus: {aus_time.strftime("%d %b %H:%M:%S")}\n'
	log += f'London: {london_time.strftime("%d %b %H:%M:%S")}\n'

	log += f'Time State: {time_state}, Exit State: {exit_state}, Last Direction: {last_direction}\n\n'

	log += f'OHLC: {tick.chart.bids.ONE_MINUTE[0]}\n'
	log += f'DONCH: {tick.chart.indicators.donch.bids[0]}, '\
		   f'SMA 15 {tick.chart.indicators.sma_slow.bids[0]}, '\
		   f'SMA 5 {tick.chart.indicators.sma_fast.bids[0]}, '\
		   f'EMA 3 {tick.chart.indicators.ema_fast.bids[0]}\n'

	log += f'L: {long_trigger}\n\n'
	log += f'S: {short_trigger}\n\n'

	for pos in session:
		if pos.direction == LONG:
			sp = utils.convertToPips(pos.entry_price - pos.sl)
		else:
			sp = utils.convertToPips(pos.sl - pos.entry_price)

		if pos.close_price is None:
			log += (
				f'(O) D: {pos.direction.upper()} E: {pos.entry_price} SL: {pos.sl} TP: {pos.tp} '
				f'SP: {sp} P: {pos.getProfit()} -> {pos.account_id}\n'
			)
		else:
			log += (
				f'(C) D: {pos.direction.upper()} E: {pos.entry_price} SL: {pos.sl} TP: {pos.tp} '
				f'SP: {sp} P: {pos.getProfit()} -> {pos.account_id}\n'
			)

	log += f'POS: {strategy.positions}\n'
	print(log)

'''
Hook functions
'''

def init():
	'''Initialization on script startup'''

	# print(strategy.getBalance())

	# Set Input Variables 
	setInputs()

	# Charts
	chart = strategy.getChart(product.GBPUSD, period.ONE_MINUTE)

	# Indicators
	donch = indicator.DONCH(DONCH_PERIOD)
	sma_slow = indicator.SMA(SMA_SLOW_PERIOD)
	sma_fast = indicator.SMA(SMA_FAST_PERIOD)
	ema_fast = indicator.EMA(EMA_FAST_PERIOD)

	chart.addIndicator('donch', period.ONE_MINUTE, donch)
	chart.addIndicator('sma_slow', period.ONE_MINUTE, sma_slow)
	chart.addIndicator('sma_fast', period.ONE_MINUTE, sma_fast)
	chart.addIndicator('ema_fast', period.ONE_MINUTE, ema_fast)

	# Start From
	strategy.startFrom(datetime.utcnow() - timedelta(hours=12))

	# Set Global Vars
	setGlobals()


def onStart():
	# Clear any backtest positions if real positions exist
	if any([not pos.isBacktest() for pos in strategy.positions]):
		strategy.clearBacktestTrades()


def onTrade(trade):
	'''Hook function for broker trade events'''

	# On Stop Loss
	if trade.type == STOP_LOSS:
		if trade.item.direction == LONG:
			long_trigger.requires_variation = True
			if long_trigger.is_stop_point:
				long_trigger.is_re_entry = True
		else:
			short_trigger.requires_variation = True
			if short_trigger.is_stop_point:
				short_trigger.is_re_entry = True

	# On Take Profit
	elif trade.type == TAKE_PROFIT:
		global time_state
		if time_state == TimeState.TRADING:
			time_state = TimeState.NO_NEW_ENTRIES
			# Draw session line
			drawSessionLine(chart)



def onTick(tick):
	'''Hook function for broker price events'''
	# On Bar End
	if tick.bar_end:
		onEventLoop(tick.timestamp, tick.chart)
		report(tick)

	# Stop Points
	stopPoints(tick.chart)

'''
Constants
'''

TZ = 'Europe/London'
AUS_TZ = 'Australia/Melbourne'
START_TIME = [6, 0] # Hour, Minute
END_TIME = [19, 30] # Hour, Minute
EXIT_ONE = [15, 0] # Hour, Minute
EXIT_TWO = [15, 0] # Hour, Minute
EXIT_THREE = [17, 30] # Hour, Minute
EXIT_FOUR = [19, 30] # Hour, Minute

'''
Structures
'''

class Trigger(dict):

	def __init__(self, direction):
		self.direction = direction

		self.baseline = None
		self.baseline_state = BaselineState.WAIT
		self.start_baseline = None
		self.preceding_baseline = None
		self.current_baseline = None
		self.current_baseline_count = 0
		self.reverse_line = None

		self.t_rev_state = TRevState.ONE
		self.t_rev_flat = None
		self.t_rev_hl = None

		self.entry_state = EntryState.IDLE
		self.requires_variation = False

		self.pending_stop_line = None
		self.stop_line = None
		self.stop_line_state = StopLineState.NONE
		self.is_stop_point = False
		self.is_re_entry = False

	def __getattr__(self, key):
		return self[key]

	def __setattr__(self, key, value):
		self[key] = value

	def jsonable(self):
		json_safe = {
			'baseline_state': str(self.baseline_state),
			't_rev_state': str(self.t_rev_state),
			'entry_state': str(self.entry_state),
			'stop_line_state': str(self.stop_line_state),
		}
		return {**self, **json_safe}

	def isValidBaseline(self, x):
		c_direction = getCurrentPositionDirection()
		if self.direction == LONG:
			return c_direction == self.direction or self.baseline is None or x < self.baseline
		else:
			return c_direction == self.direction or self.baseline is None or x > self.baseline

	def setBaseline(self, x):
		self.baseline = x

	def resetBaseline(self):
		self.baseline = self.start_baseline
		self.preceding_baseline = None

	def setReverseLine(self, x):
		if self.direction == LONG:
			if self.reverse_line is None or x < self.reverse_line:
				self.reverse_line = x
		else:
			if self.reverse_line is None or x > self.reverse_line:
				self.reverse_line = x

	def resetReverseLine(self):
		self.reverse_line = None
		self.t_rev_flat = None
		self.t_rev_state = TRevState.ONE

	def setPendingStopLine(self, x):
		if self.direction == LONG:
			if self.pending_stop_line is None or x < self.pending_stop_line:
				self.pending_stop_line = x
				return True
		else:
			if self.pending_stop_line is None or x > self.pending_stop_line:
				self.pending_stop_line = x
				return True

	def setStopLine(self, x):
		if self.direction == LONG:
			if self.stop_line is None or x < self.stop_line:
				self.stop_line = x
				return True
		else:
			if self.stop_line is None or x > self.stop_line:
				self.stop_line = x
				return True

	def resetStopLine(self):
		self.pending_stop_line = None
		self.stop_line_state = StopLineState.NONE

	def setTRevHL(self, x):
		if self.direction == LONG:
			if x > self.t_rev_hl:
				self.t_rev_hl = x
		else:
			if x < self.t_rev_hl:
				self.t_rev_hl = x


class BaselineState(Enum):
	WAIT = 1
	COMPLETE_A = 2
	COMPLETE_B = 3


class TRevState(Enum):
	ONE = 1
	TWO = 2
	THREE = 3
	COMPLETE = 4


class EntryState(Enum):
	IDLE = 1
	ONE = 2
	TWO = 3
	THREE = 4
	COMPLETE = 5


class StopLineState(Enum):
	NONE = 1
	ONE = 2
	TWO = 3
	ACTIVE_ONE = 4
	ACTIVE_TWO = 5


class ExitState(Enum):
	NONE = 1
	ACTIVE = 2
	COMPLETE = 3


class TimeState(Enum):
	WAIT = 1
	TRADING = 2
	NO_NEW_ENTRIES = 3


