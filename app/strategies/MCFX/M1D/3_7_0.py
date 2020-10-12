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
			vals = chart.indicators.donch30.bids[offset:length+1+offset, 1]
			return all([i == vals[0] for i in vals])
		else:
			vals = chart.indicators.donch30.bids[offset:length+1+offset, 0]
			return all([i == vals[0] for i in vals])
	else:
		if direction == LONG:
			vals = chart.indicators.donch30.bids[offset:length+1+offset, 0]
			return all([i == vals[0] for i in vals])
		else:
			vals = chart.indicators.donch30.bids[offset:length+1+offset, 1]
			return all([i == vals[0] for i in vals])


def isDonchTag(chart, direction, reverse=False):
	if reverse:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0,2] <= chart.indicators.donch30.bids[0,1]
		else:
			return chart.bids.ONE_MINUTE[0,1] >= chart.indicators.donch30.bids[0,0]
	else:
		if direction == LONG:
			return chart.bids.ONE_MINUTE[0,1] >= chart.indicators.donch30.bids[0,0]
		else:
			return chart.bids.ONE_MINUTE[0,2] <= chart.indicators.donch30.bids[0,1]


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
	return profit / SL_RANGE * RISK <= -(RISK * LOSS_LIMIT)


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
			return chart.indicators.donch30.bids[offset, 1]
		else:
			return chart.indicators.donch30.bids[offset, 0]
	else:
		if direction == LONG:
			return chart.indicators.donch30.bids[offset, 0]
		else:
			return chart.indicators.donch30.bids[offset, 1]


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


'''
Confirmations
'''

# T Reverse Confirmations
def tRevFlatConf(chart, trigger):
	donch_val = getDonchValue(chart, trigger.direction)
	return (
		isCrossed(donch_val, trigger.reverse_line, trigger.direction) and
		isDonchFlat(chart, trigger.direction, 1)
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
		isCrossed(donch_val, trigger.reverse_line, trigger.direction)
	)


def tRevTwoConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isCrossed(close, trigger.reverse_line, trigger.direction) and
		(trigger.t_rev_flat is None or isCrossed(close, trigger.t_rev_flat, trigger.direction)) and
		isBB(chart, trigger.direction) and not isDoji(chart, 0.2) and
		isMinDist(trigger.reverse_line, close, RL_CLOSE_MIN_DIST)
	)


def tRevThreeConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]
	donch_val = getDonchValue(chart, trigger.direction)
	return (
		isCrossed(close, trigger.reverse_line, trigger.direction) and
		isMinDist(close, trigger.reverse_line, RL_CLOSE_MIN_DIST) and
		isBB(chart, trigger.direction) and not isDoji(chart, 0.2)
	)


def tRevFourConf(chart, trigger):
	hl = getHL(chart, trigger.direction)
	
	return (
		isCrossed(hl, trigger.t_rev_hl, trigger.direction)
	)


# Entry Confirmations
def goldCrossVariationConf(chart, direction):
	ema3_val = chart.indicators.ema3.bids[0]
	sma5_val = chart.indicators.sma5.bids[0]
	sma15_val = chart.indicators.sma15.bids[0]

	return (
		isCrossed(ema3_val, sma15_val, direction, reverse=True) and
		isCrossed(sma5_val, sma15_val, direction, reverse=True)
	)


def goldCrossOneConf(chart, direction, reverse=False):
	ema3_val = chart.indicators.ema3.bids[0]
	sma5_val = chart.indicators.sma5.bids[0]
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isCrossed(ema3_val, sma5_val, direction, reverse=not reverse) and
		isCrossed(close, sma5_val, direction, reverse=not reverse)
	)


def goldCrossTwoConf(chart, direction, reverse=False):
	ema3_val = chart.indicators.ema3.bids[0]
	sma5_val = chart.indicators.sma5.bids[0]
	donch_val = getDonchValue(chart, direction)
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isTagged(ema3_val, sma5_val, direction, reverse=reverse) and
		not isBB(chart, direction, reverse=not reverse) and
		isCrossed(close, sma5_val, direction, reverse=reverse) and
		isCrossed(close, ema3_val, direction, reverse=reverse)
	)


def goldCrossCancelConf(chart, direction, reverse=False):
	donch_val = getDonchValue(chart, direction)
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isDonchTag(chart, direction, reverse=reverse) or
		not isMinDist(close, donch_val, CLOSE_DONCH_MIN_DIST)
	)


# Stop Line Confirmations
def stopLineOneConf(chart, direction):
	ema3_val = chart.indicators.ema3.bids[0]
	sma15_val = chart.indicators.sma15.bids[0]

	return isCrossed(ema3_val, sma15_val, direction, reverse=True)


def stopLineTwoConf(chart, direction):
	sma5_val = chart.indicators.sma5.bids[0]
	sma15_val = chart.indicators.sma15.bids[0]

	return isCrossed(sma5_val, sma15_val, direction)


def stopLineActiveOneConf(chart, trigger):
	close = chart.bids.ONE_MINUTE[0, 3]

	return (
		isCrossed(close, trigger.stop_line, trigger.direction, reverse=True) and
		isBB(chart, trigger.direction, reverse=True) and not isDoji(chart, 0.2) and
		isMinDist(close, trigger.stop_line, CLOSE_SL_MIN_DIST)
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
	return getSessionPotentialProfit(chart) >= EXIT_POINTS_PIPS[0]


def exitTwoConf(chart):
	profit = getSessionPotentialProfit(chart)
	return (
		profit >= EXIT_POINTS_PIPS[1]
	)


def exitThreeConf(chart):
	profit = getSessionPotentialProfit(chart)
	return (
		profit >= EXIT_POINTS_PIPS[2] or
		profit / SL_RANGE * RISK >= EXIT_POINTS_PERC[2]
	)


'''
Events
'''

def confirmation(chart, trigger, reverse=False):
	'''On entry confirmation, enter position'''
	global time_state

	if isSessionLoss():
		time_state = TimeState.NO_NEW_ENTRIES

	if time_state == TimeState.TRADING:
		if reverse:
			direction = getOppDirection(trigger.direction)
		else:
			direction = trigger.direction

		if len(strategy.positions) == 0:
			if direction == LONG:
				result = strategy.buy(
					product.GBPUSD, 1.0, sl_range=SL_RANGE, tp_range=TP_RANGE
				)

				strategy.draw(
					'arrowAltCircleUpRegular', 'arrows', product.GBPUSD,
					chart.bids.ONE_MINUTE[0, 1] + utils.convertToPrice(2.0), 
					chart.timestamps.ONE_MINUTE[0],
					color='#3498db', scale=7.0, rotation=0
				)
			else:
				result = strategy.sell(
					product.GBPUSD, 1.0, sl_range=SL_RANGE, tp_range=TP_RANGE
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
		if trigger.current_baseline_count == 3:
			trigger.preceding_baseline = value

		if trigger.baseline_state == BaselineState.COMPLETE_A:
			if (
				trigger.current_baseline_count in (2,3) and
				isCrossed(trigger.current_baseline, trigger.baseline, 
							trigger.direction, reverse=True)
			):
				trigger.baseline_state = BaselineState.COMPLETE_B

		trigger.current_baseline = value
		trigger.current_baseline_count = 0

	if isDonchFlat(chart, trigger.direction, BL_MIN_FLAT):
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
	if isDonchFlat(chart, trigger.direction, BL_MIN_FLAT):
		trigger.start_baseline = value



def reverseLineSetup(chart, trigger):

	if trigger.baseline is not None and trigger.baseline_state != BaselineState.WAIT:
		# Check donch is A/B current baseline
		value = getDonchValue(chart, trigger.direction)

		# Reverse Line (b) (i)
		if isCrossed(value, trigger.baseline, trigger.direction):
			if isDonchFlat(chart, trigger.direction, RL_MIN_FLAT):

				if (trigger.baseline_state == BaselineState.COMPLETE_B or 
						isMinDist(trigger.baseline, value, BL_RL_MIN_DIST)):
					trigger.setReverseLine(value)
					trigger.baseline_state = BaselineState.WAIT


def tRevSetup(chart, trigger):
	
	if trigger.reverse_line is not None:

		if tRevFlatConf(chart, trigger):
			trigger.t_rev_flat = getDonchValue(chart, trigger.direction)

		if tRevFlatCancelConf(chart, trigger):
			trigger.t_rev_flat = None


		# Cancel on T Donch tick below RL
		if (trigger.t_rev_state.value >= TRevState.TWO.value and
				trigger.t_rev_state != TRevState.COMPLETE):
			if tRevRLCrossCancelConf(chart, trigger):
				trigger.resetReverseLine()
				trigger.resetBaseline()
				trigger.baseline_state = BaselineState.COMPLETE_A
				return

		# Cancel on CT Donch tick
		if tRevCtDonchCancelConf(chart, trigger):
			trigger.resetReverseLine()
			trigger.resetBaseline()
			trigger.baseline_state = BaselineState.COMPLETE_A
			return


		if trigger.t_rev_state == TRevState.ONE:
			if tRevOneConf(chart, trigger):
				trigger.t_rev_state = TRevState.TWO
				return tRevSetup(chart, trigger)

		elif trigger.t_rev_state == TRevState.TWO:
			if tRevTwoConf(chart, trigger):
				trigger.t_rev_state = TRevState.THREE
				trigger.t_rev_hl = getHL(chart, trigger.direction)

		elif trigger.t_rev_state == TRevState.THREE:
			if tRevThreeConf(chart, trigger):
				trigger.t_rev_close = True
			if tRevFourConf(chart, trigger):
				trigger.t_rev_hl_tag = True

			if trigger.t_rev_hl_tag and trigger.t_rev_close:
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

		if (trigger.entry_state.value >= EntryState.TWO.value):
			trigger.setPendingStopLine(getHL(chart, trigger.direction, reverse=True))

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
				# Check variation

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
					ema3_val = chart.indicators.ema3.bids[0]
					sma15_val = chart.indicators.sma15.bids[0]
					if isTagged(ema3_val, sma15_val, trigger.direction):
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
			trigger.resetStopLine()


def exitSetup(chart):
	global exit_state, time_state
	direction = getCurrentPositionDirection()

	if exit_state == ExitState.ACTIVE:
		if goldCrossTwoConf(chart, direction, reverse=True):
			if (time_state == TimeState.TRADING or
					getSessionProfit() >= 0 or getPositionProfit() >= 0):
				exit_state = ExitState.COMPLETE
				time_state = TimeState.NO_NEW_ENTRIES
				exit(direction)


def stopPoints(chart):
	if len(strategy.positions) > 0:

		for pos in strategy.positions:
			for i in range(len(STOP_LEVELS)-1,-1,-1):
				level = STOP_LEVELS[i]
				point = STOP_POINTS[i]

				if pos.direction == LONG:
					sl_range = utils.convertToPips(pos.entry_price - pos.sl)
					profit = utils.convertToPips(chart.bids.ONE_MINUTE[0, 1] - pos.entry_price)
				else:
					sl_range = utils.convertToPips(pos.sl - pos.entry_price)
					profit = utils.convertToPips(pos.entry_price - chart.bids.ONE_MINUTE[0, 2])

				if sl_range > -point:
					if profit >= level:
						trigger = getTrigger(pos.direction)
						trigger.is_stop_point = True
						pos.modify(sl_range=-point)


def onTime(timestamp, chart):
	global time_state, exit_state, session, no_more_entries
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
			long_trigger.resetReverseLine()
			long_trigger.resetBaseline()
			long_trigger.baseline_state = BaselineState.COMPLETE_A
			short_trigger.resetReverseLine()
			short_trigger.resetBaseline()
			short_trigger.baseline_state = BaselineState.COMPLETE_A

			if last_direction == LONG and long_trigger.entry_state != EntryState.IDLE:
				long_trigger.requires_variation = True
			if last_direction == SHORT and short_trigger.entry_state != EntryState.IDLE:
				short_trigger.requires_variation = True

			strategy.draw(
				'verticalLine', 'arrows', product.GBPUSD,
				None, chart.timestamps.ONE_MINUTE[0],
				color='#000', scale=2.0
			)


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
Setup
'''

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


def report(tick):
	log = ''

	utc_time = utils.convertTimestampToTime(tick.timestamp)
	aus_time = utils.convertTimezone(utc_time, AUS_TZ)
	london_time = utils.convertTimezone(utc_time, TZ)

	log += f'\nAus: {aus_time.strftime("%d %b %H:%M:%S")}\n'
	log += f'London: {london_time.strftime("%d %b %H:%M:%S")}\n'

	log += f'Time State: {time_state}, Exit State: {exit_state}, Last Direction: {last_direction}\n\n'

	log += f'OHLC: {tick.chart.bids.ONE_MINUTE[0]}\n'
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
				f'SP: {sp} P: {pos.getProfit()}\n'
			)
		else:
			log += (
				f'(C) D: {pos.direction.upper()} E: {pos.entry_price} SL: {pos.sl} TP: {pos.tp} '
				f'SP: {sp} P: {pos.getProfit()}\n'
			)

	print(log)

'''
Hook functions
'''

def init():
	'''Initialization on script startup'''
	# Charts
	chart = strategy.getChart(product.GBPUSD, period.ONE_MINUTE)

	# Indicators
	donch30 = indicator.DONCH(30)
	ema3 = indicator.EMA(3)
	sma5 = indicator.SMA(5)
	sma15 = indicator.SMA(15)

	chart.addIndicator('donch30', period.ONE_MINUTE, donch30)
	chart.addIndicator('ema3', period.ONE_MINUTE, ema3)
	chart.addIndicator('sma5', period.ONE_MINUTE, sma5)
	chart.addIndicator('sma15', period.ONE_MINUTE, sma15)

	# Start From
	strategy.startFrom(datetime.utcnow() - timedelta(hours=12))

	# Set Global Vars
	setGlobals()


def ontrade(trade):
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


def ontick(tick):
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

BL_MIN_FLAT = 5 # periods
RL_MIN_FLAT = 2 # periods
BL_RL_MIN_DIST = 0.5 # pips
RL_CLOSE_MIN_DIST = 0.2 # pips
CLOSE_DONCH_MIN_DIST = 2.0 # pips
CLOSE_SL_MIN_DIST = 0.2 # pips

STOP_POINTS = [0, 24, 36, 48, 72]
STOP_LEVELS = [24, 48, 60, 72, 84]
EXIT_POINTS_PIPS = [60, 48, 24]
EXIT_POINTS_PERC = [None, 2.0, 1.0]

SL_RANGE = 12 # pips
TP_RANGE = 96 # pips

RISK = 0.5
LOSS_LIMIT = 5 # R

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
		self.t_rev_hl_tag = False
		self.t_rev_close = False

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
		self.t_rev_hl = None
		self.t_rev_flat = None
		self.t_rev_state = TRevState.ONE
		self.t_rev_close = False
		self.t_rev_hl_tag = False

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


