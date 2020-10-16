import pendulum
import math
from datetime import datetime, timedelta

'''
Utilities
'''

TS_START_DATE = datetime(year=2000, month=1, day=1)

def convertToPips(x):
	return round(x * 10000, 1)

def convertToPrice(x):
	return round(x / 10000, 5)

def convertTimezone(dt, tz):
	return dt.astimezone(pendulum.timezone(tz))

def setTimezone(dt, tz):
	return pendulum.timezone(tz).convert(dt)

def isOffsetAware(dt):
	if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
		return True
	else:
		return False

def convertTimeToTimestamp(dt):
	if isOffsetAware(dt):
		dt = convertTimezone(dt, 'UTC')
	else:
		dt = setTimezone(dt, 'UTC')
	return datetime.timestamp(dt)

def convertTimestampToTime(ts):
	return setTimezone(datetime.utcfromtimestamp(ts), 'UTC')

def isWeekend(dt):
	FRI = 4
	SAT = 5
	SUN = 6
	return (
		(dt.weekday() == FRI and dt.hour >= 17) or
		dt.weekday() == SAT or
		(dt.weekday() == SUN and dt.hour < 17)
	)

def getWeekendDate(dt):
	FRI = 4
	SUN = 6
	if dt.weekday() == SUN and dt.hour >= 17:
		dt += timedelta(days=5)
	else:
		dt += timedelta(days=FRI-dt.weekday())

	return dt.replace(dt.year,dt.month,dt.day,17,0,0,0)

def getWeekstartDate(dt):
	SUN = 6
	if dt.weekday() == SUN and dt.hour >= 17:
		dt += timedelta(days=7)
	else:
		dt += timedelta(days=SUN-dt.weekday())

	return dt.replace(dt.year,dt.month,dt.day,17,0,0,0)

def getWeekendSecondsOffset(start, end):
	ONE_MINUTE = 60.0
	# Get weekend seconds offset
	return sum(
		ONE_MINUTE for x in range(int((end-start).total_seconds()/ONE_MINUTE)) 
		if isWeekend(start + timedelta(seconds=x*ONE_MINUTE))
	)

def getWeeklySecondsOffset(start, end):
	ONE_MINUTE = 60.0
	# Get weekend seconds offset
	return sum(
		ONE_MINUTE for x in range(int((end-start).total_seconds()/ONE_MINUTE)) 
		if not isWeekend(start + timedelta(seconds=x*ONE_MINUTE))
	)

def getCountDate(period, count, start=None, end=None):
		off = tl.period.getPeriodOffsetSeconds(period)

		if start:
			date = start
			direction = 1
		elif end:
			date = end
			direction = -1
		else:
			date = datetime.utcnow()
			direction = -1

		i = x = 0
		while x < count:
			if not isWeekend(date + timedelta(seconds=off*i*direction)):
				x += 1
			i += 1

		return date + timedelta(seconds=off*i*direction)

def getDateCount(period, start, end):
		off = tl.period.getPeriodOffsetSeconds(period)

		week_off = getWeeklySecondsOffset(start, end)
		return math.floor(week_off / off)

def isCurrentBar(period, ts, off=1):
	# `off` = 1, for current incomplete bar check
	# `off` = 2, for current complete bar check
	now_time = datetime.utcnow()
	if tl.utils.isWeekend(now_time):
		now_time = tl.utils.getWeekendDate(now_time)
	now_ts = tl.utils.convertTimeToTimestamp(now_time)
	return ts > now_ts - tl.period.getPeriodOffsetSeconds(period) * off


'''
Imports
'''
from app import pythonsdk as tl


