from datetime import datetime, timedelta

def init():
	chart = strategy.getChart(product.GBPUSD, period.ONE_MINUTE)

	sma = indicator.SMA(1)
	chart.addIndicator('sma20', period.ONE_MINUTE, sma)

	global count
	count = 0
	# strategy.startFrom(datetime.now() - timedelta(days=1))

def ontrade(trade):
	return


def ontick(tick):
	global count

	print('TICK')
	if tick.bar_end:
		print('END')
		if count < 1:
			strategy.sell(product.GBPUSD, 1.0)
			strategy.draw(
				'arrowAltCircleUpRegular', '0', product.GBPUSD,
				tick.chart.bids.ONE_MINUTE[0, 3], 
				tick.chart.timestamps.ONE_MINUTE[0],
				color='#3498db', scale=2.0, rotation=0
			)
			print('Hello this is LOG < 10')
			strategy.info('< 10', True)
		elif count < 2:
			# pos = strategy.positions[count-10]
			for pos in strategy.positions:
				pos.modify(sl_range=100, tp_range=20)
			strategy.draw(
				'arrowAltCircleUpRegular', '0', product.GBPUSD,
				tick.chart.bids.ONE_MINUTE[0, 3], 
				tick.chart.timestamps.ONE_MINUTE[0],
				color='#2ecc71', scale=2.5, rotation=90
			)
			print('Hello this is LOG < 20')
			strategy.info('< 20', True)
		elif count < 3:
			for pos in strategy.positions:
				pos.close()
			strategy.draw(
				'arrowAltCircleUpRegular', '0', product.GBPUSD,
				tick.chart.bids.ONE_MINUTE[0, 3], 
				tick.chart.timestamps.ONE_MINUTE[0],
				color='#e74c3c', scale=1.5, rotation=180
			)
			print('Hello this is LOG < 30')
			strategy.info('< 30', True)

		count += 1
