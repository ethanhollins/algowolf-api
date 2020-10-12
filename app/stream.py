import functools
from flask import request
from flask_socketio import (
	ConnectionRefusedError, 
	send, emit, join_room, leave_room
)
from v1 import key_or_login_required, AccessLevel


def create_stream(sio):

	@sio.on('subscribe', namespace='/user')
	def subscribe(data):
		print('SUBSCRIBE')
		strategy_id = data.get('strategy_id')
		field = data.get('field')
		items = data.get('items')

		print(data)

		# Validation
		if strategy_id is None:
			raise ConnectionRefusedError('`strategy_id` not found.')
		elif field is None:
			raise ConnectionRefusedError('`field` not found.')

		# Authenticate user
		# res, status = key_or_login_required(strategy_id, AccessLevel.LIMITED, disable_abort=True)
		# if status != 200:
		# 	raise ConnectionRefusedError(res['message'])
		res = '9sXFsHDK7oAEFaaDaZy8XL'
		print('GET ACCOUNT')
		account = ctrl.accounts.getAccount(res)
		print('GET BROKER')
		strategy = account.getStrategyBroker(strategy_id)
		print('DONE')

		if field == 'ontrade':
			# with ctrl.app.app_context():
			join_room(strategy_id)

		elif field == 'ontick':
			# `ontick` Validation
			if items is None:
				raise ConnectionRefusedError('`items` not found.')

			if isinstance(items, list):
				print(1)
				for i in range(len(items)):
					c_i = items[i]
					print(c_i)
					if isinstance(c_i, dict):
						print(2)
						# Item Validation
						if (c_i.get('broker') is None or 
							c_i.get('product') is None or
							c_i.get('period') is None):
							raise ConnectionRefusedError(f'Insufficient item data ({i}).')

						print(3)
						strategy.getChart(c_i.get('product'))
						print('CHART DONE')
						print(f"JOIN: {c_i.get('broker')}:{c_i.get('product')}:{c_i.get('period')}")
						# with ctrl.app.app_context():
						join_room(
							f"{c_i.get('broker')}:{c_i.get('product')}:{c_i.get('period')}"
						)
					else:
						raise ConnectionRefusedError(f'Item object must be dict ({i}).')

			else:
				raise ConnectionRefusedError(f'`items` object must be list.')


	@sio.on('unsubscribe', namespace='/user')
	def unsubscribe(data):
		strategy_id = data.get('strategy_id')
		field = data.get('field')
		items = data.get('items')

		# Validation
		if strategy_id is None:
			raise ConnectionRefusedError('`strategy_id` not found.')
		elif field is None:
			raise ConnectionRefusedError('`field` not found.')

		# Authenticate user
		res, status = key_or_login_required(strategy_id, AccessLevel.LIMITED, disable_abort=True)
		if status != 200:
			raise ConnectionRefusedError(res['message'])

		if field == 'ontrade':
			with ctrl.app.app_context():
				leave_room(strategy_id)

		elif field == 'ontick':
			# `ontick` Validation
			if items is None:
				raise ConnectionRefusedError('`items` not found.')

			if isinstance(items, list):
				for i in range(len(items)):
					c_i = items[i]
					if isinstance(c_i, dict):
						# Item Validation
						if (c_i.get('broker') is None or 
							c_i.get('product') is None or
							c_i.get('period') is None):
							raise ConnectionRefusedError(f'Insufficient item data ({i}).')

						with ctrl.app.app_context():
							leave_room(
								f"{c_i.get('broker')}:{c_i.get('product')}:{c_i.get('period')}"
							)
					else:
						raise ConnectionRefusedError(f'Item object must be dict ({i}).')
			else:
				raise ConnectionRefusedError(f'`items` object must be list.')




