import os
import time
import math
import json, jwt
import re, string, random
from datetime import datetime
from enum import Enum
from flask import (
	Blueprint, Response, flash, abort, current_app, 
	g, redirect, request, url_for, stream_with_context,
	make_response
)
from app import auth, tradelib as tl
from app.error import OrderException, AccountException
from werkzeug.utils import secure_filename
from werkzeug.exceptions import BadRequest

bp = Blueprint('v1', __name__, url_prefix='/v1')

# `/strategy` ept

class AccessLevel(Enum):
	ADMIN = 0
	DEVELOPER = 1
	LIMITED = 2

def getJson():
	try:
		body = request.get_json(force=True)
	except BadRequest:
		error = {
			'error': 'BadRequest',
			'message': 'Unrecognizable JSON body provided.'
		}
		abort(Response(
			json.dumps(error, indent=2),
			status=400, content_type='application/json'
		))

	return body


def upload():
	file = request.data
	save_path = os.path.join(current_app.config['DATA_DIR'], secure_filename(request.headers.get('Filename')))
	current_chunk = int(request.headers.get('Chunkindex'))

	# If the file already exists it's ok if e are appending to it,
	# but not if it's a new file that would overwrite an existing one
	if os.path.exists(save_path) and current_chunk == 0:
		res = { 'error': 'IOError', 'message': 'File already exists.' }
		return abort(
			Response(
				json.dumps(res, indent=2),
				status=400, content_type='application/json'
			)
		)

	try:
		with open(save_path, 'ab') as f:
			f.seek(int(request.headers.get('Chunkbyteoffset')))
			f.write(file)
	except OSError:
		raise Exception('Could not write to file')

	total_chunks = int(request.headers.get('Totalchunkcount'))

	if current_chunk + 1 == total_chunks:
		# This was the last chunk, the file should be complete and the size we expect
		if os.path.getsize(save_path) != int(request.headers.get('Totalfilesize')):
			raise Exception('Size mismatch')
		else:
			print('Successfully uploaded')
			return True
	else:
		# print(f"Chunk {current_chunk + 1} of {total_chunks} for file {request.headers.get('Filename')} complete")
		return False


@bp.route('/account', methods=('GET',))
@auth.login_required
def get_account_ept():
	res = g.user.getAccountDetails()
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy', methods=('POST',))
@auth.login_required
def create_strategy_ept():
	body = getJson()

	if not body.get('name'):
		error = {
			'error': 'ValueError',
			'message': '`name` not submitted.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=400, content_type='application/json'
		)
	elif body.get('broker') is None:
		error = {
			'error': 'ValueError',
			'message': '`broker` not submitted.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=400, content_type='application/json'
		)
	elif body.get('accounts') is None:
		error = {
			'error': 'ValueError',
			'message': '`accounts` not submitted.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=400, content_type='application/json'
		)

	strategy_id = g.user.createStrategy(body.get('name'), body.get('broker'), body.get('accounts'))

	res = {
		'strategy_id': strategy_id
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>', methods=('PUT',))
@auth.login_required
def update_strategy_ept():
	return


@bp.route('/strategy/<strategy_id>', methods=('DELETE',))
@auth.login_required
def delete_strategy_ept(strategy_id):
	g.user.deleteStrategy(strategy_id)

	res = {
		'strategy_id': strategy_id
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


def check_key(strategy_id, req_access):
	key = request.headers.get('Authorization')
	if key is None:
		error = {
			'error': 'AuthorizationException',
			'message': 'Invalid authorization key.'
		}
		return error, 403

	key = key.split(' ')
	if len(key) == 2:
		if key[0] == 'Bearer':
			# Decode JWT API key
			try:
				payload = jwt.decode(key[1], current_app.config['SECRET_KEY'], algorithms=['HS256'])
			except jwt.exceptions.DecodeError:
				error = {
					'error': 'AuthorizationException',
					'message': 'Invalid authorization key.'
				}
				return error, 403

			# Check if key exists
			acc = ctrl.accounts.getAccount(payload.get('sub'))

			if not acc.checkKey(strategy_id, key[1]):
				error = {
					'error': 'AuthorizationException',
					'message': 'Invalid authorization key.'
				}
				return error, 403

			# Check if access level requirements are met
			if int(payload.get('access')) > req_access.value:
				error = {
					'error': 'AuthorizationException',
					'message': 'Permission requirements were not met.'
				}
				return error, 403

			return payload.get('sub'), 200

	error = {
		'error': 'ValueError',
		'message': 'Unrecognizable authorization key.'
	}
	return error, 400

def key_or_login_required(strategy_id, req_access, disable_abort=False):
	res, status = auth.check_login()
	if status != 200:
		res, status = check_key(strategy_id, req_access)
		if status != 200:
			if disable_abort:
				return res, status
			else:
				abort(Response(
					json.dumps(res, indent=2),
					status=status, content_type='application/json'
				))
	return res, 200

def get_user_id():
	if g.user is not None:
		return g.user.userId
	else:
		key = request.headers.get('Authorization')
		if key is None:
			error = {
				'error': 'AuthorizationException',
				'message': 'Invalid authorization key.'
			}
			abort(Response(
				json.dumps(error, indent=2),
				status=403, content_type='application/json'
			))
			
		key = key.split(' ')
		if len(key) == 2:
			if key[0] == 'Bearer':
				# Decode JWT API key
				try:
					payload = jwt.decode(key[1], current_app.config['SECRET_KEY'], algorithms=['HS256'])
				except jwt.exceptions.DecodeError:
					error = {
						'error': 'AuthorizationException',
						'message': 'Invalid authorization key.'
					}
					abort(Response(
						json.dumps(error, indent=2),
						status=403, content_type='application/json'
					))

				return payload.get('sub')

		error = {
			'error': 'ValueError',
			'message': 'Unrecognizable authorization key.'
		}
		abort(Response(
			json.dumps(error, indent=2),
			status=400, content_type='application/json'
		))

@bp.route('/strategy/<strategy_id>/key', methods=('POST',))
@auth.login_required
def generate_key(strategy_id):
	access = request.args.get('access')
	if access == None:
		access = AccessLevel.LIMITED.value

	user_id = g.user.userId

	# Generate JWT
	payload = { 'sub': user_id, 'iat': round(time.time()), 'access': access }
	key = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')

	# Save to database
	result = ctrl.getDb().createKey(user_id, strategy_id, key)
	# Save to account
	g.user.addKey(strategy_id, key)

	res = { 'key': key }
	return Response(
		json.dumps(res, indent=2), 
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/key/<key>', methods=('DELETE',))
@auth.login_required
def delete_key(strategy_id, key):
	user_id = g.user.userId

	# Delete from database
	result = ctrl.getDb().deleteKey(user_id, strategy_id, key)
	# Delete from account
	g.user.deleteKey(strategy_id, key)

	res = { 'key': key }
	return Response(
		json.dumps(res, indent=2), 
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>', methods=('GET',))
def get_strategy_info_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	strategy = account.getStrategy(strategy_id)
	strategy.update(account.getStrategyGui(strategy_id))

	script_id = account.getScriptId(strategy_id)
	strategy['input_variables'] = account.getStrategyInputVariables(strategy_id, script_id)
	# strategy.update(account.getStrategyTransactions(strategy_id))

	return Response(
		json.dumps(strategy, indent=2), 
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/init', methods=('POST',))
def init_strategy_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	strategy = account.getStrategy(strategy_id)

	return Response(
		json.dumps(strategy, indent=2), 
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/<broker_id>/<account_id>', methods=('GET',))
def get_strategy_account_info_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	account.startStrategy(strategy_id)

	account_code = '.'.join((broker_id, account_id))
	result = account.getAccountInfo(strategy_id, account_code)
	return Response(
		json.dumps(result, indent=2), 
		status=200, content_type='application/json'
	)


@bp.route('/scripts/<script_id>', methods=('POST',))
def update_script_ept(script_id):
	body = getJson()
	if body['properties'].get('input_variables') is not None:
		ctrl.getDb().updateScriptInputVariables(script_id, body['properties']['input_variables'])

	result = {
		'script_id': script_id
	}
	return Response(
		json.dumps(result, indent=2), 
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/start/<broker_id>', methods=('POST',))
def start_script_ept(strategy_id, broker_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.ADMIN)
	account = ctrl.accounts.getAccount(user_id)
	key = request.headers.get('Authorization').replace('Bearer ', '')
	
	# Make sure strategy is started
	account.startStrategy(strategy_id)

	# Get accounts
	body = getJson()

	accounts = body.get('accounts')
	input_variables = body.get('input_variables')
	if accounts is not None:
		broker = account.getStrategyBroker(broker_id)
		for account_id in accounts:
			# Account validation check
			if broker is None or not account_id in broker.getAccounts():
				res = { 'error': 'NotFound', 'message': f'Account {account_code} not found.' }
				return Response(
					json.dumps(res, indent=2), 
					status=404,
					content_type='application/json'
				)

		# package = account.runStrategyScript(strategy_id, broker_id, accounts, input_variables)
		success = account._runStrategyScript(strategy_id, broker_id, accounts, key, input_variables)

		res = account.getStrategy(strategy_id)
		return Response(
			json.dumps(res, indent=2),
			status=200, content_type='application/json'
		)


	else:
		raise AccountException('Body does not contain `accounts`.')

@bp.route('/strategy/<strategy_id>/stop/<broker_id>', methods=('POST',))
def stop_script_ept(strategy_id, broker_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.ADMIN)
	account = ctrl.accounts.getAccount(user_id)
	broker = account.getStrategyBroker(strategy_id)

	# Get accounts
	body = getJson()

	accounts = body.get('accounts')
	if accounts is not None:
		# package = account.stopStrategyScript(broker_id, accounts)
		success = account._stopStrategyScript(broker_id, accounts)

		res = account.getStrategy(strategy_id)
		return Response(
			json.dumps(res, indent=2),
			status=200, content_type='application/json'
		)

	else:
		raise AccountException('Body does not contain `accounts`.')


@bp.route('/strategy/<strategy_id>/compile', methods=('POST',))
@auth.login_required
def compile_strategy_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	properties = account.compileStrategy(strategy_id)
	res = { 'properties': properties }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/variables', methods=('GET',))
@auth.login_required
def get_strategy_input_variables_ept(strategy_id, script_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	script_id = account.getScriptId(strategy_id)
	input_variables = account.getStrategyInputVariables(strategy_id, script_id)

	body = getJson()
	if body.get('preset'):
		input_variables = input_variables.get(preset)

	res = { 'input_variables': input_variables }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/variables', methods=('POST',))
@auth.login_required
def replace_strategy_input_variables_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	body = getJson()
	input_variables = account.replaceStrategyInputVariables(strategy_id, body)
	res = { 'input_variables': input_variables }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/variables/<broker_id>/<account_id>', methods=('GET',))
@auth.login_required
def get_account_input_variables_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	script_id = account.getScriptId(strategy_id)
	account_code = '.'.join((broker_id, account_id))
	input_variables = account.getAccountInputVariables(strategy_id, account_code, script_id)

	body = getJson()
	if body.get('preset'):
		input_variables = input_variables.get(preset)

	res = { 'input_variables': input_variables }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/variables/<broker_id>/<account_id>', methods=('POST',))
@auth.login_required
def replace_account_input_variables_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	body = getJson()
	account_code = '.'.join((broker_id, account_id))
	input_variables = account.replaceAccountInputVariables(strategy_id, account_code, body)
	res = { 'input_variables': input_variables }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


# Order/Position Functions
def create_order(strategy_id, broker_id, data):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	# Validation
	if not (
		'product' in data and 'lotsize' in data and
		'order_type' in data and 'direction' in data and
		'accounts' in data
	):
		raise OrderException('Insufficient data provided.')

	direction = data['direction'].lower()
	del data['direction']

	broker_accounts = data['accounts']
	del data['accounts']

	if not all(map(lambda x: x in broker.getAccounts(), broker_accounts)):
		raise OrderException('Some account(s) provided not found in strategy.')

	if data['order_type'] == tl.MARKET_ORDER:
		if 'entry_range' in data: del data['entry_range']
		if 'entry_price' in data: del data['entry_price']

	res = {}
	for account_id in broker_accounts:
		data['account_id'] = account_id
		if direction == tl.LONG:
			result = broker.buy(**data)
		elif direction == tl.SHORT:
			result = broker.sell(**data)
		else:
			raise OrderException('Unrecognizable `direction` provided.')

		res.update(result)

	return res, 200


def get_all_orders(strategy_id, broker_id, accounts):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	if accounts:
		accounts = re.split(', |,', accounts)
		for acc in accounts:
			res[acc] = broker.getAllOrders(acc)
	else:
		for order in broker.getAllOrders():
			if not order['account_id'] in res:
				res[order['account_id']] = []
			res[order['account_id']].append(order)

	return res, 200


def get_orders(strategy_id, broker_id, order_ids):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	for o in order_ids:
		order = broker.getOrderByID(o)
		if order:
			if not order['account_id'] in res:
				res[order['account_id']] = []
			res[order['account_id']].append(order)

	return res, 200


def update_order(strategy_id, broker_id, data):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}

	if data.get('items'):
		for modify in data['items']:
			order_id = modify['order_id']
			del modify['order_id']
			
			order = broker.getOrderByID(order_id)
			if order:
				result = order.modify(**modify)
				res.update(result)
			else:
				res[broker.generateReference()] = {
					'timestamp': math.floor(time.time()),
					'order_type': tl.MODIFY,
					'accepted': False,
					'message': 'Order does not exist.',
					'item': {
						'order_id': order_id
					}
				}

	else:
		raise OrderException('Body does not contain `items`.')

	return res, 200


def delete_order(strategy_id, broker_id, data):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	if data.get('items'):
		for delete in data['items']:
			order_id = delete['order_id']
			order = broker.getOrderByID(order_id)
			if order:
				result = order.cancel()
				res.update(result)
			else:
				res[broker.generateReference()] = {
					'timestamp': math.floor(time.time()),
					'order_type': tl.ORDER_CANCEL,
					'accepted': False,
					'message': 'Order does not exist.',
					'item': {
						'order_id': order_id
					}
				}
	else:
		raise OrderException('Body does not contain `items`.')

	return res, 200


def get_all_positions(strategy_id, broker_id, accounts):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	if accounts:
		accounts = re.split(', |,', accounts)
		for acc in accounts:
			res[acc] = broker.getAllPositions(acc)
	else:
		for pos in broker.getAllPositions():
			if not pos['account_id'] in res:
				res[pos['account_id']] = []
			res[pos['account_id']].append(pos)

	return res, 200


def get_positions(strategy_id, broker_id, order_ids):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	for o in order_ids:
		pos = broker.getPositionByID(o)
		if pos:
			if not pos['account_id'] in res:
				res[pos['account_id']] = []
			res[pos['account_id']].append(pos)

	return res, 200


def update_position(strategy_id, broker_id, data):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	if data.get('items'):
		for modify in data['items']:
			order_id = modify['order_id']
			del modify['order_id']
			
			pos = broker.getPositionByID(order_id)
			if pos:
				result = pos.modify(**modify)
				res.update(result)
			else:
				res[broker.generateReference()] = {
					'timestamp': math.floor(time.time()),
					'order_type': tl.MODIFY,
					'accepted': False,
					'message': 'Position does not exist.',
					'item': {
						'order_id': order_id
					}
				}

	else:
		raise OrderException('Body does not contain `items`.')

	return res, 200


def delete_position(strategy_id, broker_id, data):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = {}
	if data.get('items'):
		for delete in data['items']:
			order_id = delete['order_id']
			pos = broker.getPositionByID(order_id)
			if pos:
				result = pos.close(delete.get('lotsize'))
				res.update(result)
			else:
				res[broker.generateReference()] = {
					'timestamp': math.floor(time.time()),
					'order_type': tl.POSITION_CLOSE,
					'accepted': False,
					'message': 'Position does not exist.',
					'item': {
						'order_id': order_id
					}
				}
	else:
		raise OrderException('Body does not contain `items`.')

	return res, 200

def get_account_info(strategy_id, broker_id, account_id):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = broker.getAccountInfo(account_id)
	return res, 200

# Order/Position epts

# `/orders` ept
@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/orders', methods=('POST',))
def create_orders_ept(strategy_id, broker_id):
	# Order Data
	body = getJson()
	res, status = create_order(strategy_id, broker_id, body)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/orders', methods=('GET',))
def get_all_orders_ept(strategy_id, broker_id):
	accounts = request.args.get('accounts')
	res, status = get_all_orders(strategy_id, broker_id, accounts)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/orders/<order_ids>', methods=('GET',))
def get_orders_ept(strategy_id, broker_id, order_ids):
	order_ids = re.split(', |,', order_ids)
	res, status = get_orders(strategy_id, broker_id, order_ids)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/orders', methods=('PUT',))
def update_orders_ept(strategy_id, broker_id):
	# Request Data
	body = getJson()
	res, status = update_order(strategy_id, broker_id, body)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/orders', methods=('DELETE',))
def delete_orders_ept(strategy_id, broker_id):
	# Request Data
	body = getJson()
	res, status = delete_order(strategy_id, broker_id, body)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


# `/positions` ept
@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/positions', methods=('GET',))
def get_all_positions_ept(strategy_id, broker_id):
	accounts = request.args.get('accounts')
	res, status = get_all_positions(strategy_id, broker_id, accounts)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/positions/<order_ids>', methods=('GET',))
def get_positions_ept(strategy_id, broker_id, order_ids):
	order_ids = re.split(', |,', order_ids)
	res, status = get_positions(strategy_id, broker_id, order_ids)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/positions', methods=('PUT',))
def update_position_ept(strategy_id, broker_id):
	# Request Data
	body = getJson()
	res, status = update_position(strategy_id, broker_id, body)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/positions', methods=('DELETE',))
def delete_position_ept(strategy_id, broker_id):
	# Request Data
	body = getJson()
	res, status = delete_position(strategy_id, broker_id, body)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


# `/account` ept
@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/accounts/<account_id>', methods=('GET',))
def get_account_info_ept(strategy_id, broker_id, account_id):
	res, status = get_account_info(strategy_id, broker_id, account_id)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


# `/prices` ept
@bp.route('/prices/<broker>/<product>/<period>', methods=('GET',))
def get_historical_prices_ept(broker, product, period):
	_from = request.args.get('from')
	to = request.args.get('to')
	count = request.args.get('count')
	tz = request.args.get('tz')
	if not tz: tz = 'UTC'

	broker = ctrl.brokers.getBroker(broker.lower())
	if broker is None:
		error = {
			'error': 'NotFound',
			'message': 'Broker not found.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=404, content_type='application/json'
		)

	# Convert time to datetime
	if count or (_from and to):
		try:
			if count:
				count = int(count)
			if _from:
				_from = tl.utils.setTimezone(
					datetime.strptime(_from, '%Y-%m-%dT%H:%M:%SZ'), tz
				)
			if to:
				to = tl.utils.setTimezone(
					datetime.strptime(to, '%Y-%m-%dT%H:%M:%SZ'), tz
				)

		except ValueError as e:
			res = {
				'error': 'Value Error',
				'message': 'Unrecognisable date format, use `%Y-%m-%dT%H:%M:%SZ`.'
			}
			return Response(
				json.dumps(res, indent=2), 
				status=400,
				content_type='application/json'
			)

		prices = broker._download_historical_data(
			product, period, start=_from, end=to,
			count=count, force_download=False
		)
		
	else:
		res = {
			'error': 'ValueError',
			'message': 'Insufficient parameters. Use `from` and `to` or `count`.'
		}
		return Response(
			json.dumps(res, indent=2), 
			status=400,
			content_type='application/json'
		)

	page_count = 5000

	# Get historical prices 
	ts = prices.index.values[:page_count]
	asks = prices.values[:page_count, :4]
	bids = prices.values[:page_count, 4:]
	res = {
		'product': product,
		'period': period,
		'ohlc': {
			'timestamps': ts.tolist(),
			'asks': asks.tolist(),
			'bids': bids.tolist(),
		}
	}
	return Response(
		json.dumps(res, indent=2), 
		status=200,
		content_type='application/json'
	)

# `/gui` ept
@bp.route('/strategy/<strategy_id>/gui', methods=('GET',))
def get_strategy_gui_details_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	gui = account.getStrategyGui(strategy_id)
	if gui is None:
		error = {
			'error': 'NotFound',
			'message': 'Strategy not found.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=404, content_type='application/json'
		)

	return Response(
		json.dumps(gui, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/gui', methods=('PUT',))
@auth.login_required
def update_strategy_gui_items_ept(strategy_id):
	account = g.user

	body = getJson()
	item_ids = account.updateStrategyGuiItems(strategy_id, body)

	res = {
		'item_ids': item_ids
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/gui', methods=('POST',))
@auth.login_required
def create_strategy_gui_item_ept(strategy_id):
	account = g.user

	body = getJson()
	item_id = account.createStrategyGuiItem(strategy_id, body)

	res = {
		'item_id': item_id
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/gui/<broker_id>/<account_id>', methods=('GET',))
def get_account_gui_details_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	account_code = '.'.join((broker_id, account_id))
	gui = account.getAccountGui(strategy_id, account_code)
	if gui is None:
		error = {
			'error': 'NotFound',
			'message': 'Strategy not found.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=404, content_type='application/json'
		)

	return Response(
		json.dumps(gui, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/gui/<broker_id>/<account_id>', methods=('PUT',))
def update_account_gui_details_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	body = getJson()
	account_code = '.'.join((broker_id, account_id))
	account.updateAccountGui(strategy_id, account_code, body)

	res = { 'message': 'success' }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/gui', methods=('DELETE',))
@auth.login_required
def delete_strategy_gui_items_ept(strategy_id):
	account = g.user

	body = getJson()
	item_ids = account.deleteStrategyGuiItems(strategy_id, body)

	res = {
		'item_ids': item_ids
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/backtest/<backtest_id>/gui', methods=('PUT',))
@auth.login_required
def update_backtest_gui_items_ept(strategy_id, backtest_id):
	account = g.user

	body = getJson()
	item_ids = account.updateBacktestGuiItems(strategy_id, backtest_id, body)

	res = {
		'item_ids': item_ids
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


# `/drawings` ept
@bp.route("/strategy/<strategy_id>/gui/drawings/<drawing_layer>", methods=('POST',))
def create_drawings_ept(strategy_id, drawing_layer):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	body = getJson()
	if body.get('drawings') is None:
		error = {
			'error': 'ValueError',
			'message': 'Unrecognisable format.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=400, content_type='application/json'
		)

	created = account.createDrawings(strategy_id, drawing_layer, body.get('drawings'))
	res = { 'created': created }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route("/strategy/<strategy_id>/gui/drawings/<drawing_layer>", methods=('DELETE',))
def delete_drawings_ept(strategy_id, drawing_layer):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	body = getJson()
	drawing_ids = body.get('drawings')
	if drawing_ids is not None:
		deleted = account.deleteDrawingsById(strategy_id, drawing_layer, drawing_ids)
		res = { 'deleted': deleted }
	else:
		layer = account.deleteDrawingLayer(strategy_id, drawing_layer)
		res = { 'layer': layer }
		

	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route("/strategy/<strategy_id>/gui/drawings", methods=('DELETE',))
def delete_all_drawings_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	deleted = account.deleteAllDrawings(strategy_id)
	res = { 'deleted': deleted }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


# `/backtest` ept

@bp.route('/strategy/<strategy_id>/backtest/<backtest_id>', methods=('GET',))
@auth.login_required
def get_backtest_info_ept(strategy_id, backtest_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	backtest = account.getBacktestInfo(strategy_id, backtest_id)
	# backtest.update(account.getBacktestTransactions(strategy_id, backtest_id))
	return Response(
		json.dumps(backtest, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/backtest', methods=('POST',))
# @auth.login_required
def upload_backtest_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)

	if upload():
		account = ctrl.accounts.getAccount(user_id)

		filename = request.headers.get('Filename')
		path = os.path.join(current_app.config['DATA_DIR'], filename)
		with open(path, 'r') as f:
			backtest = json.loads(f.read())

		backtest_id = account.uploadBacktest(strategy_id, backtest)
		res = { 'backtest_id': backtest_id }
		os.remove(path)
	else:
		res = {'message': 'Chunk upload successful.'}

	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/backtest/<backtest_id>/transactions', methods=('GET',))
@auth.login_required
def get_backtest_transactions_ept(strategy_id, backtest_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	transactions = account.getBacktestTransactions(strategy_id, backtest_id)
	return Response(
		json.dumps(transactions, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/backtest/<start>/<end>', methods=('POST',))
@auth.login_required
def perform_backtest_ept(strategy_id, start, end):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	key = request.headers.get('Authorization').replace('Bearer ', '')

	start = datetime.strptime(start, '%Y-%m-%dT%H:%M:%SZ').timestamp()
	end = datetime.strptime(end, '%Y-%m-%dT%H:%M:%SZ').timestamp()
	
	body = getJson()
	if body.get('broker'):
		broker = body.get('broker')
	else:
		broker = 'ig'
	
	input_variables = body.get('input_variables')

	account.performBacktest(strategy_id, broker, start, end, key, input_variables)
	res = { 'message': 'started' }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


# `/charts` ept
@bp.route('/strategy/<strategy_id>/charts', methods=('POST',))
@auth.login_required
def create_chart_ept(strategy_id):
	account = g.user

	body = getJson()
	broker = account.getStrategyBroker(strategy_id)

	result = []
	for product in body.get('items'):
		broker.getChart(product)
		result.append(product)

	res = {
		'broker': broker.name,
		'products': result
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


# `/stream` ept
@bp.route("/strategy/<strategy_id>/stream/ontick", methods=('POST',))
def ontick_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	broker = account.getStrategyBroker(strategy_id)

	s_buffer = []
	def append_buffer(item):
		item = json.dumps(item) + '\n'
		print(item)
		s_buffer.append(item.encode('utf-8'))

	def data_stream(subs):
		try:
			while True:
				for i in range(len(s_buffer)-1,-1,-1):
					yield(s_buffer.pop(i))
				greenthread.sleep(0)
		except GeneratorExit:
			for sub in subs:
				sub[0].unsubscribe(sub[1], sub[2], sub[3])

	# Schema: { $product: [$periods] }
	charts_req = getJson()
	subs = []
	for product, v in charts_req.items():
		chart = ctrl.charts.getChart(broker.name, product)
		for period in v:
			# TODO: Validation
			sub_id = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
			chart.subscribe(period, strategy_id, sub_id, append_buffer)
			subs.append((chart, period, strategy_id, sub_id))

	return Response(
		stream_with_context(data_stream(subs)),
		status=200,
		content_type='application/json'
	)


@bp.route("/strategy/<strategy_id>/stream/ontrade")
def ontrade_ept(strategy_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	broker = account.getStrategyBroker(strategy_id)

	s_buffer = []
	def append_buffer(item):
		s_buffer.append(json.dumps(item).encode('utf-8'))

	def data_stream(sub_id):
		try:
			while True:
				for i in range(len(s_buffer)-1,-1,-1):
					yield(s_buffer.pop(i))

		except GeneratorExit:
			broker.unsubscribeOnTrade(sub_id)

	sub_id = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
	broker.subscribeOnTrade(append_buffer, sub_id)

	return Response(
		stream_with_context(data_stream(sub_id)),
		status=200,
		content_type='application/json'
	)





