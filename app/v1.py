import os
import io
import time
import math
import json, jwt
import gzip
import re, string, random
import requests
import shortuuid
import traceback
import pandas as pd
import stripe
from stripe.error import CardError
from dateutil import parser
from datetime import datetime, timedelta
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
from werkzeug.security import generate_password_hash
from threading import Thread

bp = Blueprint('v1', __name__, url_prefix='/v1')

subscription_info = {
	"hgpro": {
		0: {
			"name": "HG Pro Kick Starter",
			"product": "HolyGrail_Pro_kickstarter"
		},
		1: {
			"name": "HG Pro Standard",
			"product": "HolyGrail_Pro"
		},
		2: {
			"name": "HG Pro Professional",
			"product": "HolyGrail_Pro_professional"
		},
		3: {
			"name": "HG Pro Hedge Fund",
			"product": "HolyGrail_Pro_professional"
		},
		4: {
			"name": "HG Pro Kick Starter",
			"product": "HolyGrail_Pro_kickstarter"
		},
		5: {
			"name": "HG Pro Standard",
			"product": "HolyGrail_Pro"
		}
	}
}

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


@bp.route('/account', methods=('POST',))
@auth.login_required
def update_account_ept():
	body = getJson()

	if 'password' in body:
		body['password'] = generate_password_hash(body['password'])

	if 'email' in body:
		email = body['email']
		if ctrl.getDb().getUserByEmail(email) is not None:
			error = {
				'error': 'ValueError',
				'message': 'Email {} is already registered.'.format(email)
			}
			return Response(
				json.dumps(error, indent=2), 
				status=400, content_type='applcation/json'
			)
		else:
			account = g.user.getAccountDetails()
			if account.get('email') != body['email']:
				body['email_confirmed'] = False

	if 'notify_me' in body:
		body['email_opt_out'] = not body['notify_me']
		del body['notify_me']
		
	print(body)

	if len(body):
		ctrl.getDb().updateUser(g.user.userId, body)
	res = g.user.getAccountDetails()
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/unsubscribe', methods=('POST',))
def unsubscribe_email_ept():
	body = getJson()

	if not "email" in body:
		res = { "message": "No email provided." }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	email = body["email"]

	user = ctrl.getDb().getUserByEmail(email)

	if user is not None:
		update = { "email_opt_out": True }
		ctrl.getDb().updateUser(user["user_id"], update)
		res = { "message": "Success." }
		return Response(
			json.dumps(res, indent=2),
			status=200, content_type='application/json'
		)
	else:
		res = { "message": "User not found." }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)
	


@bp.route('/account', methods=('DELETE',))
@auth.login_required
def delete_account_ept():
	# Check all scripts have been stopped

	# Remove All user Memory


	# Delete User Storage
	ctrl.getDb().deleteUser(g.user.userId)
	ctrl.getDb().deleteAllUserStrategyStorage(g.user.userId)


	
	res = { 'user_id': g.user.userId }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/session', methods=('GET',))
@auth.login_required
def get_session_token():
	res = {
		'token': g.user.generateSessionToken()
	}
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

	strategy_id = g.user.createStrategy(body)

	if not strategy_id:
		res = {
			'message': 'Package in use.'
		}
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)
	else:
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


@bp.route('/strategy/details', methods=('GET',))
@auth.login_required
def get_all_strategy_details_ept():
	user = ctrl.getDb().getUser(g.user.userId)

	details = []
	for strategy_id in user['strategies']:
		details.append({
			'strategy_id': strategy_id,
			**user['strategies'][strategy_id]
		})

	res = {
		'strategies': details
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/package/<package_id>', methods=('GET',))
@auth.login_required
def is_package_in_use_ept(package_id):
	user = ctrl.getDb().getUser(g.user.userId)

	for strategy_id in user['strategies']:
		if user['strategies'][strategy_id].get('package').split('.')[0] == package_id:
			res = {
				'strategy_id': strategy_id
			}
			return Response(
				json.dumps(res, indent=2),
				status=200, content_type='application/json'
			)

	res = {
		'strategy_id': None
	}
	return Response(
		json.dumps(res, indent=2),
		status=400, content_type='application/json'
	)


@bp.route('/package/available', methods=('POST',))
@auth.login_required
def is_multiple_packages_in_use_ept():
	body = getJson()
	user = ctrl.getDb().getUser(g.user.userId)

	result = []
	for package_id in body.get('packages'):
		for strategy_id in user['strategies']:
			if user['strategies'][strategy_id].get('package').split('.')[0] == package_id:
				result.append(package_id)

	res = {
		'packages': result
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
			except jwt.exceptions.ExpiredSignatureError:
				error = {
					'error': 'AuthorizationException',
					'message': 'Authorization key expired.'
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

	def data_stream(data):
		data = json.dumps(data).encode('utf-8')

		blocksize = 512
		chunkindex = 0
		chunkbyteoffest = 0
		totalfilesize = len(data)
		totalchunkcount = math.ceil(totalfilesize / blocksize)

		while chunkindex < totalchunkcount:
			t_data = data[chunkbyteoffest:chunkbyteoffest+blocksize]
			yield(t_data)

			chunkindex += 1
			chunkbyteoffest += blocksize

	return Response(
		stream_with_context(data_stream(strategy)),
		status=200,
		content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/info/<broker_id>/<account_id>', methods=('GET',))
def get_basic_strategy_info_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	strategy_info = ctrl.getDb().getStrategy(account.userId, strategy_id)
	script_id = strategy_info["package"].split('.')[0]
	strategy_info["used_bank"] = account.getMaximumBanks(strategy_id, broker_id, account_id, script_id)

	return Response(
		json.dumps(strategy_info, indent=2), status=200,
		content_type='application/json'
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


@bp.route('/strategy/<strategy_id>/<broker_id>', methods=('GET',))
def is_broker_authorized_ept(strategy_id, broker_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	account.startStrategy(strategy_id)
	is_auth = account.brokers.get(broker_id).is_auth

	res = {
		'Authorized': is_auth
	}
	if is_auth:
		status = 200
	else:
		status = 400

	return Response(
		res, status=status,
		content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/<broker_id>/<account_id>', methods=('GET',))
def get_strategy_account_info_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	account.startStrategy(strategy_id)

	account_code = '.'.join((broker_id, account_id))
	result = account.getAccountInfo(strategy_id, account_code)

	def data_stream(data):
		data = json.dumps(data).encode('utf-8')

		blocksize = 512
		chunkindex = 0
		chunkbyteoffest = 0
		totalfilesize = len(data)
		totalchunkcount = math.ceil(totalfilesize / blocksize)

		while chunkindex < totalchunkcount:
			t_data = data[chunkbyteoffest:chunkbyteoffest+blocksize]
			yield(t_data)

			chunkindex += 1
			chunkbyteoffest += blocksize

	return Response(
		stream_with_context(data_stream(result)),
		status=200,
		content_type='application/json'
	)
	# return Response(
	# 	json.dumps(result, indent=2), 
	# 	status=200, content_type='application/json'
	# )


@bp.route('/users/<user_id>/file', methods=('POST',))
def get_user_file_ept(user_id):
	body = getJson()

	result = None
	file_name = body.get("file_name")
	if (
		file_name is not None and
		user_id == "hgpro" or user_id == "demo"
	):
		file_name = file_name.replace('.gz', '')

		file_type = None
		if file_name.endswith('.csv'):
			file_type = 'csv'
		elif file_name.endswith('.json'):
			file_type = 'json'
		else:
			return None

		result = ctrl.getDb().getUserFile(user_id, file_name)

		if result is not None:
			if file_type == 'csv':
				result = pd.read_csv(io.BytesIO(result), sep=',', dtype=str).to_dict()
			elif file_type == 'json':
				result = json.loads(result)
			else:
				result = None

	if result is None:
		res = { 'message': 'Bad Request.' }
		return Response(
			json.dumps(result, indent=2), 
			status=400, content_type='application/json'
		)
	else:
		res = { 'item': result }
		return Response(
			json.dumps(res, indent=2), 
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


@bp.route('/scripts/<script_id>/<file_name>', methods=('GET',))
def get_script_file_ept(script_id, file_name):
	file_name = file_name.replace('.gz', '')

	file_type = None
	if file_name.endswith('.csv'):
		file_type = 'csv'
	elif file_name.endswith('.json'):
		file_type = 'json'
	else:
		return None

	result = ctrl.getDb().getScriptFile(script_id, file_name)

	if result is not None:
		if file_type == 'csv':
			result = pd.read_csv(io.BytesIO(result), sep=',', dtype=str).to_dict()
		elif file_type == 'json':
			result = json.loads(result)
		else:
			result = None

	if result is None:
		res = { 'message': 'Bad Request.' }
		return Response(
			json.dumps(result, indent=2), 
			status=400, content_type='application/json'
		)
	else:
		res = { 'item': result }
		return Response(
			json.dumps(res, indent=2), 
			status=200, content_type='application/json'
		)


@bp.route('/strategy/<strategy_id>/start/<broker_id>', methods=('POST',))
def start_script_ept(strategy_id, broker_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.ADMIN)
	account = ctrl.accounts.getAccount(user_id)
	# key = request.headers.get('Authorization').replace('Bearer ', '')
	
	# Make sure strategy is started
	account.startStrategy(strategy_id)

	# Get accounts
	body = getJson()

	strategy_info = ctrl.getDb().getStrategy(account.userId, strategy_id)
	account_limit = strategy_info.get("account_limit", 1)
	num_running_scripts = account.getNumScriptsRunning()
	if num_running_scripts < int(account_limit):
		accounts = body.get('accounts')
		input_variables = body.get('input_variables')
		if accounts is not None:
			broker = account.getStrategyBroker(broker_id)
			for account_id in accounts:
				# Account validation check
				if broker is None or not account_id in broker.getAccounts():
					res = { 'error': 'NotFound', 'message': f'Account {account_id} not found.' }
					return Response(
						json.dumps(res, indent=2), 
						status=404,
						content_type='application/json'
					)

			# package = account.runStrategyScript(strategy_id, broker_id, accounts, input_variables)
			success = account._runStrategyScript(strategy_id, broker_id, accounts, input_variables)

			res = account.getStrategy(strategy_id)
			return Response(
				json.dumps(res, indent=2),
				status=200, content_type='application/json'
			)

		else:
			raise AccountException('Body does not contain `accounts`.')

	else:
		res = {
			'error': 'AccountException',
			'message': 'Account limit exceeded.',
			'account_limit': account_limit
		}
		return Response(
				json.dumps(res, indent=2),
				status=400, content_type='application/json'
			)

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
		success = account._stopStrategyScript(strategy_id, broker_id, accounts)

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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
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
	# account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = broker.getAccountInfo(account_id)
	return res, 200

def get_transaction_info(strategy_id, broker_id, account_id):
	user_id = get_user_id()
	account = ctrl.accounts.getAccount(user_id)
	# account.getStrategy(strategy_id)
	broker = account.getStrategyBroker(broker_id)

	res = broker.getTransactionInfo(account_id)
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


# `/accounts` ept
@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/accounts/<account_id>', methods=('GET',))
def get_account_info_ept(strategy_id, broker_id, account_id):
	res, status = get_account_info(strategy_id, broker_id, account_id)

	return Response(
		json.dumps(res, indent=2), 
		status=status, content_type='application/json'
	)


# `/transactions` ept
@bp.route('/strategy/<strategy_id>/brokers/<broker_id>/transactions/<account_id>', methods=('GET',))
def get_transaction_info_ept(strategy_id, broker_id, account_id):
	res, status = get_transaction_info(strategy_id, broker_id, account_id)

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
	# asks = prices.values[:page_count, :4]
	mids = prices.values[:page_count, 4:8]
	# bids = prices.values[:page_count, 8:]
	res = {
		'product': product,
		'period': period,
		'ohlc': {
			'timestamps': ts.tolist(),
			# 'asks': asks.tolist(),
			'mids': mids.tolist()
			# 'bids': bids.tolist()
		}
	}

	def data_stream(data):
		data = json.dumps(data).encode('utf-8')

		blocksize = 512
		chunkindex = 0
		chunkbyteoffest = 0
		totalfilesize = len(data)
		totalchunkcount = math.ceil(totalfilesize / blocksize)

		while chunkindex < totalchunkcount:
			t_data = data[chunkbyteoffest:chunkbyteoffest+blocksize]
			yield(t_data)

			chunkindex += 1
			chunkbyteoffest += blocksize

	return Response(
		stream_with_context(data_stream(res)),
		status=200,
		content_type='application/json'
	)

	# return Response(
	# 	res, 
	# 	status=200,
	# 	content_type='application/json'
	# )

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


@bp.route('/strategy/<strategy_id>/current_account', methods=('PUT',))
@auth.login_required
def update_strategy_account_ept(strategy_id):
	body = getJson()
	account_code = body.get('account_code')

	if account_code is not None:
		account = g.user
		account.updateStrategyAccount(strategy_id, account_code)

		res = {
			'account_code': account_code
		}
		return Response(
			json.dumps(res, indent=2),
			status=200, content_type='application/json'
		)

	else:
		res = {
			'error': 'BadRequest',
			'message': 'Account code missing.'
		}
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)


@bp.route('/strategy/<strategy_id>/gui', methods=('PUT',))
@auth.login_required
def update_strategy_gui_items_ept(strategy_id):
	account = g.user

	body = getJson()
	item_ids = account.updateStrategyGuiItems(strategy_id, body)

	res = {
		'item_ids': []
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


@bp.route('/strategy/<strategy_id>/gui/<broker_id>/<account_id>', methods=('POST',))
def update_account_gui_details_ept(strategy_id, broker_id, account_id):

	if upload():
		user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
		account = ctrl.accounts.getAccount(user_id)

		filename = request.headers.get('Filename')
		path = os.path.join(current_app.config['DATA_DIR'], filename)
		with open(path, 'r') as f:
			body = json.loads(f.read())

		account_code = '.'.join((broker_id, account_id))
		account.appendAccountGui(strategy_id, account_code, body)
		account = ctrl.accounts.getAccount(user_id)

		os.remove(path)

		res = { 'message': 'success' }
	else:
		res = {'message': 'Chunk upload successful.'}

	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/gui', methods=('DELETE',))
@auth.login_required
def delete_strategy_gui_items_ept(strategy_id):
	account = g.user

	# body = getJson()
	# item_ids = account.deleteStrategyGuiItems(strategy_id, body)

	# res = {
	# 	'item_ids': item_ids
	# }
	res = {
		'item_ids': []
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/strategy/<strategy_id>/<broker_id>/<account_id>/reports/<name>', methods=('GET',))
def get_account_report_ept(strategy_id, broker_id, account_id, name):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	account_code = '.'.join((broker_id, account_id))
	gui = account.getAccountReport(strategy_id, account_code, name)
	if gui is None:
		error = {
			'error': 'NotFound',
			'message': 'Report not found.'
		}
		return Response(
			json.dumps(error, indent=2),
			status=404, content_type='application/json'
		)

	return Response(
		json.dumps(gui.to_dict(), indent=2),
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


@bp.route('/strategy/<strategy_id>/<broker_id>/<account_id>/backtest', methods=('POST',))
# @auth.login_required
def upload_live_backtest_ept(strategy_id, broker_id, account_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)

	if upload():
		account = ctrl.accounts.getAccount(user_id)

		filename = request.headers.get('Filename')
		path = os.path.join(current_app.config['DATA_DIR'], filename)
		with open(path, 'r') as f:
			backtest = json.loads(f.read())

		backtest_id = Thread(target=account.uploadLiveBacktest, args=(strategy_id, broker_id, account_id, backtest)).start()
		res = { 'message': 'successful' }
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

	result = {}
	result.update(account.getBacktestChartInfo(strategy_id, backtest_id))
	result.update(account.getBacktestTransactions(strategy_id, backtest_id))

	def data_stream(data):
		data = json.dumps(data).encode('utf-8')

		blocksize = 512
		chunkindex = 0
		chunkbyteoffest = 0
		totalfilesize = len(data)
		totalchunkcount = math.ceil(totalfilesize / blocksize)

		while chunkindex < totalchunkcount:
			t_data = data[chunkbyteoffest:chunkbyteoffest+blocksize]
			yield(t_data)

			chunkindex += 1
			chunkbyteoffest += blocksize

	return Response(
		stream_with_context(data_stream(result)),
		status=200,
		content_type='application/json'
	)
	# return Response(
	# 	json.dumps(result, indent=2),
	# 	status=200, content_type='application/json'
	# )


@bp.route('/strategy/<strategy_id>/backtest/<backtest_id>/reports/<name>', methods=('GET',))
@auth.login_required
def get_backtest_report_ept(strategy_id, backtest_id, name):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)

	report = account.getBacktestReport(strategy_id, backtest_id, name)
	return Response(
		json.dumps(report.to_dict('list'), indent=2),
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

	spread = body.get('spread')
	process_mode = body.get('process_mode')
	
	input_variables = body.get('input_variables')

	account.performBacktest(strategy_id, broker, start, end, key, input_variables, spread, process_mode)
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

	print('CREATE CHARTS')
	print(ctrl.brokers)
	print(body)
	broker = ctrl.brokers.getBroker(body.get('broker'))
	# broker = account.getStrategyBroker(strategy_id)

	if not broker is None:
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

	else:
		res = {
			'error': 'NotFound',
			'message': 'Broker not found.'
		}
		return Response(
			json.dumps(res, indent=2),
			status=404, content_type='application/json'
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


# `/analytics` ept
@bp.route("/analytics/visitors/daily", methods=("POST",))
def count_daily_visitor_ept():
	daily_visitors = ctrl.getDb().countDailyVisitor()

	res = { 'daily': daily_visitors }
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/analytics/visitors/first", methods=("POST",))
def count_unique_visitor_ept():
	unique_visitors = ctrl.getDb().countUniqueVisitor()

	res = { 'unique': unique_visitors }
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/analytics/subscribe", methods=("POST",))
def subscribe_email_ept():
	body = getJson()
	email = ctrl.getDb().subscribeEmail(body)

	res = { 'subscribed': email }
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail", methods=("GET",))
def get_all_holygrail_users_ept():
	users = ctrl.getDb().getAllHolyGrailUsers()

	res = { 'users': users }
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail/<user_id>", methods=("GET",))
def get_holygrail_user_ept(user_id):
	user = ctrl.getDb().getHolyGrailUser(user_id)

	if user is not None:
		res = user
	else:
		res = {}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail/<user_id>", methods=("POST",))
def add_holygrail_user_ept(user_id):
	user = ctrl.getDb().getProdUser(user_id)
	user_id = ctrl.getDb().addHolyGrailUser(
		user.get('user_id'), user.get('email'),
		user.get('first_name'), user.get('last_name'),
		False
	)

	res = { 
		'user_id': user.get('user_id'),
		'email': user.get('email'),
		'first_name': user.get('first_name'),
		'last_name': user.get('last_name'),
		'approved': False
	}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail/approve", methods=("PUT",))
def set_holygrail_user_approved_ept():
	body = getJson()

	if 'approved' in body and 'users' in body:
		ACCESS_GRANTED_ZOHO_EPT = 'https://flow.zoho.com.au/7001001266/flow/webhook/incoming?zapikey=1001.44fdf71722d081f97958c88559b5c639.541402092e6abc3fa6ca30e8e39c1f0b&isdebug=false'
		for user_id in body.get('users'):
			ctrl.getDb().updateHolyGrailUser(
				user_id, { 'approved': body.get('approved') }
			)

			user = ctrl.getDb().getHolyGrailUser(user_id)

			payload = {
				'email': user.get('email'),
				'first_name': user.get('first_name'),
				'last_name': user.get('last_name')
			}
			requests.post(
				ACCESS_GRANTED_ZOHO_EPT,
				data=json.dumps(payload)
			)

	res = { 
		'user_id': user_id
	}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail", methods=("DELETE",))
def delete_holygrail_user_ept():
	body = getJson()

	if 'users' in body:
		for user_id in body.get('users'):
			ctrl.getDb().deleteHolyGrailUser(user_id)

	res = { 'completed': True }
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail/auth/<user_id>", methods=("POST",))
def authorize_holygrail_user_ept(user_id):
	
	if user_id in [
		'8M5LU6uEZY9DDiH8nftqEH',
		'WbcHtB9iqBkehm3YvAGMzd', 'bgAATeNkqpn4LP6mt85Fsy'
	]:
		res = { 'authorized': True }
		return Response(
			json.dumps(res, indent=2), status=200,
			content_type='application/json'
		)

	else:
		res = { 'authorized': False }
		return Response(
			json.dumps(res, indent=2), status=403,
			content_type='application/json'
		)


@bp.route("/holygrail/all", methods=("POST",))
def add_all_current_users_to_holygrail_ept():
	users = ctrl.getDb().getAllProdUsers()

	for user in users:
		ctrl.getDb().addHolyGrailUser(
			user.get('user_id'), user.get('email'),
			user.get('first_name'), user.get('last_name'),
			True
		)

	res = { 
		'completed': True
	}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail/invite", methods=("POST",))
def invite_holygrail_user_ept():
	body = getJson()

	if 'emails' in body:
		DEMO_INVITE_ZOHO_EPT = 'https://flow.zoho.com.au/7001001266/flow/webhook/incoming?zapikey=1001.2831b69b6eeb35463eb596398c30a387.9607608a976d1031ba4e608960c3db96&isdebug=false'
		for email in body.get('emails'):
			jwt_payload = {
				'sub': email,
				'iat': time.time()
			}
			token = jwt.encode(jwt_payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf-8')
			ctrl.getDb().addHolyGrailToken(token)

			BASE_CLIENT_URL = current_app.config['BASE_CLIENT_URL']
			payload = {
				'email': email,
				'link': f'{BASE_CLIENT_URL}/login?redirect=auth%2Fholygrail&code={token}'
			}
			requests.post(
				DEMO_INVITE_ZOHO_EPT,
				data=json.dumps(payload)
			)

	res = { 
		'completed': True
	}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/holygrail/auth/token", methods=("GET",))
@auth.login_required
def check_holygrail_token_ept():
	user_id = g.user.userId

	token = request.args.get('code')
	token_exists = ctrl.getDb().checkHolyGrailToken(token)

	if token_exists:
		ctrl.getDb().deleteHolyGrailToken(token)
		user = ctrl.getDb().getUser(user_id)
		user_id = ctrl.getDb().addHolyGrailUser(
			user.get('user_id'), user.get('email'),
			user.get('first_name'), user.get('last_name'),
			True
		)

	res = { 
		'authorized': token_exists
	}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/reset-password/send", methods=("POST",))
def send_reset_password_email_ept():
	body = getJson()

	if 'email' in body:
		email = body.get('email')
		# Check email exists
		user = ctrl.getDb().getUserByEmail(email)

		if user:
			RESET_PASSWORD_ZOHO_EPT = 'https://flow.zoho.com.au/7001001266/flow/webhook/incoming?zapikey=1001.8aec610d5eba8a37c0648d19844a9dcb.5ad8e950c9a7e264265572296bf99972&isdebug=false'
			email = body.get('email')
			jwt_payload = {
				'sub': email,
				'iat': time.time()
			}
			token = jwt.encode(jwt_payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf-8')
			ctrl.getDb().addPasswordResetToken(token)

			BASE_CLIENT_URL = current_app.config['BASE_CLIENT_URL_FRONT']
			payload = {
				'email': email,
				'entry_id': shortuuid.uuid(),
				'url': f'{BASE_CLIENT_URL}/reset?code={token}'
			}
			requests.post(
				RESET_PASSWORD_ZOHO_EPT,
				data=json.dumps(payload)
			)
		else:
			res = { 
				'error': 'User does not exist.'
			}
			return Response(
				json.dumps(res, indent=2), status=400,
				content_type='application/json'
			)

	else:
		res = { 
			'completed': False
		}
		return Response(
			json.dumps(res, indent=2), status=400,
			content_type='application/json'
		)

	res = { 
		'completed': True
	}
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/reset-password", methods=("POST",))
def reset_password_ept():
	body = getJson()

	token = request.args.get('code')
	new_password = body.get('password')

	if token and new_password:	
		token_exists = ctrl.getDb().checkResetPasswordToken(token)
		if token_exists:
			email = jwt.decode(
				token, ctrl.app.config['SECRET_KEY'], algorithms=['HS256']
			).get('sub')

			if email:
				user = ctrl.getDb().getUserByEmail(email)
				if user:
					ctrl.getDb().deletePasswordResetToken(token)
					
					password_hash = generate_password_hash(new_password)
					ctrl.getDb().updateUser(
						user.get('user_id'),
						{ 'password': password_hash }
					)

					res = { 
						'completed': True
					}
					return Response(
						json.dumps(res, indent=2), status=200,
						content_type='application/json'
					)

		else:
			res = { 
				'error': 'Token does not exist.'
			}
			return Response(
				json.dumps(res, indent=2), status=400,
				content_type='application/json'
			)

	res = { 
		'completed': False
	}
	return Response(
		json.dumps(res, indent=2), status=400,
		content_type='application/json'
	)



@bp.route("/spotware/<access_token>", methods=("GET",))
def check_access_token_ept(access_token):
	broker = ctrl.brokers.getBroker('spotware')
	res = broker.checkAccessToken(access_token)

	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)


@bp.route("/brokers/replace/<broker_name>/<broker_id>", methods=("POST",))
@auth.login_required
def set_replace_broker_ept(broker_name, broker_id):
	result = g.user.setBrokerReplacement(broker_name, broker_id)

	if result:
		res = { "message": "Success" }
		return Response(
			json.dumps(res, indent=2), status=200,
			content_type='application/json'
		)
	else:
		res = { "message": "Failed" }
		return Response(
			json.dumps(res, indent=2), status=400,
			content_type='application/json'
		)


@bp.route("/ib/broker", methods=("POST",))
@auth.login_required
def reserve_ib_broker_ept():

	# Look for unused Port
	used_ports = ctrl.brokers.getUsedPorts()

	# Get/Create IB Client


	# If no ports available, create new IB Client



	res = { 'port': port, 'token': token }
	return Response(
		json.dumps(res, indent=2), status=200,
		content_type='application/json'
	)

@bp.route("/ib/auth/<broker_id>", methods=("GET",))
@auth.login_required
def get_ib_auth_ept(broker_id):
	account = g.user
	ib_broker = account.brokers.get(broker_id)

	if ib_broker is not None and not ib_broker.is_auth:
		port = ib_broker.findUnusedPort()
		payload = { 'sub': str(port), 'iat': time.time() }
		token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256').decode('utf-8')

		res = { 'port': port, 'token': token }

		start_time = time.time()
		while not ib_broker._gateway_loaded:
			if time.time() - start_time > 10:
				return Response(
					json.dumps(res, indent=2), status=400,
					content_type='application/json'
				)

		return Response(
			json.dumps(res, indent=2), status=200,
			content_type='application/json'
		)

	res = { 'message': 'Unsuccessful' }
	return Response(
		json.dumps(res, indent=2), status=400,
		content_type='application/json'
	)



@bp.route("/ib/auth", methods=("POST",))
def ib_auth_ept():
	body = getJson()
	ip = body.get('ip')
	port = str(body.get('port'))
	token = body.get('token')

	print(f'[ib_auth_ept] {ip}, {port}, {token}\n{ctrl.brokers.ib_port_sessions}')

	try:
		if port in ctrl.brokers.ib_port_sessions:
			print('[ib_auth_ept] 1')
			if (
				ip in ctrl.brokers.ib_port_sessions[port]['ips'] and
				time.time() < ctrl.brokers.ib_port_sessions[port]['expiry']
			):
				print('[ib_auth_ept] 2')
				return Response(
					json.dumps({}, indent=2), status=200,
					content_type='application/json'
				)
			elif token is not None:
				print('[ib_auth_ept] 3')
				payload = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
				if str(payload.get('sub')) == port:
					print('[ib_auth_ept] 4')
					# if not port in ctrl.brokers.ib_port_sessions:
					# 	ctrl.brokers.ib_port_sessions[port] = { 'client': None, 'expiry': time.time() + (60*10), 'ips': [ip] }
					# else:
					ctrl.brokers.ib_port_sessions[port]['expiry'] = time.time() + (60*10)
					ctrl.brokers.ib_port_sessions[port]['ips'].append(ip)

					return Response(
						json.dumps({}, indent=2), status=200,
						content_type='application/json'
					)

	except Exception:
		print(traceback.format_exc())

	return Response(
		json.dumps({}, indent=2), status=401,
		content_type='application/json'
	)
	

@bp.route("/ib/auth/confirmed", methods=("POST",))
def ib_auth_confirmed_ept():
	body = getJson()
	port = str(body.get('port'))

	print(f'[ib_auth_confirmed_ept] {port} {ctrl.brokers.ib_port_sessions}')

	if port in ctrl.brokers.ib_port_sessions:
		time.sleep(1)
		ib_broker = ctrl.brokers.ib_port_sessions[port]['client']
		if ib_broker.isLoggedIn():
			ib_broker.getAllAccounts()


	return Response(
		json.dumps({}, indent=2), status=200,
		content_type='application/json'
	)

	
@bp.route("/dukascopy/auth/captcha/<broker_id>", methods=("GET",))
@auth.login_required
def dukascopy_get_auth_captcha_ept(broker_id):
	account = g.user
	broker = account.brokers.get(broker_id)

	res = { 'image': None }
	if broker is not None:
		res['image'] = broker.getLoginCaptchaBytes()

		return Response(
			json.dumps(res, indent=2), status=200,
			content_type='application/json'
		)

	else:
		return Response(
			json.dumps(res, indent=2), status=400,
			content_type='application/json'
		)


@bp.route("/dukascopy/auth/complete/<broker_id>", methods=("POST",))
@auth.login_required
def dukascopy_complete_login_ept(broker_id):
	body = getJson()

	account = g.user
	broker = account.brokers.get(broker_id)
	username = body.get('username')
	password = body.get('password')
	is_demo = body.get('is_demo')
	captcha_result = body.get('captcha_result')

	res = { 'result': False }
	if broker is not None and all((
		username is not None,
		password is not None,
		is_demo is not None,
		captcha_result is not None
	)):
		res['result'] = broker.completeLogin(username, password, is_demo, captcha_result)

		if res['result']:
			status = 200
		else:
			status = 401

		return Response(
			json.dumps(res, indent=2), status=status,
			content_type='application/json'
		)
	else:
		return Response(
			json.dumps(res, indent=2), status=401,
			content_type='application/json'
		)


@bp.route("/delete-strategies", methods=("POST",))
def deleteStrategiesEpt():
	ctrl.getDb().deleteAllStrategies()

	res = {'message': 'done'}
	return Response(
		json.dumps(res, indent=2), status=401,
		content_type='application/json'
	)


@bp.route("/flip-notify-me", methods=("POST",))
def flipNotifyMeEpt():
	ctrl.getDb().flipNotifyMe()

	res = {'message': 'done'}
	return Response(
		json.dumps(res, indent=2), status=401,
		content_type='application/json'
	)


@bp.route('/tos', methods=('GET',))
@auth.login_required
def check_tos_ept():
	user = ctrl.getDb().getUser(g.user.userId)

	if user.get('registration_date'):
		res = { 'read': True }
		return Response(
			json.dumps(res, indent=2),
			status=200, content_type='application/json'
		)
	else:
		res = { 'read': False }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	
@bp.route('/tos', methods=('PUT',))
@auth.login_required
def update_tos_ept():
	update = { 'registration_date': datetime.utcnow().isoformat() }
	ctrl.getDb().updateUser(g.user.userId, update)

	res = { 'read': True }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)

@bp.route('/nab/callback', methods=('POST',))
def nab_callback_ept():
	
	print("NAB CALLBACK:")
	print(f"FORM: {request.form}")

	rescode = str(request.form.get("rescode"))

	# On Success Code
	if rescode == "00":
		refid = str(request.form.get("refid"))
		user_id = refid.split('|')[0]
		product = refid.split('|')[1]
		plan = refid.split('|')[2]

		user = ctrl.getDb().getUser(user_id)

		if user is not None and (plan == "hgpro_standard" or plan == "hgpro_professional"):
			# Add Purchase Verification
			if "products" in user:
				update = { "products": user["products"] }
				update["products"][product] = plan
			else:
				update = { "products": { product: plan } }
				
			update = ctrl.getDb().updateUser(user_id, update)

			# Create Strategy
			account = ctrl.accounts.getAccount(user_id)
			strategy_id = account.createStrategy({
				"name": "",
				"package": product + ".v1_0_0"
			})

			res = { "message": "Success" }
			return Response(
				json.dumps(res, indent=2),
				status=200, content_type='application/json'
			)

	res = { "message": "Done" }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/analytics/page', methods=('POST',))
@auth.login_required
def update_analytics_page_count_ept():
	body = getJson()

	user = ctrl.getDb().getUser(g.user.userId)
	page = body.get("page")
	update = { "analytics": {} }
	if user is not None and page is not None:
		if user.get("analytics") is not None:
			update["analytics"] = user["analytics"]
			if update["analytics"].get("pages") is None:
				update["analytics"]["pages"] = {}

		else:
			update["analytics"] = { "pages": {} }
		
		if page in update["analytics"]["pages"]:
			lastVisit = parser.parse(update["analytics"]["pages"][page]["lastVisit"])
			timeSinceLastVisit = (datetime.utcnow() - lastVisit).total_seconds()
			ONE_DAY = 86400
			if timeSinceLastVisit >= ONE_DAY:
				update["analytics"]["pages"][page]["visits"] += 1
				update["analytics"]["pages"][page]["lastVisit"] = datetime.utcnow().isoformat()
		else:
			update["analytics"]["pages"][page] = { 
				"visits": 1,
				"lastVisit": datetime.utcnow().isoformat()
			}

		ctrl.getDb().updateUser(g.user.userId, update)

	res = { "message": "Done" }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/payments/pay/<plan>', methods=('POST',))
@auth.login_required
def create_payment_intent(plan):
	body = getJson()
	
	email = body.get("email")
	if not email:
		res = { "message": "Bad request." }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	if plan == "hgpro_standard":
		intent = stripe.PaymentIntent.create(
			api_key=ctrl.app.config['STRIPE_API_KEY'],
			amount=100,
			currency="usd",
			receipt_email=email
		)
	elif plan == "hgpro_professional":
		intent = stripe.PaymentIntent.create(
			api_key=ctrl.app.config['STRIPE_API_KEY'],
			amount=100,
			currency="usd",
			receipt_email=email
		)
	else:
		res = { "message": "Plan not found." }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	res = { "client_secret": intent["client_secret"] }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/payments/subscribe/<plan>', methods=('POST',))
@auth.login_required
def create_subscription(plan):
	body = getJson()

	user_id = body.get("user_id")
	name = body.get("name")
	address = body.get("address")
	email = body.get("email")
	payment_method = body.get("payment_method")
	level = body.get("level")

	user = ctrl.getDb().getUser(user_id)

	if not user or not user_id or not email or not payment_method or not address or not name or not plan:
		res = { "message": "Bad request." }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	# Create a subscription and attach the customer to it
	if not (
		"products" in user and 
		plan in user["products"] and 
		user["products"][plan]["level"] >= level
	):
		# Delete old subscription
		if "products" in user and plan in user["products"]:
			sub_id = user["products"][plan]["sub_id"]
			stripe.Subscription.delete(
				sub_id,
				api_key=ctrl.app.config['STRIPE_API_KEY']
			)

		# Create a new Customer and attach the default PaymentMethod
		customer = stripe.Customer.create(
			api_key=ctrl.app.config['STRIPE_API_KEY'],
			payment_method=payment_method,
			email=email,
			name=name,
			address=address,
			invoice_settings={
				"default_payment_method": payment_method
			},
			metadata={
				"user_id": user_id
			}
		)

		subscription = None
		try:
			if plan == "hgpro":
				price = None
				if level == 0:
					# price = "price_1JRnMIBtSFeX56k3stqYyEj6" # TEST MODE PRICE
					# price = "price_1JZUipBtSFeX56k35d2p43oV" # PROD TEST PRICE
					price = "price_1Jlj7kBtSFeX56k3EJ7SVYrX" # REAL PRICE
				elif level == 1:
					# price = "price_1JRnNHBtSFeX56k3jzh0rp9h" # TEST MODE PRICE
					# price = "price_1JZUipBtSFeX56k35d2p43oV" # PROD TEST PRICE
					price = "price_1Jlj6tBtSFeX56k39TreiwAp" # REAL PRICE
				elif level == 2:
					# price = "price_1JRnNHBtSFeX56k3jzh0rp9h" # TEST MODE PRICE
					# price = "price_1JZUipBtSFeX56k35d2p43oV" # PROD TEST PRICE
					price = "price_1JbKKaBtSFeX56k3MuRj7ecT" # REAL PRICE
				elif level == 4:
					# price = "price_1JRnNHBtSFeX56k3jzh0rp9h" # TEST MODE PRICE
					price = "price_1JZUipBtSFeX56k35d2p43oV" # PROD TEST PRICE
					# price = "price_1JbKJWBtSFeX56k3783LhUbi" # REAL PRICE
				elif level == 5:
					# price = "price_1JRnNHBtSFeX56k3jzh0rp9h" # TEST MODE PRICE
					price = "price_1JZUipBtSFeX56k35d2p43oV" # PROD TEST PRICE
					# price = "price_1JQUbsBtSFeX56k3aw6fw9Nr" # REAL PRICE

				if price is not None:
					if address["country"] == "AU":
						subscription = stripe.Subscription.create(
							api_key=ctrl.app.config['STRIPE_API_KEY'],
							customer=customer["id"],
							items=[{
								"price": price,
								"tax_rates": ["txr_1JQVr9BtSFeX56k3i6L9BoBZ"] 
							}],
							expand=["latest_invoice.payment_intent"]
						)
					else:
						subscription = stripe.Subscription.create(
							api_key=ctrl.app.config['STRIPE_API_KEY'],
							customer=customer["id"],
							items=[{
								"price": price
							}],
							expand=["latest_invoice.payment_intent"]
						)
			
				else:
					res = { "message": "Plan not found." }
					return Response(
						json.dumps(res, indent=2),
						status=400, content_type='application/json'
					)

			else:
				res = { "message": "Plan not found." }
				return Response(
					json.dumps(res, indent=2),
					status=400, content_type='application/json'
				)

		except CardError as e:
			print(traceback.format_exc())
			res = { "message": f"Card Error: {str(e)}" }
			return Response(
				json.dumps(res, indent=2),
				status=400, content_type='application/json'
			)

		# Add Purchase Verification
		if "products" in user:
			update = { "products": user["products"] }

			if not plan in update:
				update["products"][plan] = {}

			update["products"][plan]["level"] = level
			update["products"][plan]["sub_id"] = subscription["id"]
		else:
			update = { 
				"products": { 
					plan: {
						"level": level,
						"sub_id": subscription["id"]
					}  
				} 
			}
		
		update = ctrl.getDb().updateUser(user_id, update)

		# Create Strategy
		strategy_info = {
			"name": "",
			"package": subscription_info[plan][level]["product"] + ".v1_0_0"
		}

		if level == 2:
			strategy_info["account_limit"] = 5
			strategy_info["bank_limit"] = 100000

		account = ctrl.accounts.getAccount(user_id)
		strategy_id = account.createStrategy(strategy_info)
	
	else:
		res = { "message": "Product already purchased." }
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	status = subscription['latest_invoice']['payment_intent']['status'] 
	client_secret = subscription['latest_invoice']['payment_intent']['client_secret']

	res = {'status': status, 'client_secret': client_secret}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/payments/subscribe/<plan>', methods=('DELETE',))
@auth.login_required
def cancel_subscription(plan):
	user = ctrl.getDb().getUser(g.user.userId)

	if "products" in user:
		if plan in user["products"]:
			sub_id = user["products"][plan]["sub_id"]
			stripe.Subscription.delete(
				sub_id,
				api_key=ctrl.app.config['STRIPE_API_KEY']
			)

			update = { "products": user["products"] }
			del update["products"][plan]
			ctrl.getDb().updateUser(g.user.userId, update)

			res = { "message": "Success" }
			return Response(
				json.dumps(res, indent=2),
				status=200, content_type='application/json'
			)

	res = { "message": "Product not found." }
	return Response(
		json.dumps(res, indent=2),
		status=400, content_type='application/json'
	)


@bp.route('/payments/subscribe', methods=('GET',))
@auth.login_required
def get_subscriptions():
	user = ctrl.getDb().getUser(g.user.userId)

	result = []
	if "products" in user:
		for plan in user["products"]:
			level = user["products"][plan]["level"]
			result.append({ "name": subscription_info[plan][level]["name"], "plan": plan })
		
	res = { "subscriptions": result }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)



@bp.route('/tests/broker/disconnect/<strategy_id>/<broker_id>', methods=('POST',))
def disconnect_broker(strategy_id, broker_id):
	user_id, _ = key_or_login_required(strategy_id, AccessLevel.LIMITED)
	account = ctrl.accounts.getAccount(user_id)
	account.startStrategy(strategy_id)
	broker = account.brokers.get(broker_id)

	broker.disconnectBroker()

	res = { "message": "done" }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/messages', methods=('GET',))
@auth.login_required
def get_all_messages():
	messages = ctrl.getDb().getAllMessages()
	user = ctrl.getDb().getUser(g.user.userId)

	result = []
	for m in messages:
		if "all" in m["message_users"] or g.user.userId in m["message_users"]:
			result.append(m)
			if "messages_read" in user:
				if m["message_id"] in user["messages_read"]:
					m["unread"] = False
					continue
			m["unread"] = True

	result = sorted(result, key=lambda x: parser.parse(x["message_date"]), reverse=True)
	res = { "messages": result }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/messages', methods=('POST',))
def create_message():
	body = getJson()
	title = body.get("message_title")
	date = datetime.utcnow().isoformat()
	message_body = body.get("message_body")
	users = body.get("message_users")

	message_id = ctrl.getDb().createMessage(title, date, message_body, users)

	res = { "message_id": message_id }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/messages/<message_id>', methods=('GET',))
def get_message(message_id):
	message = ctrl.getDb().getMessage(message_id)

	res = { "message": message }
	if message is not None:
		return Response(
			json.dumps(res, indent=2),
			status=200, content_type='application/json'
		)
	else:
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)


@bp.route('/messages/<message_id>', methods=('PUT',))
def update_message(message_id):
	body = getJson()
	title = body.get("message_title")
	date = datetime.utcnow().isoformat()
	message_body = body.get("message_body")
	users = body.get("message_users")

	update = {}
	if title is not None:
		update["message_title"] = title
	if date is not None:
		update["message_date"] = date
	if message_body is not None:
		update["message_body"] = message_body
	if users is not None:
		update["message_users"] = users

	ctrl.getDb().updateMessage(message_id, update)

	res = { "message_id": message_id }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/messages/<message_id>', methods=('DELETE',))
def delete_message(message_id):
	ctrl.getDb().deleteMessage(message_id)

	res = { "message": "done" }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/messages/user/<message_id>', methods=('PUT',))
@auth.login_required
def mark_message_read(message_id):
	user = ctrl.getDb().getUser(g.user.userId)

	update = None
	if "messages_read" in user:
		if not message_id in user["messages_read"]:
			update = { "messages_read": user["messages_read"] + [message_id] }	
	else:
		update = { "messages_read": [message_id] }

	if update is not None:
		ctrl.getDb().updateUser(g.user.userId, update)

	res = { "message": "done" }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/variables/<variable_name>', methods=('GET',))
def get_master_variable(variable_name):
	result = ctrl.getDb().getVariable(variable_name)

	res = { variable_name: result }
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)
