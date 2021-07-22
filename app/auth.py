import functools
import json, jwt
import requests

from flask import (
	Blueprint, Response, abort, current_app, g, request, session, url_for, redirect
)
from werkzeug.security import check_password_hash, generate_password_hash
from app import tradelib as tl
from app.error import AccountException, BrokerException

bp = Blueprint('auth', __name__)

def start_session(user_id):
	# Check if account unintialized
	acc = ctrl.accounts.getAccount(user_id)
	return acc

@bp.route('/register', methods=('POST',))
def register():
	body = request.get_json(force=True)
	first_name = body.get('first_name')
	last_name = body.get('last_name')
	email = body.get('email')
	password = body.get('password')
	notify_me = body.get('notify_me')
	db = ctrl.getDb()

	if not first_name:
		error = {
			'error': 'ValueError',
			'message': 'First name is required.'
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='application/json'
		)

	elif not last_name:
		error = {
			'error': 'ValueError',
			'message': 'Last name is required.'
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='application/json'
		)

	elif not email:
		error = {
			'error': 'ValueError',
			'message': 'Email is required.'
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='application/json'
		)

	elif not password:
		error = {
			'error': 'ValueError',
			'message': 'Password is required.'
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='application/json'
		)

	elif db.getUserByEmail(email) is not None:
		error = {
			'error': 'ValueError',
			'message': 'Email {} is already registered.'.format(email)
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='applcation/json'
		)

	user_id = db.registerUser(first_name, last_name, email, generate_password_hash(password), notify_me)
	msg = {
		'user_id': user_id
	}
	return msg, 200


@bp.route('/login', methods=('POST',))
def login():
	body = request.get_json(force=True)
	email = body.get('email')
	password = body.get('password')
	remember_me = body.get('remember_me')
	db = ctrl.getDb()
	
	user = db.getUserByEmail(email)

	if user is None:
		error = {
			'error': 'AuthorizationException',
			'message': 'Incorrect email.'
		}
		return error, 403

	elif not check_password_hash(user['password'], password):
		error = {
			'error': 'AuthorizationException',
			'message': 'Incorrect password.'
		}
		return error, 403

	# Start User session in memory
	user_id = user.get('user_id')
	account = start_session(user_id)

	if remember_me:
		token = account.generatePermanentToken()
	else:
		token = account.generateToken()

	msg = {
		'user_id': user_id,
		'token': token
	}
	return msg, 200

@bp.route('/logout', methods=('POST',))
def logout():
	user_id = session.get('user_id')
	msg = {}
	if user_id:
		session.clear()
		msg = {
			'user_id': user_id
		}
	return Response(
		json.dumps(msg, indent=2), 
		status=200, content_type='application/json'
	)


def decode_auth_token():
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
				return jwt.decode(key[1], current_app.config['SECRET_KEY'], algorithms=['HS256']), 200
			except jwt.ExpiredSignatureError:
				error = {
					'error': 'AuthorizationException',
					'message': 'Authorization key Expired.'
				}
				return error, 403
			except jwt.InvalidTokenError:
				error = {
					'error': 'AuthorizationException',
					'message': 'Invalid authorization key.'
				}
				return error, 403
			except jwt.exceptions.DecodeError:
				error = {
					'error': 'AuthorizationException',
					'message': 'Invalid authorization key.'
				}
				return error, 403

	error = {
		'error': 'AuthorizationException',
		'message': 'Invalid authorization key.'
	}
	return error, 403


def check_login():
	if g.get('user') is None:
		error = {
			'error': 'AuthorizationException',
			'message': 'Must be logged in.'
		}
		return error, 403
	return g.user.userId, 200

def login_required(view):
	@functools.wraps(view)
	def wrapped_view(*args, **kwargs):
		res, status = check_login()
		if status != 200:
			return Response(
				json.dumps(res, indent=2),
				status=status, content_type='application/json'
			)
		return view(*args, **kwargs)
	return wrapped_view

# @bp.before_app_request
# def load_logged_in_user():
# 	user_id = session.get('user_id')
# 	if user_id is None:
# 		g.user = None
# 	else:
# 		try:
# 			g.user = ctrl.accounts.getAccount(user_id)
# 		except AccountException:
# 			session.clear()

@bp.before_app_request
def load_logged_in_user():
	token, status = decode_auth_token()
	if status == 200:
		try:
			g.user = ctrl.accounts.getAccount(token.get('sub'))
		except AccountException:
			pass
	else:
		g.user = None


@bp.route('/authorize', methods=('POST',))
@login_required
def check_auth():
	user_info = ctrl.getDb().getUser(g.user.userId)

	res = {
		'user_id': g.user.userId,
		'first_name': user_info.get('first_name'),
		'last_name': user_info.get('last_name')
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/broker', methods=('GET',))
@login_required
def get_all_brokers():
	res = g.user.getAllBrokers()
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/broker', methods=('POST',))
@login_required
def create_broker():
	body = request.get_json(force=True)
	print(body)
	broker_id = body.get('broker_id')
	name = body.get('name')
	broker_name = body.get('broker')

	if broker_id is None or name is None or broker_name is None:
		raise BrokerException('Invalid data submitted.')

	del body['broker_id']
	del body['name']
	del body['broker']

	res = g.user.createBroker(broker_id, name, broker_name, **body)
	if res is None:
		raise BrokerException('Invalid data submitted.')

	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/broker', methods=('PUT',))
@login_required
def update_broker():
	body = request.get_json(force=True)
	broker_id = body.get('broker_id')

	if broker_id is None:
		raise BrokerException('Invalid data submitted.')

	del body['broker_id']

	res = g.user.updateBroker(broker_id, body)
	if res is None:
		raise BrokerException('Invalid data submitted.')

	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/broker/<name>', methods=('GET',))
@login_required
def get_broker(name):
	res = g.user.getBroker(name)
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/broker/<old_name>/<new_name>', methods=('PUT',))
@login_required
def change_broker_name(old_name, new_name):
	res = g.user.changeBrokerName(old_name, new_name)
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/broker/<broker_id>', methods=('DELETE',))
@login_required
def delete_broker(broker_id):
	# Check no scripts are running on broker
	if g.user.account.isAnyScriptRunning():
		res = {
			'error': 'BrokerException',
			'message': 'Please stop all scripts on this broker before deleting.'
		}
		return Response(
			json.dumps(res, indent=2),
			status=400, content_type='application/json'
		)

	res = {
		'name': g.user.deleteBroker(broker_id)
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


@bp.route('/auth/spotware', methods=('GET',))
@login_required
def spotware_broker_auth():
	code = request.args.get('code')
	broker_id = request.args.get('broker_id')
	print('SPOTWARE BROKER AUTH')
	print(request.args)

	broker_id = ''
	if not code is None:
		res = requests.get(
			'https://connect.spotware.com/apps/token',
			params={
				'grant_type': 'authorization_code',
				'code': code,
				'redirect_uri': current_app.config['SPOTWARE_REDIRECT'],
				'client_id': current_app.config['SPOTWARE_CLIENT_ID'],
				'client_secret': current_app.config['SPOTWARE_CLIENT_SECRET']
			}
		)

		if res.status_code == 200:
			result = res.json()

			print(result)
			print(result.get('errorCode') is None)
			if result.get('errorCode') is None:
				access_token = result.get('accessToken')
				refresh_token = result.get('refreshToken')
				token_type = result.get('tokenType')
				expires_in = result.get('expiresIn')

				# Add broker to account
				broker_id = g.user.generateId()

				props = {
					"access_token": access_token,
					"refresh_token": refresh_token,
				}
				broker_id = g.user.createBroker(broker_id, 'My Broker', 'spotware', **props)


	res_ = { 'broker_id': broker_id }
	return Response(
		json.dumps(res_, indent=2),
		status=res.status_code, content_type='application/json'
	)
