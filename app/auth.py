import functools
import json, jwt

from flask import (
	Blueprint, Response, abort, current_app, g, request, session, url_for
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
	username = body.get('username')
	password = body.get('password')
	db = ctrl.getDb()

	if not username:
		error = {
			'error': 'ValueError',
			'message': 'Username is required.'
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='applcation/json'
		)

	elif not password:
		error = {
			'error': 'ValueError',
			'message': 'Password is required.'
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='applcation/json'
		)

	elif db.getUserByUsername(username) is not None:
		error = {
			'error': 'ValueError',
			'message': 'Username {} is already registered.'.format(username)
		}
		return Response(
			json.dumps(error, indent=2), 
			status=400, content_type='applcation/json'
		)

	user_id = db.registerUser(username, generate_password_hash(password))
	msg = {
		'user_id': user_id
	}
	return msg, 200


@bp.route('/login', methods=('POST',))
def login():
	body = request.get_json(force=True)
	username = body.get('username')
	password = body.get('password')
	db = ctrl.getDb()
	
	user = db.getUserByUsername(username)

	if user is None:
		error = {
			'error': 'AuthorizationException',
			'message': 'Incorrect username.'
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

	# session.clear()
	# session['user_id'] = user_id
	# session.permanent = True
	msg = {
		'user_id': user_id,
		'token': account.generateToken()
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
	res = {
		'user_id': g.user.userId
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
	name = body.get('name')
	broker_name = body.get('broker')

	if name is None:
		raise BrokerException('Invalid data submitted.')
	if broker_name is None:
		raise BrokerException('Invalid data submitted.')

	del body['name']
	del body['broker']

	res = g.user.createBroker(name, broker_name, **body)
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


@bp.route('/broker/<name>', methods=('DELETE',))
@login_required
def delete_broker(name):
	res = {
		'name': g.user.deleteBroker(name)
	}
	return Response(
		json.dumps(res, indent=2),
		status=200, content_type='application/json'
	)


