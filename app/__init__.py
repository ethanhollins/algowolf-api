import os
import platform
import json
import traceback
import sys
from flask import Flask, Response
from flask_cors import CORS
from app.error import (
	AccountException, AuthorizationException, BrokerException, 
	OrderException
)

def create_app(test_config=None):

	# Create and configure app
	instance_path = os.path.join(os.path.abspath(os.getcwd()), 'instance')
	app = Flask(__name__, instance_relative_config=True, instance_path=instance_path)

	app.config.from_mapping(
		SECRET_KEY='dev',
		BROKERS=os.path.join(app.instance_path, 'brokers.json')
	)

	if test_config is None:
		# load the instance config, if it exists, when not testing
		app.config.from_pyfile(os.path.join(app.instance_path, 'config.py'), silent=True)
	else:
		# load the test config if passed in
		app.config.from_mapping(test_config)

	# Ensure the instance folder exists
	try:
		os.makedirs(app.instance_path)
	except OSError:
		pass

	# Instance assertions
	assert 'STREAM_URL' in app.config, 'STREAM_URL not found.'
	assert 'ORIGINS' in app.config, 'ORIGINS not found.'

	cors = CORS(
		app, resources={r"/*": {"origins": app.config['ORIGINS']}}, 
		supports_credentials=True,
		allow_headers=["Authorization", "Content-Type", "Accept"]
	)

	if 'DEBUG' in app.config:
		app.debug = app.config['DEBUG']

	# Hello World ept
	@app.route('/')
	def hello():
		res = { 'message': 'Hello World' }
		return Response(json.dumps(res, indent=2), status=200, content_type='application/json')

	@app.errorhandler(404)
	def page_not_found(e):
		res = {
			'error': 'NotFound',
			'message': str(e).strip('404 Not Found: ')
		}
		return Response(
			json.dumps(res, indent=2), 
			status=404, content_type='application/json'
		)

	@app.errorhandler(405)
	def internal_server_error(e):
		res = {
			'error': 'MethodNotAllowed',
			'message': 'No message available.'
		}
		return Response(
			json.dumps(res, indent=2), 
			status=405, content_type='application/json'
		)

	@app.errorhandler(AccountException)
	@app.errorhandler(AuthorizationException)
	@app.errorhandler(BrokerException)
	@app.errorhandler(OrderException)
	@app.errorhandler(400)
	def internal_server_error(e):
		if isinstance(e, AccountException):
			res = {
				'error': 'AccountException',
				'message': str(e)
			}
		elif isinstance(e, AuthorizationException):
			res = {
				'error': 'AuthorizationException',
				'message': str(e)
			}
		elif isinstance(e, BrokerException):
			res = {
				'error': 'BrokerException',
				'message': str(e)
			}
		elif isinstance(e, OrderException):
			res = {
				'error': 'OrderException',
				'message': str(e)
			}
		else:
			res = {
				'error': 'BadRequest',
				'message': str(e)
			}
		return Response(
			json.dumps(res, indent=2), 
			status=400, content_type='application/json'
		)

	@app.errorhandler(500)
	@app.errorhandler(Exception)
	def internal_server_error(e):
		print(traceback.format_exc())
		res = {
			'error': 'InternalServerError',
			'message': 'No message available.'
		}
		return Response(
			json.dumps(res, indent=2), 
			status=500, content_type='application/json'
		)

	from app import controller
	controller.initController(app)

	from app import auth
	auth.ctrl = controller.ctrl
	app.register_blueprint(auth.bp)

	from app import v1
	v1.ctrl = controller.ctrl
	app.register_blueprint(v1.bp)
	app.add_url_rule('/', endpoint='index')

	return app

app = create_app()

