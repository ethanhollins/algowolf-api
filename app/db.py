import shortuuid
import boto3
import io
import json, csv, gzip, collections
import string, random
import jwt
import pandas as pd
import dateutil.parser
import time
import traceback
from copy import copy
from datetime import datetime
from app import tradelib as tl
from app.error import BrokerException
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from threading import Thread

class Database(object):

	def __init__(self, ctrl, env):
		self._job_queue = []

		self.ctrl = ctrl
		self._generate_db()
		self._generate_s3()
		if env == 'development':
			self.userTable = self._generate_table('algowolf-users-dev')
			self.scriptTable = self._generate_table('algowolf-scripts-dev')
			self.holygrailAccessTable = self._generate_table('algowolf-holygrail-access')
			self.holygrailTokenTable = self._generate_table('algowolf-holygrail-tokens')
			self.resetPasswordTokenTable = self._generate_table('algowolf-reset-password-tokens')
			self.strategyBucketName = 'algowolf-strategies-dev'
			self.scriptBucketName = 'algowolf-scripts-dev'
		else:
			self.userTable = self._generate_table('algowolf-users')
			self.scriptTable = self._generate_table('algowolf-scripts')
			self.holygrailAccessTable = self._generate_table('algowolf-holygrail-access')
			self.holygrailTokenTable = self._generate_table('algowolf-holygrail-tokens')
			self.resetPasswordTokenTable = self._generate_table('algowolf-reset-password-tokens')
			self.strategyBucketName = 'algowolf-strategies'
			self.scriptBucketName = 'algowolf-scripts-dev'

		self.prodUserTable = self._generate_table('algowolf-users')

		self.analyticsTable = self._generate_table('algowolf-analytics')
		self.emailsTable = self._generate_table('algowolf-emails')
		self.priceDataBucketName = 'brokerlib-prices'

		Thread(target=self._handle_jobs).start()


	'''
	Utilities
	'''

	def addAsyncJob(self, func, *args, **kwargs):
		self._job_queue.append((func, args, kwargs))


	def _handle_jobs(self):
		while True:
			if len(self._job_queue):
				i = self._job_queue[0]
				try:
					result = i[0](*i[1], **i[2])
				except Exception:
					print(traceback.format_exc(), flush=True)
				finally:
					del self._job_queue[0]
			time.sleep(0.1)


	def generateId(self):
		letters = string.ascii_uppercase + string.digits
		return ''.join(random.choice(letters) for i in range(6))

	# Dynamo DB
	def _generate_db(self):
		self._db_client = boto3.resource(
			'dynamodb',
			region_name='ap-southeast-2'
		)

	def _generate_table(self, table_name):
		return self._db_client.Table(table_name)

	# S3 Storage
	def _generate_s3(self):
		self._s3_client = boto3.client('s3')
		self._s3_res = boto3.resource('s3')

	def _convert_to_decimal(self, row):
		if isinstance(row, dict):
			for k in row:
				row[k] = self._convert_to_decimal(row[k])
		elif (not isinstance(row, str) and
			isinstance(row, collections.Iterable)):
			row = list(row)
			for i in range(len(row)):
				row[i] = self._convert_to_decimal(row[i])
		elif isinstance(row, float):
			return Decimal(str(float(row)))
			
		return row

	def _convert_to_float(self, row):
		if isinstance(row, dict):
			for k in row:
				row[k] = self._convert_to_float(row[k])
		elif (not isinstance(row, str) and
			isinstance(row, collections.Iterable)):
			row = list(row)
			for i in range(len(row)):
				row[i] = self._convert_to_float(row[i])
		elif isinstance(row, Decimal):
			return float(row)

		return row

	def _flat_dump(self, obj, indent=2):
		assert isinstance(obj, dict)
		if not len(obj):
			return '{}'

		result = '{\n'
		new_l = ',\n'
		for k, v in obj.items():
			result += ' '*indent + f'\"{k}\": '
			if isinstance(v, list):
				if len(v):
					result += '[\n'
					for i in v:
						result += ' '*indent*2+json.dumps(i)+new_l
					result = result.strip(new_l)
					result += '\n'+' '*indent+']'+new_l
				else:
					result += '[]'+new_l
			else:
				result += json.dumps(v) + new_l
		result = result.strip(new_l)
		result += '\n}'

		return result

	'''
	User DB Functions
	'''

	def registerUser(self, first_name, last_name, email, password, notify_me):
		user_id = shortuuid.uuid()

		res = self.userTable.put_item(
			Item={
				'user_id': user_id,
				'first_name': first_name,
				'last_name': last_name,
				'email': email,
				'password': password,
				'notify_me': notify_me,
				'email_confirmed': False,
				'beta_access': False,
				'brokers': {},
				'strategies': {},
				'metadata': {
					'current_strategy': '',
					'open_strategies': []
				},
				'registration_date': datetime.utcnow().isoformat(),
				'analytics': {}
			}
		)

		# self.generateHolyGrailStrategy(user_id)

		return user_id


	def getAllUsers(self):
		res = self.userTable.scan()
		data = res['Items']

		while 'LastEvaluatedKey' in res:
			res = self.userTable.scan(ExclusiveStartKey=res['LastEvaluatedKey'])
			data.extend(res['Items'])

		return self._convert_to_float(data)


	def getUser(self, user_id):
		res = self.userTable.get_item(
			Key={ 'user_id': user_id }
		)
		if res.get('Item'):
			return self._convert_to_float(res['Item'])
		else:
			return None

	def getUserByUsername(self, username):
		res = self.userTable.scan(
			FilterExpression=Key('username').eq(username)
		)
		if res.get('Items') and len(res.get('Items')):
			return self._convert_to_float(res['Items'][0])
		else:
			return None

	def getUserByEmail(self, email):
		res = self.userTable.scan(
			FilterExpression=Key('email').eq(email)
		)
		if res.get('Items') and len(res.get('Items')):
			return self._convert_to_float(res['Items'][0])
		else:
			return None

	def updateUser(self, user_id, update):
		update_values = self._convert_to_decimal(
			dict([tuple([':{}'.format(i[0]), i[1]])
					for i in update.items()])
		)

		update_exp = ('set ' + ' '.join(
			['{} = :{},'.format(k, k) for k in update.keys()]
		))[:-1]

		res = self.userTable.update_item(
			Key={
				'user_id': user_id
			},
			UpdateExpression=update_exp,
			ExpressionAttributeValues=update_values,
			ReturnValues="UPDATED_NEW"
		)
		return True

	def deleteUser(self, user_id):
		res = self.userTable.delete_item(
			Key={
				'user_id': user_id,
			}
		)
		return True

	'''
	Analytics
	'''

	def getVisitors(self):
		res = self.analyticsTable.get_item(
			Key={ 'subject': 'visitors' }
		)
		if res.get('Item'):
			return self._convert_to_float(res['Item'])
		else:
			return None


	def countDailyVisitor(self):
		visitors = self.getVisitors()

		if visitors is None:
			visitors = {}

		if not 'daily' in visitors:
			visitors['daily'] = 0

		if not 'total_daily' in visitors:
			visitors['total_daily'] = 0

		if 'current_date' in visitors:
			date = dateutil.parser.isoparse(visitors['current_date'])
			if date.day != datetime.utcnow().day:
				visitors['daily'] = 0
				visitors['current_date'] = datetime.utcnow().isoformat()
		else:
			visitors['current_date'] = datetime.utcnow().isoformat()

		res = self.analyticsTable.update_item(
			Key={
				'subject': 'visitors'
			},
			UpdateExpression='set daily = :d, total_daily = :r, current_date = :t',
			ExpressionAttributeValues={
				':d': self._convert_to_decimal(visitors['daily'] + 1),
				':r': self._convert_to_decimal(visitors['total_daily'] + 1),
				':t': visitors['current_date']
			},
			ReturnValues="UPDATED_NEW"
		)

		return visitors['daily'] + 1


	def countUniqueVisitor(self):
		visitors = self.getVisitors()

		if visitors is None:
			visitors = {}

		if not 'unique_visitors' in visitors:
			visitors['unique_visitors'] = 0

		res = self.analyticsTable.update_item(
			Key={
				'subject': 'visitors'
			},
			UpdateExpression='set unique_visitors = :u',
			ExpressionAttributeValues={
				':u': self._convert_to_decimal(visitors['unique_visitors'] + 1)
			},
			ReturnValues="UPDATED_NEW"
		)

		return visitors['unique_visitors'] + 1


	def subscribeEmail(self, item):
		self.emailsTable.put_item(
			Item={
				'email': str(item.get('email')),
				'name': str(item.get('name'))
			}
		)

		return item.get('email')


	'''
	Strategy DB Functions
	'''

	def getStrategy(self, user_id, strategy_id):
		user = self.getUser(user_id)
		if user is None:
			return None

		return user['strategies'].get(strategy_id)
	
	def createStrategy(self, user_id, strategy):
		
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False

		script_id = strategy['package'].split('.')[0]
		# Check package not already in use
		for strategy_id in user['strategies']:
			if user['strategies'][strategy_id].get('package').split('.')[0] == script_id:
				return False

		# Make sure id is unique
		strategy_id = self.generateId()
		while strategy_id in user['strategies']:
			strategy_id = self.generateId()
		# Add strategy to db
		user['strategies'][strategy_id] = strategy
		user['metadata']['current_strategy'] = strategy_id
		if strategy_id not in user['metadata']['open_strategies']:
			user['metadata']['open_strategies'].append(strategy_id)

		# Update changes
		update = { 'strategies': user.get('strategies'), 'metadata': user.get('metadata') }
		result = self.updateUser(user_id, update)
		
		# Add strategy to storage
		self.initStrategyStorage(user_id, strategy_id, strategy['name'], script_id)
		
		return strategy_id

	def updateStrategy(self, user_id, strategy_id, update):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False

		# Add strategy to db
		if user['strategies'].get(strategy_id) is None:
			return False

		user['strategies'][strategy_id] = update
		# Update changes
		update = { 'strategies': user.get('strategies') }
		result = self.updateUser(user_id, update)
		return result

	def deleteStrategy(self, user_id, strategy_id):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False

		# Add strategy to db
		if user['strategies'].get(strategy_id) is None:
			return False

		del user['strategies'][strategy_id]
		# Update changes
		update = { 'strategies': user.get('strategies') }
		result = self.updateUser(user_id, update)

		# Delete strategy storage
		self.deleteStrategyStorage(user_id, strategy_id)

		return True

	def getKeys(self, user_id, strategy_id):
		strategy = self.getStrategy(user_id, strategy_id)
		if strategy is None:
			return None

		return strategy.get('keys')

	def createKey(self, user_id, strategy_id, key):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False
		elif user['strategies'].get(strategy_id) is not None:
			if user['strategies'][strategy_id].get('keys') is None:
				user['strategies'][strategy_id]['keys'] = [key]
			else:
				user['strategies'][strategy_id]['keys'].append(key)
		
		# Update changes
		update = { 'strategies': user.get('strategies') }
		result = self.updateUser(user_id, update)
		return result

	def deleteKey(self, user_id, strategy_id, key):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False
		elif user['strategies'].get(strategy_id) is not None:
			if user['strategies'][strategy_id].get('keys') is not None:
				if key in user['strategies'][strategy_id]['keys']:
					idx = user['strategies'][strategy_id]['keys'].index(key)
					del user['strategies'][strategy_id]['keys'][idx]
		
		# Update changes
		update = { 'strategies': user.get('strategies') }
		result = self.updateUser(user_id, update)
		return result

	def getBroker(self, user_id, name):
		user = self.getUser(user_id)
		if user is None:
			return None

		broker_key = user['brokers'].get(name)
		if broker_key is None:
			return None

		return jwt.decode(broker_key, self.ctrl.app.config['SECRET_KEY'], algorithms=['HS256'])

	def createBroker(self, user_id, broker_id, name, broker_name, props):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			raise BrokerException('User not found.')

		print(f"[db.createBroker] 1")
		# Check if key in use
		for v in user['brokers'].values():
			v = jwt.decode(
				v, self.ctrl.app.config['SECRET_KEY'], 
				algorithms=['HS256']
			)
			if (
				v.get('broker') == broker_name and 
				(v.get('key') is not None and
					v.get('key') == props.get('key'))
			):
				return None
		print(f"[db.createBroker] 2")

		props.update({
			'name': name,
			'broker': broker_name
		})

		if broker_name == tl.broker.IG_NAME:
			# IG Validation
			if props.get('key') is None:
				raise BrokerException('Invalid data submitted.')
			if props.get('username') is None:
				raise BrokerException('Invalid data submitted.')
			elif props.get('password') is None:
				raise BrokerException('Invalid data submitted.')
			elif props.get('is_demo') is None:
				raise BrokerException('Invalid data submitted.')

			# Run broker API call check
			dummy_broker = tl.broker.IG(
				self.ctrl, props.get('username'), props.get('password'),
				props.get('key'), props.get('is_demo'), is_dummy=True
			)
			accounts = dummy_broker.getAllAccounts()

			if accounts is None:
				raise BrokerException('Unable to connect to broker.')

			# Set Accounts Information
			props['accounts'] = {
				account_id: { 'active': True, 'nickname': '' }
				for account_id in accounts
			}
		
		elif broker_name == tl.broker.OANDA_NAME:
			print(props)
			if props.get('key') is None:
				raise BrokerException('Invalid data submitted.')
			if props.get('is_demo') is None:
				raise BrokerException('Invalid data submitted.')
			print(f"[db.createBroker] 3")

			# Run broker API call check
			dummy_broker = tl.broker.Oanda(
				self.ctrl, props.get('key'), props.get('is_demo'), broker_id=broker_id, is_dummy=True
			)
			accounts = dummy_broker.getAllAccounts()
			print(f"[db.createBroker] {accounts}")

			if accounts is None:
				raise BrokerException('Unable to connect to broker.')

			# Set Accounts Information
			props['accounts'] = {
				account_id: { 'active': True, 'nickname': '' }
				for account_id in accounts
			}

		elif broker_name == tl.broker.SPOTWARE_NAME:
			if props.get('access_token') is None:
				raise BrokerException('Invalid data submitted.')
			# if props.get('is_demo') is None:
			# 	raise BrokerException('Invalid data submitted.')

			print(f'[createBroker] creating dummy broker: {props}')
			# Run broker API call check
			dummy_broker = tl.broker.Spotware(
				self.ctrl, props.get('is_demo'), props.get('access_token'), broker_id=broker_id, is_dummy=True
			)
			accounts = dummy_broker.getAllAccounts()
			dummy_broker.deleteChild()

			if accounts is None:
				raise BrokerException('Unable to connect to broker.')

			# Set Accounts Information
			props['is_demo'] = True

			props['accounts'] = {}
			for account in accounts:
				props['accounts'][account['id']] = { 
					'active': True, 
					'nickname': '', 
					'is_demo': account['is_demo'],
					'account_id': account.get('account_id'), 
					'broker': account.get('broker')
				}

			# Check if account is already being used
			# for v in user['brokers'].values():
			# 	v = jwt.decode(
			# 		v, self.ctrl.app.config['SECRET_KEY'], 
			# 		algorithms=['HS256']
			# 	)
			# 	if v.get('accounts'):
			# 		for x in v['accounts']:
			# 			if str(account['id']) == str(x):
			# 				raise BrokerException('One or more accounts is already being used. Please delete the broker container that account and try again.')

		elif broker_name == tl.broker.IB_NAME:
			print(f'ADDING IB: {broker_id}, {props}')
			account = self.ctrl.accounts.getAccount(user_id)
			ib_broker = tl.broker.IB(
				self.ctrl, broker_id=broker_id, user_account=account
			)

			account.brokers[broker_id] = ib_broker
			props['accounts'] = {}
			props['broker'] = tl.broker.IB_NAME

		elif broker_name == tl.broker.DUKASCOPY_NAME:
			print(f'ADDING Dukscopy: {broker_id}, {props}')
			account = self.ctrl.accounts.getAccount(user_id)
			dukascopy_broker = tl.broker.Dukascopy(
				self.ctrl, broker_id=broker_id, user_account=account
			)

			account.brokers[broker_id] = dukascopy_broker
			props['username'] = None
			props['password'] = None
			props['is_demo'] = None
			props['accounts'] = {}
			props['broker'] = tl.broker.DUKASCOPY_NAME
			props['complete'] = False

		# Upload new broker info
		key = jwt.encode(props, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')
		print(f'[createBroker] set broker: {broker_id}, {key}')
		user['brokers'][broker_id] = key

		# Update changes
		update = { 'brokers': user.get('brokers') }
		result = self.updateUser(user_id, update)
		return broker_id


	def updateBroker(self, user_id, broker_id, props):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			raise BrokerException('User not found.')


		# Upload new broker info
		if 'brokers' in user and broker_id in user['brokers']:
			prev_broker = jwt.decode(user['brokers'][broker_id], self.ctrl.app.config['SECRET_KEY'], algorithms=['HS256'])
			new_broker = { **prev_broker, **props }

			print(f'NEW BROKER: {new_broker}')
			key = jwt.encode(new_broker, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')
			user['brokers'][broker_id] = key

			# Update changes
			update = { 'brokers': user.get('brokers') }
			result = self.updateUser(user_id, update)
			return props


	def updateBrokerName(self, user_id, old_name, new_name):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False
		elif user['brokers'].get(old_name) is not None:
			user['brokers'][new_name] = user['brokers'][old_name]
			del user['brokers'][old_name]

		# Update changes
		update = { 'brokers': user.get('brokers') }
		result = self.updateUser(user_id, update)
		return new_name

	def deleteBroker(self, user_id, name):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			return False
		elif user['brokers'].get(name) is not None:
			del user['brokers'][name]
		
		# Shutdown broker if running

		# Update changes
		update = { 'brokers': user.get('brokers') }
		result = self.updateUser(user_id, update)
		return name

	'''
	Strategy Storage Functions
	'''

	def initStrategyStorage(self, user_id, strategy_id, name, script_id):
		gui = self.getScriptGui(script_id)
		print(f'[initStrategyStorage] GUI: {gui}')

		if not (isinstance(gui, dict) and len(gui)):
			gui = {
				'name': name,
				'pages': ["main"],
				'windows': [],
				'drawings': {}
			}

		self.updateStrategyGui(user_id, strategy_id, gui)
	
		empty_trades = { 'positions': [], 'orders': [] }
		self.updateStrategyTrades(user_id, strategy_id, empty_trades)

		columns = [
			'timestamp', 'reference_id', 'type', 'accepted',
			'order_id', 'account_id', 'product', 'order_type',
			'direction', 'lotsize', 'entry_price', 'close_price', 'sl', 'tp',
			'open_time', 'close_time'
		]
		df = pd.DataFrame(columns=columns).set_index('timestamp')
		self.updateStrategyTransactions(user_id, strategy_id, df)

		return True


	def getStrategyGui(self, user_id, strategy_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/gui.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}

		except Exception:
			return {}


	def getAccountGui(self, user_id, strategy_id, account_code):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/accounts/{account_code}/gui.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}

		except Exception:
			return {}


	def getStrategyTrades(self, user_id, strategy_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/trades.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}
		
		except Exception:
			return {}


	def getStrategyTransactions(self, user_id, strategy_id):

		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/transactions.csv.gz'
			)
			if res.get('Body'):
				f_obj = gzip.decompress(res['Body'].read())
				return pd.read_csv(io.BytesIO(f_obj), sep=',').set_index('reference_id').sort_values(by=['timestamp'])
			else:
				return None

		except Exception:
			return None


	def getStrategyInputVariables(self, user_id, strategy_id, script_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/input_variables/{script_id}.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}

		except Exception:
			return {}

	def getAccountInputVariables(self, user_id, strategy_id, account_code, script_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/accounts/{account_code}/input_variables/{script_id}.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}

		except Exception:
			return {}


	def getScriptInputVariables(self, script_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.scriptBucketName,
				Key=f'{script_id}/input_variables.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}
		
		except Exception:
			return {}


	def getScriptGui(self, script_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.scriptBucketName,
				Key=f'{script_id}/gui.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return {}
		
		except Exception:
			return {}


	def getScriptFile(self, script_id, file_name):
		try:
			res = self._s3_client.get_object(
				Bucket=self.scriptBucketName,
				Key=f'{script_id}/{file_name}.gz'
			)
			if res.get('Body'):
				return gzip.decompress(res['Body'].read())
			else:
				return None
		
		except Exception:
			return None


	def updateStrategyGui(self, user_id, strategy_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/gui.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)

		return True


	def updateAccountGui(self, user_id, strategy_id, account_code, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/accounts/{account_code}/gui.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)

		return True


	def getAccountTransactions(self, user_id, strategy_id, account_code):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/accounts/{account_code}/transactions.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))
			else:
				return { 'transactions': [] }

		except Exception:
			return { 'transactions': [] }


	def updateAccountTransactions(self, user_id, strategy_id, account_code, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/accounts/{account_code}/transactions.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)

		return True


	def appendAccountGui(self, user_id, strategy_id, account_code, obj):
		print(f'APPEND: {account_code}')
		self.addAsyncJob(self._handle_append_account_gui, user_id, strategy_id, account_code, obj)


	def _handle_append_account_gui(self, user_id, strategy_id, account_code, obj):
		print('HANDLE APPEND ACCOUNT GUI')
		MAX_GUI = 1000
		MAX_TRANSACTIONS = 6000

		gui = self.getAccountGui(user_id, strategy_id, account_code)

		# Handle Drawings
		if 'drawings' in obj:
			if 'drawings' not in gui or not isinstance(gui['drawings'], dict):
				gui['drawings'] = {}

			for i in obj['drawings']:
				if i['type'] == tl.CREATE_DRAWING:
					if i['item']['layer'] not in gui['drawings']:
						gui['drawings'][i['item']['layer']] = []
					gui['drawings'][i['item']['layer']].append(i['item'])

				elif i['type'] == tl.CLEAR_DRAWING_LAYER:
					if i['item'] in gui['drawings']:
						gui['drawings'][i['item']] = []

				elif i['type'] == tl.CLEAR_ALL_DRAWINGS:
					for layer in gui['drawings']:
						gui['drawings'][layer] = []

			for layer in gui['drawings']:
				gui['drawings'][layer] = gui['drawings'][layer][-MAX_GUI:]

		# Handle Logs
		if 'logs' in obj:
			if 'logs' not in gui or not isinstance(gui['logs'], list):
				gui['logs'] = []

			gui['logs'] += obj['logs']
			gui['logs'] = gui['logs'][-MAX_GUI:]

		# Handle Info
		if 'info' in obj:
			if 'info' not in gui or not isinstance(gui['info'], dict):
				gui['info'] = {}

			i = None
			for i in obj['info']:
				if i['product'] not in gui['info']:
					gui['info'][i['product']] = {}
				if i['period'] not in gui['info'][i['product']]:
					gui['info'][i['product']][i['period']] = {}
				if str(int(i['timestamp'])) not in gui['info'][i['product']][i['period']]:
					gui['info'][i['product']][i['period']][str(int(i['timestamp']))] = []

				gui['info'][i['product']][i['period']][str(int(i['timestamp']))].append(i['item'])

			if i and len(gui['info'][i['product']][i['period']]) > MAX_GUI:
				gui['info'][i['product']][i['period']] = dict(
					sorted(
						gui['info'][i['product']][i['period']].items(), 
						key=lambda x: int(x[0])
					)[-MAX_GUI:]
				)


		# Handle Transactions
		if 'transactions' in obj:
			result = self.getAccountTransactions(user_id, strategy_id, account_code)
			if result is not None:
				if not isinstance(result.get('transactions'), list):
					result['transactions'] = []
				result['transactions'] += obj['transactions']
			else:
				result = { 'transactions': obj['transactions'] }

			result['transactions'] = result['transactions'][-MAX_TRANSACTIONS:]
			self.updateAccountTransactions(user_id, strategy_id, account_code, result)


		# Handle Reports
		if 'reports' in obj:
			print(f'Reports: {obj["reports"]}')
			for name in obj['reports']:
				try:
					old_df = self.getStrategyAccountReport(user_id, strategy_id, account_code, name)
					if old_df is not None:
						update_df = pd.DataFrame(data=obj['reports'][name])
						new_df = pd.concat((
							old_df, update_df
						))

						columns = sorted(new_df.columns, key=lambda x: update_df.columns.get_loc(x) if x in update_df.columns else None)
						new_df = new_df[columns]

						self.updateStrategyAccountReport(user_id, strategy_id, account_code, name, new_df)
					else:
						self.updateStrategyAccountReport(
							user_id, strategy_id, account_code, name, pd.DataFrame(data=obj['reports'][name])
						)
				except Exception:
					pass

		print('Upload!')
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/accounts/{account_code}/gui.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				json.dumps(gui).encode('utf8')
			)
		)



	def updateStrategyTrades(self, user_id, strategy_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/trades.json.gz'
		)
		# TODO: Convert dataframe to csv
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)
		return True


	def updateStrategyTransactions(self, user_id, strategy_id, df):
		# df to csv in memory
		s_buf = io.StringIO()
		df.to_csv(s_buf, sep=',', header=True)
		s_buf.seek(0)
		f_obj = s_buf.read().encode('utf8')

		transactions_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/transactions.csv.gz'
		)
		transactions_object.put(Body=gzip.compress(f_obj))
		return True


	def updateStrategyInputVariables(self, user_id, strategy_id, script_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/input_variables/{script_id}.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)

		return True


	def updateAccountInputVariables(self, user_id, strategy_id, account_code, script_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/accounts/{account_code}/input_variables/{script_id}.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)

		return True


	def deleteAllUserStrategyStorage(self, user_id):
		objects_to_delete = self._s3_res.meta.client.list_objects(
			Bucket=self.strategyBucketName, 
			Prefix=f'{user_id}'
		)

		delete_keys = {
			'Objects': [
				{'Key' : k} for k in [
					obj['Key'] for obj in objects_to_delete.get('Contents', [])
				]
			]
		}

		self._s3_res.meta.client.delete_objects(
			Bucket=self.strategyBucketName,
			Delete=delete_keys
		)


	def deleteStrategyStorage(self, user_id, strategy_id):
		objects_to_delete = self._s3_res.meta.client.list_objects(
			Bucket=self.strategyBucketName, 
			Prefix=f'{user_id}/{strategy_id}'
		)

		delete_keys = {
			'Objects': [
				{'Key' : k} for k in [
					obj['Key'] for obj in objects_to_delete.get('Contents', [])
				]
			]
		}

		self._s3_res.meta.client.delete_objects(
			Bucket=self.strategyBucketName,
			Delete=delete_keys
		)


	def updateScriptInputVariables(self, script_id, obj):
		gui_object = self._s3_res.Object(
			self.scriptBucketName,
			f'{script_id}/input_variables.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				json.dumps(obj, indent=2).encode('utf8')
			)
		)

		return True

	'''
	Backtest Storage Functions
	'''

	def getStrategyBacktestList(self, user_id, strategy_id):
		bucket = self._s3_res.Bucket(self.strategyBucketName)
		result = []
		for i in bucket.objects.filter(Prefix=f'{user_id}/{strategy_id}/backtests'):
			result.append(i.key.split('/')[-2])
		return result


	def createStrategyBacktest(self, user_id, strategy_id, backtest):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None or user['strategies'].get(strategy_id) is None:
			return False

		# Add strategy to storage
		return self.initStrategyBacktestStorage(user_id, strategy_id, backtest)


	def initStrategyBacktestStorage(self, user_id, strategy_id, backtest):
		# Create Backtest ID
		existing_ids = self.getStrategyBacktestList(user_id, strategy_id)
		backtest_id = self.generateId()
		while backtest_id in existing_ids:
			backtest_id = self.generateId()

		# Init Backtest GUI
		gui = self.getStrategyGui(user_id, strategy_id)
		if 'drawings' in gui:
			del gui['drawings']
		gui['properties'] = backtest.get('properties')
		self.updateStrategyBacktestGui(user_id, strategy_id, backtest_id, gui)

		# Handle Info
		info = { 'info': {} }
		for i in backtest['info']:
			if i['product'] not in info['info']:
				info['info'][i['product']] = {}
			if i['period'] not in info['info'][i['product']]:
				info['info'][i['product']][i['period']] = {}
			if str(int(i['timestamp'])) not in info['info'][i['product']][i['period']]:
				info['info'][i['product']][i['period']][str(int(i['timestamp']))] = []

			info['info'][i['product']][i['period']][str(int(i['timestamp']))].append(i['item'])
		self.updateStrategyBacktestInfo(user_id, strategy_id, backtest_id, info)

		# Init Backtest Transactions
		transactions = { 'transactions': backtest.get('transactions') }
		self.updateStrategyBacktestTransactions(user_id, strategy_id, backtest_id, transactions)

		if 'reports' in backtest:
			for name in backtest.get('reports'):
				report_obj = backtest['reports'][name]
				self.updateStrategyBacktestReport(user_id, strategy_id, backtest_id, name, report_obj)

		print(f'Backtest Uploaded: {backtest_id}')

		return backtest_id


	def getStrategyBacktestGui(self, user_id, strategy_id, backtest_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/backtests/{backtest_id}/gui.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))

		except Exception:
			return None


	def updateStrategyBacktestGui(self, user_id, strategy_id, backtest_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/backtests/{backtest_id}/gui.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)
		return True


	def updateStrategyBacktestInfo(self, user_id, strategy_id, backtest_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/backtests/{backtest_id}/info.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)
		return True


	def getStrategyBacktestTransactions(self, user_id, strategy_id, backtest_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/backtests/{backtest_id}/transactions.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))

		except Exception:
			return None


	def getStrategyBacktestInfo(self, user_id, strategy_id, backtest_id):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/backtests/{backtest_id}/info.json.gz'
			)
			if res.get('Body'):
				return json.loads(gzip.decompress(res['Body'].read()))

		except Exception:
			return None


	def updateStrategyBacktestTransactions(self, user_id, strategy_id, backtest_id, obj):
		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/backtests/{backtest_id}/transactions.json.gz'
		)
		gui_object.put(
			Body=gzip.compress(
				self._flat_dump(obj, indent=2).encode('utf8')
			)
		)
		return True


	def createAccountBacktest(self, user_id, strategy_id, broker_id, account_id, backtest):
		account_code = '.'.join((broker_id, account_id))
		# Clear Previous GUI
		self.updateAccountGui(
			user_id, strategy_id, account_code, 
			{ 'info': {} }
		)
		self.updateAccountTransactions(
			user_id, strategy_id, account_code, 
			{ 'transactions': [] }
		)

		# Upload New GUI
		self._handle_append_account_gui(user_id, strategy_id, account_code, backtest)

		try:
			self.ctrl.sio.emit(
				'ongui', 
				{
					'strategy_id': strategy_id, 
					'item': {
						'account_code': account_code,
						'type': 'live_backtest_uploaded',
					}
				}, 
				namespace='/admin'
			)
		except Exception:
			print(traceback.format_exc(), flush=True)


	# Reports
	def getStrategyAccountReport(self, user_id, strategy_id, account_code, name):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/accounts/{account_code}/reports/{name}.csv.gz'
			)
			if res.get('Body'):
				f_obj = gzip.decompress(res['Body'].read())
				return pd.read_csv(io.BytesIO(f_obj), sep=',', dtype=str)

		except Exception:
			return None


	def updateStrategyAccountReport(self, user_id, strategy_id, account_code, name, obj):
		if isinstance(obj, dict):
			obj = pd.DataFrame(data=obj)

		s_buf = io.StringIO()
		obj.to_csv(s_buf, sep=',', header=True, index=False)
		s_buf.seek(0)
		f_obj = s_buf.read().encode('utf8')

		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/accounts/{account_code}/reports/{name}.csv.gz'
		)
		gui_object.put(Body=gzip.compress(f_obj))
		return True


	def getStrategyBacktestReport(self, user_id, strategy_id, backtest_id, name):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/backtests/{backtest_id}/reports/{name}.csv.gz'
			)
			if res.get('Body'):
				f_obj = gzip.decompress(res['Body'].read())
				return pd.read_csv(io.BytesIO(f_obj), sep=',', dtype=str)

		except Exception:
			return None


	def updateStrategyBacktestReport(self, user_id, strategy_id, backtest_id, name, obj):
		if isinstance(obj, dict):
			obj = pd.DataFrame(data=obj)

		s_buf = io.StringIO()
		obj.to_csv(s_buf, sep=',', header=True, index=False)
		s_buf.seek(0)
		f_obj = s_buf.read().encode('utf8')
		

		gui_object = self._s3_res.Object(
			self.strategyBucketName,
			f'{user_id}/{strategy_id}/backtests/{backtest_id}/reports/{name}.csv.gz'
		)
		gui_object.put(Body=gzip.compress(f_obj))
		return True


	'''
	Prices Storage Functions
	'''

	def getPriceYearlyDateList(self, broker, product, period):
		bucket = self._s3_res.Bucket(self.priceDataBucketName)
		result = []
		for i in bucket.objects.filter(Prefix=f'{broker}/{product}/{period}'):
			result.append(datetime.strptime(
				i.key.split('/')[-1].split('-')[0], '%Y'
			))
		return result

	def getPriceDailyDateList(self, broker, product, period):
		bucket = self._s3_res.Bucket(self.priceDataBucketName)
		result = []
		for i in bucket.objects.filter(Prefix=f'{broker}/{product}/{period}'):
			result.append(datetime.strptime(
				i.key.split('/')[-1].replace('.csv.gz', ''), '%Y-%m-%d'
			))
		return result


	def getYearlyPrices(self, broekr, product, period, dt):
		try:
			res = self._s3_client.get_object(
				Bucket=self.priceDataBucketName,
				Key=f'{broker}/{product}/{period}/{dt.year}-{dt.year+1}.csv.gz'
			)
			f_obj = gzip.decompress(res['Body'].read())
			return pd.read_csv(io.BytesIO(f_obj), sep=' ').set_index('timestamp')

		except Exception:
			return None

	
	def getDailyPrices(self, broker, product, period, dt):
		try:
			res = self._s3_client.get_object(
				Bucket=self.priceDataBucketName,
				Key=f'{broker}/{product}/{period}/{dt.strftime("%Y-%m-%d")}.csv.gz'
			)
			f_obj = gzip.decompress(res['Body'].read())
			return pd.read_csv(io.BytesIO(f_obj), sep=' ').set_index('timestamp')

		except Exception:
			return None


	def updateYearlyPrices(self, broker, product, period, dt, df):
		# df to csv in memory
		s_buf = io.StringIO()
		df.to_csv(s_buf, sep=' ', header=True)
		s_buf.seek(0)
		f_obj = s_buf.read().encode('utf8')

		prices_object = self._s3_res.Object(
			self.priceDataBucketName,
			f'{broker}/{product}/{period}/{dt.year}-{dt.year+1}.csv.gz'
		)
		prices_object.put(Body=gzip.compress(f_obj))
		return True


	def updateDailyPrices(self, broker, product, period, dt, df):
		# df to csv in memory
		s_buf = io.StringIO()
		df.to_csv(s_buf, sep=' ', header=True)
		s_buf.seek(0)
		f_obj = s_buf.read().encode('utf8')

		prices_object = self._s3_res.Object(
			self.priceDataBucketName,
			f'{broker}/{product}/{period}/{dt.strftime("%Y-%m-%d")}.csv.gz'
		)
		prices_object.put(Body=gzip.compress(f_obj))
		return True

	def deletePrices(self, broker, product, period):
		self._s3_res.meta.client.delete_objects(
			Bucket=self.priceDataBucketName,
			Delete=[f'{broker}/{product}/{period}']
		)
		return True

	
	'''
	Temp Functions
	'''

	def generateHolyGrailJSON(self):
		return {
			"name": "Holy Grail",
			"pages": [
				"Main",
				"System Results",
				"Log"
			],
			"windows": [
				{"id": "SDE32F", "page": 0, "type": "chart", "pos": {"x": 20, "y": 0}, "size": {"width": 40, "height": 100}, "zIndex": 3, "properties": {"layout": "Layout 1", "broker": "fxcm", "product": "EUR_USD", "period": "M2", "price": "mids", "portion": 0.7458112657694993, "overlays": [{"type": "boll", "properties": {"Period": {"type": "number", "value": 20}, "StdDev": {"type": "number", "value": 2}}, "appearance": {"colors": [[[155, 89, 182], [155, 89, 182]]]}}, {"type": "ema", "properties": {"Period": {"type": "number", "value": 8}}, "appearance": {"colors": [[[255, 82, 82]]]}}, {"type": "ema", "properties": {"Period": {"type": "number", "value": 21}}, "appearance": {"colors": [[[255, 177, 66]]]}}], "studies": [{"type": "tr", "portion": 0.25418873423050037, "properties": {"Period": {"type": "number", "value": 14}}, "appearance": {"colors": [[[30, 144, 255]]]}}], "drawing_layers": ["main", "main_a", "dv_a", "entry_rtv_long", "entry_rtv_short", "ht_rtv_long", "ht_rtv_short", "active_rtv_long", "active_rtv_short", "entry_rtc_long", "entry_rtc_short", "ht_rtc_long", "ht_rtc_short", "active_rtc_long", "active_rtc_short", "entry_dv_long", "entry_dv_short", "active_dv_long", "active_dv_short"], "chart": {"background": [255, 255, 255], "horizontalGrid": [240, 240, 240], "verticalGrid": [240, 240, 240], "priceLabel": [80, 80, 80], "priceLine": [80, 80, 80], "crosshair": [80, 80, 80]}, "bars": {"bodyLong": [255, 255, 255], "outlineLong": [0, 0, 0], "wickLong": [0, 0, 0], "bodyShort": [0, 0, 0], "outlineShort": [0, 0, 0], "wickShort": [0, 0, 0]}, "trading": {}}, "maximised": False, "metadata": {"pos": {"x": -26.07769079645013, "y": 1.221433804088952}, "scale": {"x": 196.85220147795633, "y": 0.0068549999999999445}}},
				{"id": "SGHFDS", "page": 0, "type": "chart", "pos": {"x": 60, "y": 0}, "size": {"width": 40, "height": 50}, "zIndex": 1, "properties": {"layout": "Layout 1", "broker": "fxcm", "product": "EUR_USD", "period": "M5", "price": "mids", "portion": 0.8585202863961816, "overlays": [{"type": "boll", "properties": {"Period": {"type": "number", "value": 20}, "StdDev": {"type": "number", "value": 2}}, "appearance": {"colors": [[[155, 89, 182], [155, 89, 182]]]}}, {"type": "ema", "properties": {"Period": {"type": "number", "value": 8}}, "appearance": {"colors": [[[255, 82, 82]]]}}, {"type": "ema", "properties": {"Period": {"type": "number", "value": 21}}, "appearance": {"colors": [[[255, 177, 66]]]}}], "studies": [{"type": "tr", "portion": 0.1414797136038186, "properties": {"Period": {"type": "number", "value": 14}}, "appearance": {"colors": [[[30, 144, 255]]]}}], "drawing_layers": ["main", "main_b", "dv_b"], "chart": {"background": [255, 255, 255], "horizontalGrid": [240, 240, 240], "verticalGrid": [240, 240, 240], "priceLabel": [80, 80, 80], "priceLine": [80, 80, 80], "crosshair": [80, 80, 80]}, "bars": {"bodyLong": [255, 255, 255], "outlineLong": [0, 0, 0], "wickLong": [0, 0, 0], "bodyShort": [0, 0, 0], "outlineShort": [0, 0, 0], "wickShort": [0, 0, 0]}, "trading": {}}, "maximised": False, "metadata": {"pos": {"x": -20.1123976031434, "y": 1.2214263289687777}, "scale": {"x": 57.28933979999999, "y": 0.003210000000000046}}},
				{"id": "XCBVDS", "page": 0, "type": "chart", "pos": {"x": 60, "y": 50}, "size": {"width": 40, "height": 50}, "zIndex": 2, "properties": {"layout": "Layout 1", "broker": "fxcm", "product": "EUR_USD", "period": "M10", "price": "mids", "portion": 0.8, "overlays": [{"type": "boll", "properties": {"Period": {"type": "number", "value": 20}, "StdDev": {"type": "number", "value": 2}}, "appearance": {"colors": [[[155, 89, 182], [155, 89, 182]]]}}, {"type": "ema", "properties": {"Period": {"type": "number", "value": 8}}, "appearance": {"colors": [[[255, 82, 82]]]}}, {"type": "ema", "properties": {"Period": {"type": "number", "value": 21}}, "appearance": {"colors": [[[255, 177, 66]]]}}], "studies": [{"type": "tr", "portion": 0.2, "properties": {"Period": {"type": "number", "value": 14}}, "appearance": {"colors": [[[30, 144, 255]]]}}], "drawing_layers": ["main", "main_c", "dv_c"], "chart": {"background": [255, 255, 255], "horizontalGrid": [240, 240, 240], "verticalGrid": [240, 240, 240], "priceLabel": [80, 80, 80], "priceLine": [80, 80, 80], "crosshair": [80, 80, 80]}, "bars": {"bodyLong": [255, 255, 255], "outlineLong": [0, 0, 0], "wickLong": [0, 0, 0], "bodyShort": [0, 0, 0], "outlineShort": [0, 0, 0], "wickShort": [0, 0, 0]}, "trading": {}}, "maximised": False, "metadata": {"pos": {"x": -14.294110410202608, "y": 1.2203216526442304}, "scale": {"x": 45.18872583695999, "y": 0.008879999999999888}}},
				{"id": "LDV325", "page": 0, "type": "dockable", "opened": "VJFME2", "zIndex": 4, "pos": {"x": 0, "y": 0}, "size": {"width": 20, "height": 100}, "maximised": False, "windows": [{"id": "VJFME2", "type": "info", "properties": {}}, {"id": "ASDJ32", "type": "control_panel", "properties": {}}]},
				{"id": "BJROD3", "page": 1, "type": "dockable", "opened": "UJEJN9", "zIndex": 1, "pos": {"x": 0, "y": 0}, "size": {"width": 100, "height": 100}, "maximised": True, "windows": [{"id": "UJEJN9", "type": "report", "properties": {"name": "System Results", "format": {"Time": {"type": "date", "format": "HH:mm:ss"}}}}]},
				{"id": "ASF483", "page": 2, "type": "dockable", "opened": "KDFB21", "zIndex": 1, "pos": {"x": 0, "y": 0}, "size": {"width": 100, "height": 100}, "maximised": True, "windows": [{"id": "KDFB21", "type": "log", "properties": {}}]}
			],
			"settings": {"chart-settings": {"current": "Layout 1", "layouts": {"Layout 1": {"general": {"timezone": {"value": "UTC-5"}, "date-format": {"value": "DD MMM `YY  HH:mm"}, "font-size": {"value": 10}, "precision": {"value": "1/100000"}}, "appearance": {"body": {"enabled": True, "long": "#ffffff", "short": "#000000"}, "outline": {"enabled": True, "long": "#000000", "short": "#000000"}, "wick": {"enabled": True, "long": "#000000", "short": "#000000"}, "bid-ask-line": {"enabled": True, "ask": "#3498db", "bid": "#f39c12"}, "price-line": {"enabled": True, "value": "#3498db"}, "vert-grid-lines": {"enabled": True, "value": "#f0f0f0"}, "horz-grid-lines": {"enabled": True, "value": "#f0f0f0"}, "crosshair": {"enabled": True, "value": "#505050"}}, "trading": {"show-positions": {"enabled": True}, "show-orders": {"enabled": True}}}}}},
			"account": ""
		}


	def generateHolyGrailStrategy(self, user_id):
		strategy_id = self.createStrategy(
			user_id, {
				'name': 'Holy Grail',
				'brokers': {},
				'package': 'HolyGrail.v1_0_0'
			}
		)

		self.updateStrategyGui(user_id, strategy_id, self.generateHolyGrailJSON())
		self.updateUser(
			user_id, 
			{ 
				'metadata': {
					'current_strategy': strategy_id,
					'open_strategies': [ strategy_id ]
				} 
			}
		)


	'''
	HolyGrail Demo
	'''

	def getProdUser(self, user_id):
		res = self.prodUserTable.get_item(
			Key={ 'user_id': user_id }
		)
		if res.get('Item'):
			return self._convert_to_float(res['Item'])
		else:
			return None


	def getAllProdUsers(self):
		res = self.prodUserTable.scan()
		data = res['Items']

		while 'LastEvaluatedKey' in res:
			res = self.prodUserTable.scan(ExclusiveStartKey=res['LastEvaluatedKey'])
			data.extend(res['Items'])

		return self._convert_to_float(data)


	def getHolyGrailUser(self, user_id):
		res = self.holygrailAccessTable.get_item(
			Key={ 'user_id': user_id }
		)
		if res.get('Item'):
			return res['Item']
		else:
			return None


	def getAllHolyGrailUsers(self):
		res = self.holygrailAccessTable.scan()
		data = res['Items']

		while 'LastEvaluatedKey' in res:
			res = self.holygrailAccessTable.scan(ExclusiveStartKey=res['LastEvaluatedKey'])
			data.extend(res['Items'])

		return data


	def addHolyGrailUser(self, user_id, email, first_name, last_name, approved):
		if not self.getHolyGrailUser(user_id):
			res = self.holygrailAccessTable.put_item(
				Item={
					'user_id': user_id,
					'email': email,
					'first_name': first_name,
					'last_name': last_name,
					'approved': approved
				}
			)
		else:
			self.updateHolyGrailUser(
				user_id, { 'approved': True }
			)

		return user_id


	def updateHolyGrailUser(self, user_id, update):
		update_values = self._convert_to_decimal(
			dict([tuple([':{}'.format(i[0]), i[1]])
					for i in update.items()])
		)

		update_exp = ('set ' + ' '.join(
			['{} = :{},'.format(k, k) for k in update.keys()]
		))[:-1]

		res = self.holygrailAccessTable.update_item(
			Key={
				'user_id': user_id
			},
			UpdateExpression=update_exp,
			ExpressionAttributeValues=update_values,
			ReturnValues="UPDATED_NEW"
		)
		return True


	def deleteHolyGrailUser(self, user_id):
		res = self.holygrailAccessTable.delete_item(
			Key={
				'user_id': user_id,
			}
		)
		return True


	def checkHolyGrailToken(self, token):
		res = self.holygrailTokenTable.get_item(
			Key={ 'token': token }
		)
		if res.get('Item'):
			print(res.get('Item'))
			return True
		else:
			return False


	def addHolyGrailToken(self, token):
		res = self.holygrailTokenTable.put_item(
			Item={
				'token': token
			}
		)

		return token


	def deleteHolyGrailToken(self, token):
		res = self.holygrailTokenTable.delete_item(
			Key={
				'token': token,
			}
		)
		return True


	'''
	Password Reset
	'''

	def addPasswordResetToken(self, token):
		res = self.resetPasswordTokenTable.put_item(
			Item={
				'token': token
			}
		)

		return token


	def deletePasswordResetToken(self, token):
		res = self.resetPasswordTokenTable.delete_item(
			Key={
				'token': token,
			}
		)
		return True


	def checkResetPasswordToken(self, token):
		res = self.resetPasswordTokenTable.get_item(
			Key={ 'token': token }
		)
		if res.get('Item'):
			return True
		else:
			return False



	def deleteAllStrategies(self):
		# users = self.userTable.scan(AttributesToGet=['user_id'])['Items']
		users = self.get_all_primary_keys()


		rm = ['demo', 'spotware']
		for i in rm:
			if i in users:
				del users[users.index(i)]

		print(users)
		update = {'strategies': {}}
		for user_id in users:
			# Delete Strategies in DB
			self.updateUser(user_id, update)

			bucket = self._s3_res.Bucket(self.strategyBucketName)
			bucket.objects.filter(Prefix=user_id+'/').delete()

			# # Delete user storage
			# objects_to_delete = self._s3_res.meta.client.list_objects(
			# 	Bucket=self.strategyBucketName, 
			# 	Prefix=f'{user_id}'
			# )

			# delete_keys = {
			# 	'Objects': [
			# 		{'Key' : k} for k in [
			# 			obj['Key'] for obj in objects_to_delete.get('Contents', [])
			# 		]
			# 	]
			# }

			# self._s3_res.meta.client.delete_objects(
			# 	Bucket=self.strategyBucketName,
			# 	Delete=delete_keys
			# )


	def get_all_primary_keys(self):
	    primary_keys = []
	    count = 0
	    r = self.userTable.scan(
	        AttributesToGet=[
	            'user_id',
	        ]
	    )
	    count += r['Count']
	    print(r['Items'])
	    for i in r['Items']:
	        primary_keys.append(i['user_id'])
	    '''discards data after 1MB, hence the following code'''
	    while True:
	        try:
	            r = self.userTable.scan(
	                AttributesToGet=[
	                    'user_id',
	                ],
	                ExclusiveStartKey={
	                    'user_id': {
	                        'S': r['LastEvaluatedKey']['user_id']
	                    }
	                }
	            )
	            count += r['Count']
	            for i in r['Items']:
	                primary_keys.append(i['user_id'])
	        except KeyError as e:
	            print(e)
	            break
	    return primary_keys