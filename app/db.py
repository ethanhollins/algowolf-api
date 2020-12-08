import shortuuid
import boto3
import io
import json, csv, gzip, collections
import string, random
import jwt
import pandas as pd
from datetime import datetime
from app import tradelib as tl
from app.error import BrokerException
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

class Database(object):

	def __init__(self, ctrl, db_name):
		self.ctrl = ctrl
		self._generate_db()
		self._generate_s3()
		if 'dev' in db_name:
			self.userTable = self._generate_table('brokerlib-user-dev')
			self.scriptTable = self._generate_table('algowolf-scripts-dev')
			self.strategyBucketName = 'brokerlib-strategies-dev'
			self.scriptBucketName = 'algowolf-scripts-dev'
		else:
			self.userTable = self._generate_table('brokerlib-user')
			self.scriptTable = self._generate_table('algowolf-scripts')
			self.strategyBucketName = 'brokerlib-strategies'
			self.scriptBucketName = 'algowolf-scripts-dev'

		self.priceDataBucketName = 'brokerlib-prices'

	'''
	Utilities
	'''

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
			print(row)
			return Decimal(row)
			
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

	def registerUser(self, username, password):
		user_id = shortuuid.uuid()

		res = self.userTable.put_item(
			Item={
				'user_id': user_id,
				'username': username,
				'password': password,
				'brokers': {},
				'strategies': {}
			}
		)
		return user_id

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

	def updateUser(self, user_id, update):
		update_values = self._convert_to_decimal(
			dict([tuple([':{}'.format(i[0][0]), i[1]])
					for i in update.items()])
		)
		update_exp = ('set ' + ' '.join(
			['{} = :{},'.format(k, k[0]) for k in update.keys()]
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

		# Make sure id is unique
		strategy_id = self.generateId()
		while strategy_id in user['strategies']:
			strategy_id = self.generateId()
		# Add strategy to db
		user['strategies'][strategy_id] = strategy
		# Update changes
		update = { 'strategies': user.get('strategies') }
		result = self.updateUser(user_id, update)
		
		# Add strategy to storage
		self.initStrategyStorage(user_id, strategy_id, strategy['name'])
		
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

		# Validation
		if props.get('key') is None:
			raise BrokerException('Invalid data submitted.')

		# Check if key in use
		for v in user['brokers'].values():
			v = jwt.decode(
				v, self.ctrl.app.config['SECRET_KEY'], 
				algorithms=['HS256']
			)
			if (v.get('broker') == broker_name and 
					v.get('key') == props.get('key')):
				return None

		props.update({
			'name': name,
			'broker': broker_name
		})

		if broker_name == tl.broker.IG_NAME:
			# IG Validation
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
				account_id: { 'active': False, 'nickname': '' }
				for account_id in accounts
			}
		
		elif broker_name == tl.broker.OANDA_NAME:
			if props.get('is_demo') is None:
				raise BrokerException('Invalid data submitted.')

			# Run broker API call check
			dummy_broker = tl.broker.Oanda(
				self.ctrl, props.get('key'), props.get('is_demo'), is_dummy=True
			)
			accounts = dummy_broker.getAllAccounts()

			if accounts is None:
				raise BrokerException('Unable to connect to broker.')

			# Set Accounts Information
			props['accounts'] = {
				account_id: { 'active': False, 'nickname': '' }
				for account_id in accounts
			}


		# Upload new broker info
		key = jwt.encode(props, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')
		user['brokers'][broker_id] = key

		# Update changes
		update = { 'brokers': user.get('brokers') }
		result = self.updateUser(user_id, update)
		return props


	def updateBroker(self, user_id, broker_id, props):
		# Retrieve user and make changes
		user = self.getUser(user_id)
		if user is None:
			raise BrokerException('User not found.')


		# Upload new broker info
		key = jwt.encode(props, self.ctrl.app.config['SECRET_KEY'], algorithm='HS256').decode('utf8')
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

	def initStrategyStorage(self, user_id, strategy_id, name):
		empty_gui = {
			'name': name,
			'pages': 1,
			'windows': [],
			'drawings': {}
		}
		self.updateStrategyGui(user_id, strategy_id, empty_gui)
	
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

		if 'reports' in obj:
			reports = obj.pop('reports')
			for name in reports:
				report_obj = obj['reports'][name]
				self.updateStrategyAccountReport(user_id, strategy_id, account_code, name, report_obj)

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

		# Init Backtest Transactions
		transactions = { 'transactions': backtest.get('transactions') }
		self.updateStrategyBacktestTransactions(user_id, strategy_id, backtest_id, transactions)

		if 'reports' in backtest:
			for name in backtest.get('reports'):
				report_obj = backtest['reports'][name]
				self.updateStrategyBacktestReport(user_id, strategy_id, backtest_id, name, report_obj)

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


	# Reports
	def getStrategyAccountReport(self, user_id, strategy_id, account_code, name):
		try:
			res = self._s3_client.get_object(
				Bucket=self.strategyBucketName,
				Key=f'{user_id}/{strategy_id}/accounts/{account_code}/reports/{name}.csv.gz'
			)
			if res.get('Body'):
				f_obj = gzip.decompress(res['Body'].read())
				return pd.read_csv(io.BytesIO(f_obj), sep=',')

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
				return pd.read_csv(io.BytesIO(f_obj), sep=',')

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

	