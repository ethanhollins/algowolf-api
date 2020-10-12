import traceback

from threading import Thread
from urllib.parse import urlparse, urljoin, urlencode
from urllib.request import urlopen

CONNECTION_URL_PATH = "lightstreamer/create_session.txt"
BIND_URL_PATH = "lightstreamer/bind_session.txt"
CONTROL_URL_PATH = "lightstreamer/control.txt"
# Request parameter to create and activate a new Table.
OP_ADD = "add"
# Request parameter to delete a previously created Table.
OP_DELETE = "delete"
# Request parameter to force closure of an existing session.
OP_DESTROY = "destroy"
# List of possible server responses
PROBE_CMD = "PROBE"
END_CMD = "END"
LOOP_CMD = "LOOP"
ERROR_CMD = "ERROR"
SYNC_ERROR_CMD = "SYNC ERROR"
OK_CMD = "OK"


class LightstreamerSubscription(object):

	def __init__(self, mode, items, fields, adapter=''):
		self.items = items
		self._items_map = {}
		self.fields = fields
		self.adapter = adapter
		self.mode = mode
		self.listeners = []

	@staticmethod
	def _decode(value, last):
		"""Decode the field value according to Lightstreamer Text Protocol specifications."""

		if value == "$":
			return u''
		elif value == "#":
			return None
		elif not value:
			return last
		elif value[0] in "#$":
			value = value[1:]

		return value


	def addListener(self, listener):
		self.listeners.append(listener)


	def onUpdate(self, item):
		"""Invoked by LSClient each time Lightstreamer Server pushes a new item event."""

		# tokenize the item line as sent by Lightstreamer
		toks = item.rstrip('\r\n').split('|')
		undecoded_item = dict(list(zip(self.fields, toks[1:])))

		# retrieve the previous item stored into the map, if present, otherwise create a new empty dict
		item_pos = int(toks[0])
		curr_item = self._items_map.get(item_pos, {})
		# update the map with new values, merging with the previous ones if any
		self._items_map[item_pos] = dict([(k, self._decode(v, curr_item.get(k))) for k, v in list(undecoded_item.items())])
		# make an item info as a new event to be passed to listeners
		item_info = {'pos': item_pos,
					 'name': self.items[item_pos - 1],
					 'values': self._items_map[item_pos]}

		# update each registered listener with new event
		for on_item_update in self.listeners:
			on_item_update(item_info)


class LightstreamerClient(object):

	def __init__(self, broker, username, password, url, adapter_set=''):
		self.broker = broker

		self.username = username
		self.password = password
		self.url = urlparse(url)
		self._adapter_set = adapter_set

		self.stream = None
		self.stream_thread = None
		self.session = {}
		self.subscriptions = {}
		self._current_subscription_key = 0
		self._control_url = None
		self._bind_counter = 0


	def _encode_params(self, params):
		"""Encode the parameter for HTTP POST submissions, but only for non empty values."""

		return urlencode(dict([(k, v) for (k, v) in iter(params.items()) if v])).encode("utf-8")


	def _call(self, base_url, url, params):
		"""Open a network connection and performs HTTP Post with provided params."""

		url = urljoin(base_url.geturl(), url)
		body = self._encode_params(params)

		return urlopen(url, data=body)


	def _control(self, params):
		"""Create a Control Connection to send control commands that manage the content of Stream Connection."""

		params['LS_session'] = self.session['SessionId']
		res = self._call(self._control_url, CONTROL_URL_PATH, params)
		decoded_res = self._read_line(res)
		return decoded_res


	def _read_line(self, stream):
		"""Read a single line of content of the Stream Connection."""
		return stream.readline().decode('utf-8').rstrip()


	def _set_control_link_url(self, custom_address=None):
		"""Set the address to use for the Control Connection in such cases where Lightstreamer is behind a Load Balancer."""

		if custom_address is None:
			self._control_url = self.url
		else:
			self._control_url = urlparse("//" + custom_address)._replace(scheme=self.url[0])


	def _handle_stream(self, stream_line):
		if stream_line == OK_CMD:
			# Parsing session
			while True:
				next_line = self._read_line(self.stream)
				if next_line:
					key, value = next_line.split(':', 1)
					self.session[key] = value
				else:
					break

			# Setup of the control url
			self._set_control_link_url(self.session.get('ControlAddress'))

			# Start thread to receive real time updates
			self.stream_thread = self.broker.ctrl.continuousThreadHandler.addJob(self._receive)

		else:
			raise IOError(stream_line)


	def connect(self):
		"""Establish a connection to Lightstreamer Server to create a new session."""
		CREATE_PARAMS = {
			'LS_op2': 'create',
			'LS_cid': 'mgQkwtwdysogQz2BJ4Ji kOj2Bg',
			"LS_adapter_set": self._adapter_set,
			"LS_user": self.username,
			"LS_password": self.password
		}

		self.stream = self._call(self.url, CONNECTION_URL_PATH, CREATE_PARAMS)
		stream_line = self._read_line(self.stream)
		self._handle_stream(stream_line)


	def bind(self):
		"""Replace a completely consumed connection in listening for an active Session."""

		BIND_PARAMS = {"LS_session": self.session['SessionId']}
		self.stream = self._call(self._control_url, BIND_URL_PATH, BIND_PARAMS)

		self._bind_counter += 1
		stream_line = self._read_line(self.stream)
		self._handle_stream(stream_line)


	def disconnect(self):
		if self.stream is not None:
			self._control({"LS_op": OP_DESTROY})
			self.broker.ctrl.continuousThreadHandler.stopJob(self.stream_thread)


	def subscribe(self, subscription):
		""""Perform a subscription request to Lightstreamer Server."""

		# Register the subscription with a new key
		self._current_subscription_key += 1
		self.subscriptions[self._current_subscription_key] = subscription

		# Send the control request to perform the subscription
		CONTROL_PARAMS = {
			"LS_Table": self._current_subscription_key,
			"LS_op": OP_ADD,
			"LS_data_adapter": subscription.adapter,
			"LS_mode": subscription.mode,
			"LS_schema": " ".join(subscription.fields),
			"LS_id": " ".join(subscription.items)
		}
		res = self._control(CONTROL_PARAMS)
		if res == OK_CMD:
			# Successful
			pass
		else:
			# Unsuccessful
			pass

		return self._current_subscription_key


	def unsubscribe(self, subscription_key):
		"""Unregister the Subscription associated to the specified subscription_key."""

		if subscription_key in self.subscriptions:
			UNSUBSCRIBE_PARAMS = {
				'LS_Table': subscription_key,
				'LS_op': OP_DELETE
			}
			res = self._control(UNSUBSCRIBE_PARAMS)

			if res == OK_CMD:
				# Successful
				pass
			else:
				# Unsuccessful
				pass
		else:
			# Not found
			pass


	def _forward_update(self, update):
		try:
			tok = update.split(',', 1)
			table, item = int(tok[0]), tok[1]
			if table in self.subscriptions:
				self.subscriptions[table].onUpdate(item)
			else:
				# Subscription not found
				pass

		except Exception:
			print(traceback.format_exc())


	def _receive(self):
		rebind = False
		receive = True

		try:
			message = self._read_line(self.stream)
			if not message.strip():
				message = None
		except Exception as e:
			print(traceback.format_exc())
			message = None
			# Reconnect
			Thread(target=self.broker._reconnect).start()

		if message is None:
			receive = False
		elif message == PROBE_CMD:
			# skipping the PROBE message, keep on receiving messages
			# log.debug("PROBE message")
			pass
		elif message.startswith(ERROR_CMD):
			# terminate the receiving loop on ERROR message
			receive = False
			# log.error("ERROR")
			pass
		elif message.startswith(LOOP_CMD):
			# terminate the the receiving loop on LOOP message
			# a complete implementation should proceed with a rebind of the session
			# log.debug("LOOP")
			receive = False
			rebind = True
			pass
		elif message.startswith(SYNC_ERROR_CMD):
			# terminate the receiving loop on SYNC ERROR message
			# a complete implementation should create a new session and re-subscribe to all the old items and relative fields
			# log.error("SYNC ERROR")
			receive = False
			pass
		elif message.startswith(END_CMD):
			# terminate the receiving loop on END message
			# the session has been forcibly closed on the server side a complete implementation should handle the "cause_code" if present
			# log.info("Connection closed by the server")
			receive = False
			pass
		elif message.startswith("Preamble"):
			# skipping Preamble message, keep on receiving messages
			# log.debug("Preamble")
			pass
		else:
			self._forward_update(message)

		if not receive:
			if not rebind:
				self.stream = None
				self.session.clear()
				self.subscriptions.clear()
				self._current_subscription_key = 0
			else:
				self.bind()

