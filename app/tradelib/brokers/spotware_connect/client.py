from twisted.internet.protocol import Factory
from twisted.internet.endpoints import clientFromString
from twisted.application.internet import ClientService
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from .protocol import Protocol
from .protobuf import Protobuf


class Client(ClientService):
    PROXY_DEMO = "demo.ctraderapi.com:5035"
    PROXY_LIVE = "live.ctraderapi.com:5035"
    EVENT_CONNECT_NAME = "connect"
    EVENT_DISCONNECT_NAME = "disconnect"
    EVENT_MESSAGE_NAME = "message"

    class Protocol(Protocol):
        client = None

        def connectionMade(self):
            super().connectionMade()
            self.client.connect()

        def connectionLost(self, reason):
            super().connectionLost(reason)
            self.client.disconnect()

        def receive(self, message):
            self.client.receive(message)

    class Factory(Factory):
        client = None

        def __init__(self, *args, **kwargs):
            super().__init__()
            self.client = kwargs['client']

        def buildProtocol(self, addr):
            p = super().buildProtocol(addr)
            p.client = self.client
            return p

    def __init__(self, live=False, retryPolicy=None,
                 clock=None, prepareConnection=None):
        host = "ssl:" + (self.PROXY_LIVE if live else self.PROXY_DEMO)
        endpoint = clientFromString(reactor, host)
        factory = Client.Factory.forProtocol(Client.Protocol, client=self)
        super().__init__(endpoint, factory, retryPolicy=retryPolicy,
                         clock=clock, prepareConnection=prepareConnection)


    def _on_loop(self):
        print('loop!')


    def start(self, timeout=None):
        # LoopingCall(self._on_loop)

        self.startService()

        if timeout:
            reactor.callLater(timeout, self.stop)

        reactor.run(installSignalHandlers=False)

    def stop(self):
        self.stopService()
        if reactor.running:
            reactor.stop()

    def connect(self):
        self.exec_events(self.EVENT_CONNECT_NAME)

    def disconnect(self):
        self.exec_events(self.EVENT_DISCONNECT_NAME)

    def receive(self, message):
        payload = Protobuf.extract(message)
        kargs = dict(msg=message, msgid=message.clientMsgId,
                     msgtype=message.payloadType,
                     payload=payload,
                     **{fv[0].name: fv[1] for fv in payload.ListFields()})

        if "ctidTraderAccountId" in kargs:
            kargs["ctid"] = payload.ctidTraderAccountId

        self.exec_events(self.EVENT_MESSAGE_NAME, **kargs)


    # def emit_from_thread(self, message, msgid=None, **params):
    #     reactor.callFromThread(self.emit, message, msgid=None, **params)


    def emit(self, message, msgid=None, **params):
        if type(message) in [str, int]:
            message = Protobuf.get(message, **params)

        def protocol_send(protocol):
            protocol.send(message, msgid=msgid)

        def protocol_err(msg):
            print(f'[ERROR]: {msg}')

        con = self.whenConnected()
        con.addCallback(protocol_send)
        con.addErrback(protocol_err)
        return con

    _events = dict()

    def event(self, name_or_func=None, func=None, **filters):
        if not self._events:  # lazy create
            for e in [self.EVENT_CONNECT_NAME,
                      self.EVENT_DISCONNECT_NAME, self.EVENT_MESSAGE_NAME]:
                self._events[e] = []

        if callable(name_or_func):  # callable append
            name = name_or_func.__name__
            self._events[name].append(name_or_func)
            return name_or_func
        

        if callable(func):
            name = name_or_func
            if 'msgtype' in filters and type(filters['msgtype']) in [str, int]:
                filters['msgtype'] = Protobuf.get_type(filters['msgtype'])

            def callback(*args, **kwargs):
                for k, v in filters.items():
                    if k not in kwargs or kwargs[k] != v:
                        return
                func(*args, **kwargs)

            self._events[name].append(callback)

        else:

            def decorate(func):  # decorate with args
                evname = name_or_func

                from functools import wraps

                @wraps(func)
                def func_wrap(*args, **kwargs):
                    for k, v in filters.items():
                        if k not in kwargs or kwargs[k] != v:
                            return
                    func(*args, **kwargs)

                self._events[evname].append(func_wrap)
                return func

            return decorate

    def message(self, **filters):
        if 'msgtype' in filters and type(filters['msgtype']) in [str, int]:
            filters['msgtype'] = Protobuf.get_type(filters['msgtype'])

        return self.event(self.EVENT_MESSAGE_NAME, **filters)

    def exec_events(self, name, *args, **kwargs):
        if name not in self._events:
            return

        for f in self._events[name]:
            f(*args, **kwargs)
