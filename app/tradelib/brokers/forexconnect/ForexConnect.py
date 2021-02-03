from typing import Callable, Dict, Any
import atexit
import threading
import logging
from datetime import datetime

import numpy as np
from . import fxcorepy
from .TableManagerListener import TableManagerListener
from .SessionStatusListener import SessionStatusListener
from .errors import (RequestFailedError,
                                 TableManagerError,
                                 LoginFailedError,
                                 TimeFrameError)
from .ResponseListener import ResponseListener, ResponseListenerAsync


@atexit.register
def on_exit():
    fxcorepy.O2GTransport.finalize_wrapper()


class ForexConnect:
    """The class is intended for working with a session."""
    _SESSION = None
    _STATUS_LISTENER = None
    _TABLE_MANAGER_LISTENER = None
    _START_URL = "http://fxcorporate.com/Hosts.jsp"
    _LISTENER = None

    OFFERS = fxcorepy.O2GTableType.OFFERS
    ACCOUNTS = fxcorepy.O2GTableType.ACCOUNTS
    TRADES = fxcorepy.O2GTableType.TRADES
    CLOSED_TRADES = fxcorepy.O2GTableType.CLOSED_TRADES
    ORDERS = fxcorepy.O2GTableType.ORDERS
    MESSAGES = fxcorepy.O2GTableType.MESSAGES
    SUMMARY = fxcorepy.O2GTableType.SUMMARY

    class TableUpdateType(fxcorepy.O2GTableUpdateType):
        pass

    class ResponseType(fxcorepy.O2GResponseType):
        pass

    class TableManagerStatus(fxcorepy.O2GTableManagerStatus):
        pass

    def __init__(self) -> None:
        self._session = ForexConnect._SESSION
        self._status_listener = ForexConnect._STATUS_LISTENER
        self._table_manager_listener = ForexConnect._TABLE_MANAGER_LISTENER
        self._prev_response_listener = None
        self._async_response_listener = ResponseListenerAsync(self)
        self._async_response_listener_subscribed = False
        self._start_url = ForexConnect._START_URL
        self._session_id = None
        self._pin = None
        self._table_manager = None
        self._com = None

    @property
    def session(self) -> fxcorepy.O2GSession:
        """ Gets an instance of the current session.
        
            Returns
            -------
            O2GSession
        
        """
        return self._session

    @property
    def table_manager(self) -> fxcorepy.O2GTableManager:
        """ Gets the current table manager of the session.
        
            Returns
            -------
            O2GTableManager
        
        """
        return self._table_manager

    @property
    def response_listener(self) -> ResponseListenerAsync:
        """Reserved for future use."""
        return self._async_response_listener

    @property
    def login_rules(self) -> fxcorepy.O2GLoginRules:
        """ Gets the rules used for the currently established session.
        
            Returns
            -------
            O2GLoginRules
        
        """
        login_rules = self._session.login_rules

        if login_rules is None:
            raise Exception("Cannot get login rules")

        return login_rules

    def __enter__(self):
        return self

    def __exit__(self, tp, value, traceback):
        self.logout()

    def _login(self,
               login_function,
               login_params,
               session_id: str = None,
               pin: str = None,
               session_status_callback: Callable[[fxcorepy.O2GSession,
                                                  fxcorepy.AO2GSessionStatus.O2GSessionStatus], None] = None,
               use_table_manager: bool = True,
               table_manager_callback: Callable[[fxcorepy.O2GTableStatus,
                                                 fxcorepy.O2GTableManager],
                                                None] = None) -> fxcorepy.O2GSession:

        self._session_id = session_id
        self._pin = pin
        self._session = fxcorepy.O2GTransport.create_session()

        if use_table_manager:
            self._table_manager_listener = TableManagerListener(table_manager_callback)
            self._session.use_table_manager(fxcorepy.O2GTableManagerMode.YES,
                                            self._table_manager_listener)

        self._status_listener = SessionStatusListener(self._session,
                                                      session_id,
                                                      pin,
                                                      session_status_callback)
        self._session.subscribe_session_status(self._status_listener)
        self._status_listener.reset()

        login_function = getattr(self._session, login_function)
        if not login_function(*login_params):
            raise LoginFailedError("The login method returned an exception."
                                   " This may be caused by the incorrect session status.")

        timeout = self._status_listener.wait_event()
        status_connect = self._status_listener.connected

        self._table_manager = self._session.table_manager

        if not timeout:
            raise LoginFailedError("Wait timeout exceeded")

        if not status_connect:
            if self._status_listener.last_error:
                raise LoginFailedError(self._status_listener.last_error)
            else:
                raise LoginFailedError("Not connected")

        if use_table_manager:
            self._table_manager_listener.wait_event()
            if self._table_manager.status != fxcorepy.O2GTableManagerStatus.TABLES_LOADED:
                raise LoginFailedError("Table manager not ready")

        return self._session

    def login(self,
              user_id: str,
              password: str,
              url: str = _START_URL,
              connection: str = "Demo",
              session_id: str = None,
              pin: str = None,
              session_status_callback: Callable[[fxcorepy.O2GSession,
                                                 fxcorepy.AO2GSessionStatus.O2GSessionStatus], None] = None,
              use_table_manager: bool = True,
              table_manager_callback: Callable[[fxcorepy.O2GTableStatus,
                                                fxcorepy.O2GTableManager],
                                               None] = None) -> fxcorepy.O2GSession:
        """ Creates a trading session and starts the connection with the specified trade server.
        
            Parameters
            ----------
            user_id : str
                The user name.
            password : str
                The password.
            url : str
                The URL of the server. The URL must be a full URL, including the path to the host descriptor.
            connection : str
                The name of the connection, for example Demo or Real.
            session_id : str
                The database name. Required only for users who have accounts in more than one database.
            pin : str
                The PIN code. Required only for users who have PIN codes.
            session_status_callback : typing.Callable[[O2GSession, AO2GSessionStatus.O2GSessionStatus], None]
                The function that is called when the session status changes.
            use_table_manager : bool
                Defines whether ForexConnect is started with the table manager.
            table_manager_callback : typing.Callable[[O2GTableManager, O2GTableStatus], None]
                The function that is called when the table manager status changes.
        
            Returns
            -------
            O2GSession
        
        """
        return self._login("login", (user_id, password, url, connection), session_id, pin, session_status_callback,
                           use_table_manager, table_manager_callback)

    def login_with_token(self,
                         user_id: str,
                         token: str,
                         url: str = _START_URL,
                         connection: str = "Demo",
                         session_id: str = None,
                         pin: str = None,
                         session_status_callback: Callable[[fxcorepy.O2GSession,
                                                            fxcorepy.AO2GSessionStatus.O2GSessionStatus], None] = None,
                         use_table_manager: bool = True,
                         table_manager_callback: Callable[[fxcorepy.O2GTableStatus,
                                                           fxcorepy.O2GTableManager],
                                                          None] = None) -> fxcorepy.O2GSession:
        """ Creates a second trading session and starts the connection with the specified trade server using a token.
        
            Parameters
            ----------
            user_id : str
                The user name.
            token : str
                The token.
            url : str
                The URL of the server. The URL must be a full URL, including the path to the host descriptor.
            connection : str
                The name of the connection, for example Demo or Real.
            session_id : str
                The database name. Required only for users who have accounts in more than one database.
            pin : str
                The PIN code. Required only for users who have PIN codes. The default value of the parameter is None.
            session_status_callback : typing.Callable[[O2GSession, AO2GSessionStatus.O2GSessionStatus], None]
                The function that is called when the session status changes. The default value of the parameter is None.
            use_table_manager : bool
                Defines whether ForexConnect is started with the table manager. The default value of the parameter is True.
            table_manager_callback : typing.Callable[[O2GTableStatus, O2GTableManager], None]
                The function that is called when the table manager status changes. The default value of the parameter is None
        
            Returns
            -------
            O2GSession
        
        """
        return self._login("login_with_token", (user_id, token, url, connection), session_id, pin,
                           session_status_callback, use_table_manager, table_manager_callback)

    def logout(self) -> None:
        self.unsubscribe_response()
        if self._session is None or \
                self._session.session_status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.DISCONNECTED:
            return
        """ Closes the trading session and connection with the trade server.
        
            Returns
            -------
            None
        
        """
        self._status_listener.reset()
        self._session.logout()
        self._status_listener.wait_event()
        self._session.unsubscribe_session_status(self._status_listener)

        self._session = None
        self._status_listener = None
        self._table_manager = None

    def set_session_status_listener(self, listener: Callable[
        [fxcorepy.O2GSession, fxcorepy.AO2GSessionStatus.O2GSessionStatus], None]) \
            -> None:
        """ Sets a session status listener on login or sets a new session status listener when necessary.
        
            Parameters
            ----------
            listener : typing.Callable[[O2GSession, AO2GSessionStatus.O2GSessionStatus], None]
                The function that is called when the session status changes.
        
            Returns
            -------
            None
        
        """
        self._status_listener.set_callback(listener)

    def get_table_reader(self, table_type: fxcorepy.O2GTable,
                         response: fxcorepy.O2GResponse = None) -> fxcorepy.O2GGenericTableResponseReader:
        """ Gets an instance of a table reader.
        
            Parameters
            ----------
            table_type : O2GTableType
                The identifier of the table (see O2GTableType).
            response : O2GResponse
                An instance of O2GResponse to get a reader for.
        
            Returns
            -------
            typing.object
        
        """
        if response is None:
            login_rules = self._session.login_rules
            response = login_rules.get_table_refresh_response(table_type)

        return self.create_reader(response)

    def get_table(self, table_type: fxcorepy.O2GTable) -> fxcorepy.O2GTable:
        """ Gets a specified table.
        
            Parameters
            ----------
            table_type : O2GTableType
                The identifier of the table. For a complete list of tables, see O2GTableType.
        
            Returns
            -------
            O2GTable
        
        """
        if self._table_manager_listener is None:
            raise TableManagerError(
                'Need login with flag "useTableManager"')

        if not self._table_manager_listener.wait_event():
            raise TableManagerError("Wait timeout exceeded")

        if self._table_manager_listener.status == fxcorepy.O2GTableManagerStatus.TABLES_LOAD_FAILED:
            raise TableManagerError(
                "Cannot refresh all tables of table manager")

        return self._session.table_manager.get_table(table_type)

    def subscribe_response(self) -> None:
        """Reserved for future use."""
        if self._async_response_listener_subscribed:
            return
        self._async_response_listener_subscribed = True
        self._session.subscribe_response(self._async_response_listener)

    def unsubscribe_response(self) -> None:
        """Reserved for future use."""
        if not self._async_response_listener_subscribed:
            return
        self._async_response_listener_subscribed = False
        self._session.unsubscribe_response(self._async_response_listener)

    def send_request_async(self,
                           request: fxcorepy.O2GRequest,
                           listener: ResponseListener = None):
        """ Sends a request.
        
            Parameters
            ----------
            request : O2GRequest
                An instance of O2GRequest.
            listener : ResponseListener
                An instance of ResponseListener. The default value of the parameter is None.
        
            Returns
            -------
            None
        
        """
        if listener is None:
            listener = ResponseListener(self._session)
        self.subscribe_response()
        request_ids = []
        Common.fill_request_ids(request_ids, request)
        listener.set_request_ids(request_ids)
        self._async_response_listener.add_response_listener(listener)
        self._session.send_request(request)

    def send_request(self,
                     request: fxcorepy.O2GRequest,
                     listener: ResponseListener = None) -> Any:
        """ Sends a request and returns the appropriate response reader or a bool value.
        
            Parameters
            ----------
            request : O2GRequest
                An instance of O2GRequest.
            listener : ResponseListener
                An instance of ResponseListener. The default value of the parameter is None.
        
            Returns
            -------
            typing.Any
        
        """
        if threading.current_thread() != threading.main_thread():
            logging.warning("Calling the send_request method is not from the main thread. "
                            "If you call the send_request method from a callback, you can get the application freezed. "
                            "It is recommended to use send_request_async method.")
        if listener is None:
            listener = ResponseListener(self._session)
        listener.reset()
        self.send_request_async(request, listener)

        if not listener.wait_event():
            self._async_response_listener.remove_response_listener(listener)
            raise RequestFailedError("Wait timeout exceeded")

        self._async_response_listener.remove_response_listener(listener)

        error = listener.error

        if error is None:
            return self.create_reader(listener.response)

        raise RequestFailedError(error)

    def get_history(self,
                    instrument: str,
                    timeframe: str,
                    date_from: datetime = None,
                    date_to: datetime = None,
                    quotes_count: int = -1,
                    candle_open_price_mode=fxcorepy.O2GCandleOpenPriceMode.PREVIOUS_CLOSE
                    ) -> np.ndarray:
        """ Gets price history of a certain instrument with a certain timeframe for a specified period or a certain number of bars/ticks.
        
            Parameters
            ----------
            instrument : str
                The symbol of the instrument. The instrument must be one of the instruments the ForexConnect session is subscribed to.
            timeframe : str
                The unique identifier of the timeframe. For details, see What is Timeframe?.
            date_from : datetime.datetime
                The date/time of the oldest bar/tick in the history.
            date_to : datetime.datetime
                The date/time of the most recent bar/tick in the history.
            quotes_count : int
                The number of bars/ticks in the history. The parameter is optional.
            candle_open_price_mode : 
                The candles open price mode that indicates how the open price is determined.
        
            Returns
            -------
            numpy.ndarray
        
        """
        if self._com is None:
            self._com = fxcorepy.PriceHistoryCommunicatorFactory.create_communicator(
                self._session, "./History")

        timeframe = self._com.timeframe_factory.create(timeframe)
        if not timeframe:
            raise TimeFrameError("Timeframe is incorrect")

        while not self._com.is_ready:
            pass

        self._com.candle_open_price_mode = candle_open_price_mode

        reader = self._com.get_history(instrument, timeframe, date_from, date_to, quotes_count)
        if timeframe.unit == fxcorepy.O2GTimeFrameUnit.TICK:
            result = np.zeros(len(reader), np.dtype([('Date', "M8[ns]"), ('Bid', "f8"), ("Ask", "f8")]))
            idx = -1
            for row in reader:
                idx += 1
                result[idx]['Date'] = np.datetime64(row.date)
                result[idx]['Bid'] = row.bid
                result[idx]['Ask'] = row.ask
            return result
        else:
            result = np.zeros(len(reader), np.dtype([
                ('Date', "M8[ns]"),
                ('BidOpen', 'f8'), ('BidHigh', 'f8'), ('BidLow', 'f8'), ('BidClose', 'f8'),
                ('AskOpen', 'f8'), ('AskHigh', 'f8'), ('AskLow', 'f8'), ('AskClose', 'f8'),
                ('Volume', 'i4')]))
            idx = -1
            for row in reader:
                idx += 1
                result[idx]['Date'] = np.datetime64(row.date)
                result[idx]['BidOpen'] = row.bid_open
                result[idx]['BidHigh'] = row.bid_high
                result[idx]['BidLow'] = row.bid_low
                result[idx]['BidClose'] = row.bid_close
                result[idx]['AskOpen'] = row.ask_open
                result[idx]['AskHigh'] = row.ask_high
                result[idx]['AskLow'] = row.ask_low
                result[idx]['AskClose'] = row.ask_close
                result[idx]['Volume'] = row.volume
            return result

    def create_reader(self,
                      response: fxcorepy.O2GResponse) -> Any:
        """ Creates a reader for a certain response using the method O2GResponseReaderFactory.create_reader.
        
            Parameters
            ----------
            response : O2GResponse
                An instance of O2GResponse.
        
            Returns
            -------
            typing.Any
        
        """
        factory = self._session.response_reader_factory
        if factory is None:
            raise Exception("Create ResponseReaderFactory failed")
        return factory.create_reader(response)

    def create_request(self, params: Dict[fxcorepy.O2GRequestParamsEnum, str],
                       request_factory: fxcorepy.O2GRequestFactory = None,
                       root: bool = True) -> fxcorepy.O2GRequest:
        """ Creates a request.
        
            Parameters
            ----------
            params : typing.Dict[O2GRequestParamsEnum, str]
                The request parameters. See O2GRequestParamsEnum.
            request_factory : O2GRequestFactory
                An instance of O2GRequestFactory.
            root : bool
                Defines whether the request is root. The default value of the parameter is True.
        
            Returns
            -------
            O2GRequest
        
        """
        if request_factory is None:
            request_factory = self._session.request_factory

            if request_factory is None:
                raise Exception("Can not create request factory")

        value_map = request_factory.create_value_map()
        items = params.items()

        for k, v in items:
            if isinstance(v, dict):
                value_map.append_child(
                    self.create_request(v, request_factory, False))

            else:
                if isinstance(v, str):
                    value_map.set_string(k, v)

                if isinstance(v, int):
                    value_map.set_int(k, v)

                if isinstance(v, bool):
                    value_map.set_bool(k, v)

                if isinstance(v, float):
                    value_map.set_double(k, v)

        if root:
            request = request_factory.create_order_request(value_map)
            if request is None:
                raise Exception(request_factory.last_error)
            return request

        else:
            return value_map

    def create_order_request(self, order_type: str,
                             command: fxcorepy.Constants.Commands = fxcorepy.Constants.Commands.CREATE_ORDER,
                             **kwargs: str) -> fxcorepy.O2GRequest:
        """ Creates a request for creating an order of a specified type using specified parameters.
        
            Parameters
            ----------
            order_type : str
                The type of the order. See Contansts.Orders.
            command : Commands
                The command. The default value of the parameter is fxcorepy.Constants.Commands.CreateOrder.
            str : kwargs
                The order parameters. For a full list of possible request parameters, see O2GRequestParamsEnum.
        
            Returns
            -------
            O2GRequest
        
        """
        if command is None:
            command = fxcorepy.Constants.Commands.CREATE_ORDER

        params = {
            fxcorepy.O2GRequestParamsEnum.COMMAND: command,
            fxcorepy.O2GRequestParamsEnum.ORDER_TYPE: order_type
        }

        for param in kwargs:
            enum = fxcorepy.O2GRequestParamsEnum.names[param]
            params[enum] = kwargs[param]

        return self.create_request(params)

    def get_timeframe(self, str_timeframe: str) -> fxcorepy.O2GTimeFrame:
        """ Gets an instance of a timeframe.
        
            Parameters
            ----------
            str_timeframe : str
                The unique identifier of the timeframe.
        
            Returns
            -------
            O2GTimeframe
        
        """
        return self._session.request_factory.timeframe_collection.get_by_id(
            str_timeframe)

    @staticmethod
    def parse_timeframe(timeframe: str) -> tuple:
        """ Parses a timeframe into O2GTimeframeUnit and the number of units.
        
            Parameters
            ----------
            timeframe : str
                The unique identifier of the timeframe.
        
            Returns
            -------
            O2GTimeframeUnit, int
        
        """
        if (len(timeframe) < 2):
            raise TimeFrameError("Timeframe is incorrect")

        available_units = {
            't': fxcorepy.O2GTimeFrameUnit.TICK,
            'm': fxcorepy.O2GTimeFrameUnit.MIN,
            'H': fxcorepy.O2GTimeFrameUnit.HOUR,
            'D': fxcorepy.O2GTimeFrameUnit.DAY,
            'W': fxcorepy.O2GTimeFrameUnit.WEEK,
            'M': fxcorepy.O2GTimeFrameUnit.MONTH,
        }

        try:
            unit = available_units[timeframe[0]]
        except KeyError as e:
            raise TimeFrameError("Unit is incorrect")

        try:
            size = int(timeframe[1:])
        except ValueError:
            raise TimeFrameError("Size must be a number")

        return unit, size

from forexconnect.common import Common