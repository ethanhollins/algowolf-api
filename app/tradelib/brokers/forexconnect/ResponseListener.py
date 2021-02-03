from typing import Callable, List
import threading
import logging

from . import fxcorepy


class ResponseListener(fxcorepy.AO2GResponseListener):
    """The class implements the AO2GResponseListener abstract class and calls the passed functions on request completions/failures and table updates."""
    def __init__(self,
                 session: fxcorepy.O2GSession,
                 on_request_completed_callback: Callable[[str, fxcorepy.O2GResponse], bool] = None,
                 on_request_failed_callback: Callable[[str, str], bool] = None,
                 on_tables_updates_callback: Callable[[fxcorepy.O2GResponse], None] = None) -> None:
        """ The constructor.
        
            Parameters
            ----------
            session : O2GSession
                An instance of O2GSession.
            on_request_completed_callback : typing.Callable[[str, O2GResponse], bool]
                The function that is called when a notification about a request completion is received.
            on_request_failed_callback : typing.Callable[[str, str], bool]
                The function that is called when a notification about a request failure is received.
            on_tables_updates_callback : typing.Callable[O2GResponse, None]
                The function that is called when a notification about a table update is received.
        
            Returns
            -------
            None
        
        """
        super(ResponseListener, self).__init__()
        self._on_request_completed_callback = on_request_completed_callback
        self._on_request_failed_callback = on_request_failed_callback
        self._on_tables_updates_callback = on_tables_updates_callback
        self._response = None
        self._error = None
        self._event = threading.Event()
        self._request_ids = None
        self._session = session

    @property
    def is_set(self) -> bool:
        """Reserved for future use."""
        return self._event.is_set()

    @property
    def response(self) -> fxcorepy.O2GResponse:
        """ Gets an instance of O2GResponse if it has been received.
        
            Returns
            -------
            O2GResponse
        
        """
        return self._response

    @property
    def error(self) -> str:
        """ Gets a string representation of an error that occurred when processing a request.
        
            Returns
            -------
            str
        
        """
        return self._error

    @property
    def session(self) -> fxcorepy.O2GSession:
        """ Gets a string representation of an error that occurred when processing a request.
        
            Returns
            -------
            str
        
        """
        return self._session

    def set_request_id(self, request_id: str) -> None:
        """Reserved for future use."""
        self.set_request_ids([request_id])

    def on_request_completed(self,
                             request: str,
                             response: fxcorepy.O2GResponse) -> None:  # native call
        """ Implements the method AO2GEachRowListener.on_request_completed and calls the function that processes notifications about the successful request completion. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        logging.debug("Request completed %s", request)
        if request in self._request_ids:
            self._response = response
            result = None
            if self._on_request_completed_callback:
                result = self._on_request_completed_callback(request, response)
            self._request_ids.remove(request)
            if not self._request_ids and (result is None or (isinstance(result, bool) and result)):
                self.stop_waiting()

    def on_request_failed(self, request: str,
                          error: str) -> None:  # native call
        """Reserved for future use."""
        logging.error("Request failed %s: %s", request, error)
        if request in self._request_ids:
            self._error = error
            self._request_ids.remove(request)
            result = None
            if self._on_request_failed_callback:
                result = self._on_request_failed_callback(request, error)

            if not self._request_ids and (result is None or (isinstance(result, bool) and result)):
                self.stop_waiting()

    def on_tables_updates(self, response: fxcorepy.O2GResponse) -> None:  # native call
        """ Implements the method AO2GEachRowListener.on_tables_updates and calls the function that processes notifications about table updates. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        if self._on_tables_updates_callback:
            self._on_tables_updates_callback(response)

    def set_request_ids(self, request_ids: List[str]) -> None:
        """Reserved for future use."""
        self._request_ids = request_ids
        self.reset()

    def wait_event(self) -> bool:
        """Reserved for future use."""
        return self._event.wait(30)

    def stop_waiting(self) -> None:
        """ Stops waiting for a response.
        
            Returns
            -------
            None
        
        """
        self._event.set()

    def reset(self) -> None:
        """ Resets the response listener after a response is received.
        
            Returns
            -------
            None
        
        """
        self._error = None
        self._response = None
        self._event.clear()


class ResponseListenerAsync(fxcorepy.AO2GResponseListener):
    """"""
    def __init__(self, fc) -> None:
        """Reserved for future use."""
        super(ResponseListenerAsync, self).__init__()
        self._another_listener = []
        self._fc = fc

    def add_response_listener(self, another_listener: ResponseListener) -> None:
        """ 
        
            Parameters
            ----------
            another_listener : ResponseListener
                
        
            Returns
            -------
            None
        
        """

        if another_listener not in self._another_listener:
            self._another_listener.append(another_listener)

    def remove_response_listener(self, another_listener: ResponseListener):
        """ 
        
            Parameters
            ----------
            another_listener : ResponseListener
                
        
            Returns
            -------
            None
        
        """
        if another_listener not in self._another_listener:
            return
        self._another_listener.remove(another_listener)

    def on_request_completed(self,
                             request: str,
                             response: fxcorepy.O2GResponse) -> None:  # native call
        """Reserved for future use."""
        if len(self._another_listener) == 0:
            return
        for listener in self._another_listener:
            listener.on_request_completed(request, response)
            if listener.is_set:
                self.remove_response_listener(listener)

    def on_request_failed(self, request: str,
                          error: str) -> None:  # native call
        """Reserved for future use."""
        if len(self._another_listener) == 0:
            return
        for listener in self._another_listener:
            listener.on_request_failed(request, error)
            if listener.is_set:
                self.remove_response_listener(listener)

    def on_tables_updates(self, response: fxcorepy.O2GResponse) -> None:  # native call
        """Reserved for future use."""
        if len(self._another_listener) == 0:
            return
        for listener in self._another_listener:
            listener.on_tables_updates(response)
            if listener.is_set:
                self.remove_response_listener(listener)
