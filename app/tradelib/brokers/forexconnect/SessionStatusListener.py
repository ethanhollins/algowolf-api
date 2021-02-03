from typing import Callable
import threading

from . import fxcorepy


class SessionStatusListener(fxcorepy.AO2GSessionStatus):
    """The class implements the abstract class AO2GSessionStatus and calls the passed function when the session status changes."""
    def __init__(self, session: fxcorepy.O2GSession, session_id: str, pin: str,
                 on_status_changed_callback: Callable[[fxcorepy.O2GSession, fxcorepy.AO2GSessionStatus.O2GSessionStatus], None] = None)\
            -> None:
        """ The constructor.
        
            Parameters
            ----------
            session : O2GSession
                An instance of O2GSession the session status listener listens to.
            session_id : str
                The identifier of the trading session. Must be one of the values returned by the
            pin : str
                The PIN code for the connection. If no PIN is required, specify an empty string ("").
            on_session_status_changed_callback : typing.Callable[[O2GSession, AO2GSessionStatus.O2GSessionStatus], None]
                The function that is called when the session status changes.
        
            Returns
            -------
            None
        
        """
        fxcorepy.AO2GSessionStatus.__init__(self)
        self.__session = session
        self.__session_id = session_id
        self.__pin = pin
        self.__semaphore = threading.Event()
        self.set_callback(on_status_changed_callback)
        self.__connected = False
        self.__disconnected = False
        self.__last_error = None

    def wait_event(self) -> bool:
        """Reserved for future use."""
        return self.__semaphore.wait(30)

    @property
    def connected(self) -> bool:
        """ Indicates if the session status is CONNECTED.
        
            Returns
            -------
            bool
        
        """
        return self.__connected

    @property
    def disconnected(self) -> bool:
        """ Indicates if the session status is DISCONNECTED.
        
            Returns
            -------
            bool
        
        """
        return self.__disconnected

    @property
    def last_error(self) -> str:
        """ Gets the last error received by the method on_login_failed.
        
            Returns
            -------
            str
        
        """
        return self.__last_error

    def reset(self) -> None:
        """ Resets the flag that is set when the session status changes to CONNECTED or DISCONNECTED.
        
            Returns
            -------
            None
        
        """
        self.__last_error = None
        self.__semaphore.clear()

    def set_callback(self, on_status_changed_callback: Callable[[fxcorepy.O2GSession,
                                                       fxcorepy.AO2GSessionStatus.O2GSessionStatus], None]) -> None:
        """ Sets a callback function.
        
            Parameters
            ----------
            on_session_status_changed_callback : typing.Callable[[O2GSession, AO2GSessionStatus.O2GSessionStatus], None]
                The function that is called when the session status changes.
        
            Returns
            -------
            None
        
        """
        self.__on_status_changed_callback = on_status_changed_callback

    def on_session_status_changed(self, status: fxcorepy.AO2GSessionStatus.O2GSessionStatus) -> None:  # native call
        """ Implements the method AO2GSessionStatus.on_session_status_changed and calls the function that processes notifications about the session status change. The function is passed in the constructor or set by the method set_callback.
        
            Returns
            -------
            
        
        """
        self.__connected = status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED
        self.__disconnected = status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.DISCONNECTED

        if self.__on_status_changed_callback is not None:
            self.__on_status_changed_callback(self.__session, status)

        if status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.CONNECTED or \
                status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.DISCONNECTED:
            self.__semaphore.set()

        if status == fxcorepy.AO2GSessionStatus.O2GSessionStatus.TRADING_SESSION_REQUESTED:
            if self.__session_id != "":
                self.__session.set_trading_session(self.__session_id, self.__pin)

    def on_login_failed(self, err: str) -> None:
        """ Implements the method AO2GSessionStatus.on_login_failed and saves the error in case of a failed login.
        
            Returns
            -------
            
        
        """
        self.__last_error = err
