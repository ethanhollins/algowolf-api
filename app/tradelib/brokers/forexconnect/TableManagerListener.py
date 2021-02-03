import threading
from typing import Callable

from . import fxcorepy


class TableManagerListener(fxcorepy.AO2GTableManagerListener):
    """The class implements the abstract class AO2GTableManagerListener and calls the passed function when the table manager status changes."""
    def __init__(self, on_status_changed_callback: Callable[
        [fxcorepy.AO2GTableManagerListener, fxcorepy.O2GTableManager, fxcorepy.O2GTableManagerStatus], None] = None) \
            -> None:
        """ The constructor.
        
            Parameters
            ----------
            on_status_changed_callback : typing.Callable[[AO2GTableManagerListener, O2GTableManager, O2GTableManagerStatus], None]
                The function that is called when the table manager status changes.
        
            Returns
            -------
            None
        
        """
        fxcorepy.AO2GTableManagerListener.__init__(self)
        self._on_status_changed_callback = on_status_changed_callback
        self._status = fxcorepy.O2GTableManagerStatus.TABLES_LOADING
        self._semaphore = threading.Event()

    @property
    def status(self) -> fxcorepy.O2GTableManagerStatus:
        """ Gets the current status of the table manager.
        
            Returns
            -------
            O2GTableManagerStatus
        
        """
        return self._status

    def set_callback(self, on_status_changed_callback: Callable[
        [fxcorepy.AO2GTableManagerListener, fxcorepy.O2GTableManager, fxcorepy.O2GTableManagerStatus], None]) \
            -> None:
        """ Sets a callback function.
        
            Parameters
            ----------
            on_status_changed_callback : 
                The function that is called when the table manager status changes.
        
            Returns
            -------
            None
        
        """
        self._on_status_changed_callback = on_status_changed_callback

    def on_status_changed(self, status: fxcorepy.O2GTableManagerStatus, table_manager: fxcorepy.O2GTableManager) \
            -> None:  # native call
        """ Implements the method AO2GTableManagerListener.on_status_changed and calls the function that processes notifications about the table manager status change. The function is passed in the constructor or set by the set_callback method.
        
            Returns
            -------
            
        
        """
        self._status = status
        if self._on_status_changed_callback:
            self._on_status_changed_callback(self, table_manager, status)

        if status != fxcorepy.O2GTableManagerStatus.TABLES_LOADING:
            self._semaphore.set()

    def wait_event(self) -> bool:
        """Reserved for future use."""
        return self._semaphore.wait(30)

    def reset(self) -> None:
        """ Resets the flag that is set when the table manager status changes to TABLES_LOADED or TABLES_LOAD_FAILED.
        
            Returns
            -------
            None
        
        """
        self._status = fxcorepy.O2GTableManagerStatus.TABLES_LOADING
        self._semaphore.clear()
