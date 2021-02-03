from typing import Callable

from . import fxcorepy


class TableListener(fxcorepy.AO2GTableListener):
    """The class implements the abstract class AO2GTableListener and calls the passed functions on the appropriate trading tables events: adding/changing/deleting of rows and table status changes."""
    def __init__(self,
                 table: fxcorepy.O2GTable = None,
                 on_changed_callback: Callable[
                     [fxcorepy.AO2GTableListener, str, fxcorepy.O2GRow], None
                 ] = None,
                 on_added_callback: Callable[
                     [fxcorepy.AO2GTableListener, str, fxcorepy.O2GRow], None
                 ] = None,
                 on_deleted_callback: Callable[
                     [fxcorepy.AO2GTableListener, str, fxcorepy.O2GRow], None
                 ] = None,
                 on_status_changed_callback: Callable[
                     [fxcorepy.AO2GTableListener,
                      fxcorepy.O2GTableStatus], None] = None
                 ) -> None:
        """ The constructor.
        
            Parameters
            ----------
            table : 
                An instance of O2GTable.
            on_added_callback : O2GTable)= None, (typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row is added to the table.
            on_deleted_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row is deleted from the table.
            on_changed_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row in the table is changed.
            on_status_changed_callback : typing.Callable[[AO2GTableListener, O2GTableStatus], None]
                The function that is called when a table status is changed.
        
            Returns
            -------
            None
        
        """
        fxcorepy.AO2GTableListener.__init__(self)
        self._on_changed_callback = on_changed_callback
        self._on_added_callback = on_added_callback
        self._on_deleted_callback = on_deleted_callback
        self._on_status_changed_callback = on_status_changed_callback
        self._table = table

    def on_added(self, row_id: str, row: fxcorepy.O2GRow) -> None:  # native call
        """ Implements the method AO2GTableListener.on_added and calls the function that processes notifications about the row addition to a table. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        if self._on_added_callback:
            self._on_added_callback(self, row_id, row)

    def on_changed(self, row_id: str, row: fxcorepy.O2GRow) -> None:  # native call
        """ Implements the method AO2GTableListener.on_changed and calls the function that processes notifications about the row change in a table. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        if self._on_changed_callback:
            self._on_changed_callback(self, row_id, row)

    def on_deleted(self, row_id: str, row: fxcorepy.O2GRow) -> None:  # native call
        """ Implements the method AO2GTableListener.on_deleted and calls the function that processes notifications about the row deletion from a table. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        if self._on_deleted_callback:
            self._on_deleted_callback(self, row_id, row)

    def on_status_changed(self, status: fxcorepy.O2GTableStatus) -> None:  # native call
        """ Implements the method AO2GTableListener.on_status_changed and calls the function that processes notifications about the table status change. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        if self._on_status_changed_callback:
            self._on_status_changed_callback(self, status)

    def _unsubscribe(self, type_u):
        self._table.unsubscribe_update(type_u, self)

    def unsubscribe(self) -> None:
        """ Unsubscribes a table listener from table updates.
        
            Returns
            -------
            None
        
        """
        if self._table is not None:
            if self._on_changed_callback is not None:
                self._unsubscribe(fxcorepy.O2GTableUpdateType.UPDATE)
            if self._on_deleted_callback is not None:
                self._unsubscribe(fxcorepy.O2GTableUpdateType.DELETE)
            if self._on_added_callback is not None:
                self._unsubscribe(fxcorepy.O2GTableUpdateType.INSERT)
            if self._on_status_changed_callback is not None:
                self._table.unsubscribe_status(self)

    def subscribe(self, table: fxcorepy.O2GTable = None) -> None:
        """ Subscribes a table listener to updates of a certain table.
        
            Parameters
            ----------
            table : O2GTable
                An instance of O2GTable.
        
            Returns
            -------
            None
        
        """
        if self._table is None:
            if table is None:
                raise ValueError("Table is not set")
            self._table = table
        if self._on_changed_callback is not None:
            self._table.subscribe_update(fxcorepy.O2GTableUpdateType.UPDATE, self)
        if self._on_deleted_callback is not None:
            self._table.subscribe_update(fxcorepy.O2GTableUpdateType.DELETE, self)
        if self._on_added_callback is not None:
            self._table.subscribe_update(fxcorepy.O2GTableUpdateType.INSERT, self)
        if self._on_status_changed_callback is not None:
            self._table.subscribe_status(self)
