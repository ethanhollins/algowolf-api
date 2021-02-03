from typing import Callable, Any
from . import fxcorepy


class EachRowListener(fxcorepy.AO2GEachRowListener):
    """The class implements the abstract class AO2GEachRowListener and calls the passed function on the iteration through rows of a table."""
    def __init__(self, on_each_row_callback: Callable[[fxcorepy.AO2GEachRowListener, str, fxcorepy.O2GRow], None] = None,
                 data: Any = None) -> None:
        """ The constructor.
        
            Parameters
            ----------
            on_each_row_callback : 
                The function that is called on an iteration through rows of a table.
            data : typing.Any
                Any user's object. The default value of the parameter is None.
        
            Returns
            -------
            None
        
        """
        fxcorepy.AO2GEachRowListener.__init__(self)
        self._on_each_row_callback = on_each_row_callback
        self._data = data

    @property
    def data(self) -> Any:
        """ Gets data passed in the constructor.
        
            Returns
            -------
            typing.Any
        
        """
        return self._data

    def on_each_row(self, row_id: str, row_data: fxcorepy.O2GRow) -> None:  # native call
        """ Implements the method AO2GEachRowListener.on_each_row and calls the function that processes notifications on the iteration through the rows of a table. The function is passed in the constructor.
        
            Returns
            -------
            
        
        """
        if self._on_each_row_callback:
            self._on_each_row_callback(self, row_id, row_data)
