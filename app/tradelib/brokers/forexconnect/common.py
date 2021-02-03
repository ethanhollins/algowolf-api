from typing import Callable, List, Any
import pandas
from . import fxcorepy
from .ForexConnect import ForexConnect
from .TableListener import TableListener
from .errors import TableManagerError


class Common:
    """The class contains helper functions."""
    @staticmethod
    def create_table_listener(table: fxcorepy.O2GTable = None,
                              on_add_callback: Callable[
                                  [fxcorepy.AO2GTableListener, str,
                                   fxcorepy.O2GRow], None
                              ] = None,
                              on_delete_callback: Callable[
                                  [fxcorepy.AO2GTableListener, str,
                                   fxcorepy.O2GRow], None
                              ] = None,
                              on_change_callback: Callable[
                                  [fxcorepy.AO2GTableListener, str,
                                   fxcorepy.O2GRow], None
                              ] = None,
                              on_status_change_callback: Callable[
                                  [fxcorepy.AO2GTableListener,
                                   fxcorepy.O2GTableStatus], None] = None) -> TableListener:
        """ Creates a table listener.
        
            Parameters
            ----------
            on_add_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row is added to the table.
            on_delete_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row is deleted from the table.
            on_change_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row in the table is changed.
            on_status_change_callback : typing.Callable[[AO2GTableListener, O2GTableStatus], None]
                The function that is called when the table status is changed.
        
            Returns
            -------
            TableListener
        
        """
        table_listener = TableListener(
            table, on_change_callback, on_add_callback, on_delete_callback, on_status_change_callback
        )

        return table_listener

    @staticmethod
    def subscribe_table_updates(table: fxcorepy.O2GTable = None,
                                on_add_callback: Callable[
                                    [fxcorepy.AO2GTableListener, str, fxcorepy.O2GRow], None] = None,
                                on_delete_callback: Callable[
                                    [fxcorepy.AO2GTableListener, str, fxcorepy.O2GRow], None] = None,
                                on_change_callback: Callable[
                                    [fxcorepy.AO2GTableListener, str, fxcorepy.O2GRow], None] = None,
                                on_status_change_callback: Callable[
                                    [fxcorepy.AO2GTableListener, fxcorepy.O2GTableStatus], None] = None) \
            -> TableListener:
        """ Creates a table listener and subscribes it to updates of a certain table.
        
            Parameters
            ----------
            table : O2GTable
                An instance of O2GTable.
            on_add_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row is added to the table.
            on_delete_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row is deleted from the table.
            on_change_callback : typing.Callable[[AO2GTableListener, str, O2GRow], None]
                The function that is called when a row in the table is changed.
            on_status_change_callback : typing.Callable[[AO2GTableListener, O2GTableStatus], None]
                The function that is called when the table status is changed.
        
            Returns
            -------
            TableListener
        
        """
        table_listener = Common.create_table_listener(table, on_add_callback, on_delete_callback,
                                                      on_change_callback, on_status_change_callback)
        table_listener.subscribe()
        return table_listener

    @staticmethod
    def join_to_new_group_request(fc: ForexConnect,
                                  account_id: str,
                                  primary_id: str,
                                  secondary_id: str,
                                  contingency_type: int) -> fxcorepy.O2GRequest:
        """ Creates a request for joining two specifed orders to a new contingency group.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            account_id : str
                The unique identifier of the account the orders belong to.
            primary_id : str
                The unique identifier of the order that will be primary in the contingency group.
            secondary_id : str
                The unique identifier of the order that will be secondary in the contingency group.
            contingency_type : int
                The type of the contingency group to be created.
        
            Returns
            -------
            O2GRequest
        
        """
        request_params = {
            fxcorepy.O2GRequestParamsEnum.COMMAND: fxcorepy.Constants.Commands.JOIN_TO_NEW_CONTINGENCY_GROUP,
            fxcorepy.O2GRequestParamsEnum.CONTINGENCY_GROUP_TYPE: contingency_type,
            primary_id: {
                fxcorepy.O2GRequestParamsEnum.ORDER_ID: primary_id,
                fxcorepy.O2GRequestParamsEnum.ACCOUNT_ID: account_id},
            secondary_id: {
                fxcorepy.O2GRequestParamsEnum.ORDER_ID: secondary_id,
                fxcorepy.O2GRequestParamsEnum.ACCOUNT_ID: account_id}
        }
        return fc.create_request(request_params)

    @staticmethod
    def is_order_exist(fc: ForexConnect, account_id: str, order_id: str) -> bool:
        """ Checks whether an order for a certain instrument exists on a specific account.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            account_id : str
                The unique identifier of the account.
            offer_id : str
                The unique identification number of the instrument.
        
            Returns
            -------
            bool
        
        """
        response_reader = Common.refresh_table_by_account(fc, fc.ORDERS, account_id)

        for order_row in response_reader:
            if order_id == order_row.order_id:
                return True
        return False

    @staticmethod
    def refresh_table_by_account(fc: ForexConnect,
                                 table: fxcorepy.O2GTable,
                                 account_id: str) -> Any:
        """ Refreshes a table for a certain account.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            table : O2GTable
                An instance of O2GTable.
            account_id : str
                The account identifier.
        
            Returns
            -------
            O2GGenericTableResponseReader
        
        """
        request_factory = fc.session.request_factory

        if request_factory is None:
            raise Exception("Cannot create request factory")

        request = request_factory.create_refresh_table_request_by_account(table, account_id)

        if request is None:
            raise Exception(request_factory.last_error)

        return fc.send_request(request)

    @staticmethod
    def is_contingency_exist(fc: ForexConnect, account_id: str, contingency_id: str) -> bool:
        """ Checks whether a contingency group exists on a specific account.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            account_id : str
                The unique identifier of the account.
            contingency_id : str
                The unique identifier of the contingency group.
        
            Returns
            -------
            bool
        
        """
        response_reader = Common.refresh_table_by_account(fc, fc.ORDERS, account_id)

        for order_row in response_reader:
            if contingency_id == order_row.contingent_order_id:
                return True
        return False

    @staticmethod
    def fill_request_ids(request_ids: List[str],
                         request: fxcorepy.O2GRequest) -> None:
        """Reserved for future use."""
        children_count = len(request)

        if children_count == 0:
            request_ids.append(request.request_id)
            return

        for i in range(children_count):
            Common.fill_request_ids(request_ids, request[i])

    @staticmethod
    def get_account(fc: ForexConnect, account_id=None, additional_check_func:  Callable[
        [fxcorepy.O2GAccountRow], bool] = None) \
            -> fxcorepy.O2GAccountRow:
        """ Gets an instance of O2GAccountRow by Account ID or searches for a trading account with no limitations on account operations and/or satisfying a certain condition.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            account_id : str
                The unique identifier of the account.
            additional_check_func : typing.Callable[[O2GAccountRow], bool]
                The function that is called if a user specifies an additional check for the account.
        
            Returns
            -------
            O2GAccountRow
        
        """
        accounts_response_reader = fc.get_table_reader(
            fxcorepy.O2GTableType.ACCOUNTS)
        for account in accounts_response_reader:
            account_kind = account.account_kind
            if account_kind == "32" or account_kind == "36":
                if account.margin_call_flag == "N" and \
                        (account_id is None or account_id == account.account_id):
                    if additional_check_func is not None:
                        if additional_check_func(account):
                            return account
                    else:
                        return account
        return None

    @staticmethod
    def get_offer(fc: ForexConnect, instrument: str) -> fxcorepy.O2GOfferRow:
        """ Gets an instance of O2GOfferRow for a specified instrument.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            instrument : str
                The name of the instrument.
        
            Returns
            -------
            O2GOfferRow
        
        """
        try:
            offers_table = fc.get_table(fxcorepy.O2GTableType.OFFERS)
            for offer in offers_table:
                if offer.instrument == instrument:
                    if offer.subscription_status == "T":
                        return offer
        except TableManagerError:
            offers_response_reader = fc.get_table_reader(
                fxcorepy.O2GTableType.OFFERS)
            for offer in offers_response_reader:
                if offer.instrument == instrument and offer.subscription_status == "T":
                    return offer
        return None

    @staticmethod
    def get_trade(fc: ForexConnect, account_id: str, offer_id: str) -> fxcorepy.O2GTradeTableRow:
        """ Gets an instance of O2GTradeRow from the Trades table by Account ID and Offer ID.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            account_id : str
                The identifier of the account the position is opened on.
            offer_id : str
                The unique identification number of the instrument the position is opened in.
        
            Returns
            -------
            O2GTradeRow
        
        """
        try:
            trades_table = fc.get_table(fxcorepy.O2GTableType.TRADES)
            for trade in trades_table:
                if trade.account_id == account_id and trade.offer_id == offer_id:
                    return trade
        except TableManagerError:
            trades_response_reader = fc.get_table_reader(
                fxcorepy.O2GTableType.TRADES)
            for trade in trades_response_reader:
                if trade.account_id == account_id and trade.offer_id == offer_id:
                    return trade
        return None

    @staticmethod
    def _create_close_all_request_buy(account_id, offer_id):
        return {
            fxcorepy.O2GRequestParamsEnum.COMMAND: fxcorepy.Constants.Commands.CREATE_ORDER,
            fxcorepy.O2GRequestParamsEnum.NET_QUANTITY: "Y",
            fxcorepy.O2GRequestParamsEnum.ORDER_TYPE: fxcorepy.Constants.Orders.TRUE_MARKET_CLOSE,
            fxcorepy.O2GRequestParamsEnum.ACCOUNT_ID: account_id,
            fxcorepy.O2GRequestParamsEnum.OFFER_ID: offer_id,
            fxcorepy.O2GRequestParamsEnum.BUY_SELL: fxcorepy.Constants.BUY}

    @staticmethod
    def _create_close_all_request_sell(account_id, offer_id):
        return {
            fxcorepy.O2GRequestParamsEnum.COMMAND: fxcorepy.Constants.Commands.CREATE_ORDER,
            fxcorepy.O2GRequestParamsEnum.NET_QUANTITY: "Y",
            fxcorepy.O2GRequestParamsEnum.ORDER_TYPE: fxcorepy.Constants.Orders.TRUE_MARKET_CLOSE,
            fxcorepy.O2GRequestParamsEnum.ACCOUNT_ID: account_id,
            fxcorepy.O2GRequestParamsEnum.OFFER_ID: offer_id,
            fxcorepy.O2GRequestParamsEnum.BUY_SELL: fxcorepy.Constants.SELL}

    @staticmethod
    def create_close_trades_request(fc: ForexConnect, trade_ids: List[str] = None) -> fxcorepy.O2GRequest:
        """ Creates a request for closing a number of positions.
        
            Parameters
            ----------
            trade_ids : typing.List[str]
                The identifiers of the positions to be closed.
        
            Returns
            -------
            O2GRequest
        
        """
        request_params = {
            fxcorepy.O2GRequestParamsEnum.COMMAND: fxcorepy.Constants.Commands.CREATE_ORDER}
        trades_table = fc.get_table(ForexConnect.TRADES)
        order_info = {}
        for trade in trades_table:
            if trade_ids is None or trade.trade_id in trade_ids:
                offer_id = trade.offer_id
                buy_sell = trade.buy_sell
                account_id = trade.account_id
                s_key = "{0}_{1}".format(account_id, offer_id)
                if buy_sell == fxcorepy.Constants.BUY:
                    if s_key not in order_info or fxcorepy.Constants.BUY not in order_info[s_key]:
                        request_params[offer_id] = Common._create_close_all_request_sell(account_id, offer_id)
                        if s_key not in order_info:
                            order_info[s_key] = {}
                        order_info[s_key][fxcorepy.Constants.BUY] = True
                else:
                    if s_key not in order_info or fxcorepy.Constants.SELL not in order_info[s_key]:
                        request_params[offer_id] = Common._create_close_all_request_buy(account_id, offer_id)
                        if s_key not in order_info:
                            order_info[s_key] = {}
                        order_info[s_key][fxcorepy.Constants.SELL] = True
        return fc.create_request(request_params)

    @staticmethod
    def add_orders_into_group_request(fc: ForexConnect,
                                      account_id: str,
                                      contingency_id: str,
                                      order_ids: List[str],
                                      contingency_type: int) -> fxcorepy.O2GRequest:
        """ Creates a request for adding certain orders to an existing contingency group.
        
            Parameters
            ----------
            fc : ForexConnect
                An instance of ForexConnect.
            account_id : str
                The identifier of the account the orders belong to.
            contingency_id : str
                The identifier of an existing contingency group.
            order_ids : typing.List[str]
                The identifiers of the orders to be added to the contingency group.
            contingency_type : int
                The type of the contingency group.
        
            Returns
            -------
            O2GRequest
        
        """
        request_params = {
            fxcorepy.O2GRequestParamsEnum.COMMAND: fxcorepy.Constants.Commands.JOIN_TO_EXISTING_CONTINGENCY_GROUP,
            fxcorepy.O2GRequestParamsEnum.CONTINGENCY_GROUP_TYPE: contingency_type,
            fxcorepy.O2GRequestParamsEnum.CONTINGENCY_ID: contingency_id}

        for str_order_id in order_ids:
            child_request = {
                fxcorepy.O2GRequestParamsEnum.ORDER_ID: str_order_id,
                fxcorepy.O2GRequestParamsEnum.ACCOUNT_ID: account_id}
            request_params[str_order_id] = child_request

        return fc.create_request(request_params)

    @staticmethod
    def convert_table_to_dataframe(table: fxcorepy.O2GTable) -> pandas.DataFrame:
        """ Converts O2GTable to a pandas.DataFrame.
        
            Parameters
            ----------
            table : O2GTable
                An instance of O2GTable.
        
            Returns
            -------
            pandas.DataFrame
        
        """
        column_names = []
        for column in table.columns:
            if column.is_key:
                continue
            column_names.append(column.id)

        data = []
        index = []
        for row in table:
            key_value = row[table.columns.key_column.id]
            r = {}
            for column in column_names:
                r[column] = row[column]
            data.append(r)
            index.append(key_value)
        return pandas.DataFrame(data, index=index, columns=column_names).sort_index(axis=1).sort_index()

    @staticmethod
    def convert_row_to_dataframe(row: fxcorepy.O2GRow) -> pandas.DataFrame:
        """ Converts O2GRow to a pandas.DataFrame.
        
            Parameters
            ----------
            row : O2GRow
                An instance of O2GRow.
        
            Returns
            -------
            pandas.DataFrame
        
        """
        row_obj = {}
        column_names = []
        for column in row.columns:
            if column.is_key:
                continue
            row_obj[column.id] = row[column.id]
            column_names.append(column.id)
        return pandas.DataFrame([row_obj], index=[row[row.columns.key_column.id]],
                                columns=column_names).sort_index(axis=1)
