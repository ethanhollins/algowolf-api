from ..spotware import Spotware

class TestSpotware(Spotware):

    def __init__(self, ctrl, user_account, broker_info):

        print(f"[TestSpotware] {broker_info}")

        # Inherit Parent Broker
        is_demo = broker_info["is_demo"]
        access_token = broker_info["access_token"]
        refresh_token = broker_info["refresh_token"]
        accounts = broker_info["accounts"]

        super().__init__(
            ctrl, is_demo, access_token=access_token, refresh_token=refresh_token,
            user_account=user_account, strategy_id="TEST", broker_id="spotware", accounts=accounts, 
            is_parent=False
        )
