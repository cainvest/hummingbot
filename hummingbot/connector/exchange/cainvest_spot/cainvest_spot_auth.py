"""
Simple header-based auth for Cainvest Spot OTC API.
Uses X-API-Key and X-API-Secret headers for both REST and WS.
"""
from typing import Dict

from hummingbot.connector.exchange.cainvest_spot import cainvest_spot_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CainvestSpotAuth(AuthBase):

    def __init__(self, api_key: str, api_secret: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.api_secret = api_secret
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = dict(request.headers) if request.headers else {}
        headers[CONSTANTS.API_KEY_HEADER] = self.api_key
        headers[CONSTANTS.API_SECRET_HEADER] = self.api_secret
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        request.payload = self.get_ws_auth_payload()
        return request

    def get_ws_auth_payload(self) -> Dict[str, str]:
        return {
            "type": CONSTANTS.WS_LOGIN,
            "api_key": self.api_key,
            "api_secret": self.api_secret,
        }
