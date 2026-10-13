from decimal import Decimal

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "USDC-BRL"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
)


def hb_pair_to_cainvest(trading_pair: str) -> str:
    """USDC-BRL -> USDC/BRL"""
    return trading_pair.replace("-", "/")


def cainvest_pair_to_hb(currency_pair: str) -> str:
    """USDC/BRL -> USDC-BRL"""
    return currency_pair.replace("/", "-")


class CainvestSpotConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="cainvest_spot", client_data=None)
    use_auth_for_public_endpoints: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Use authentication for public endpoints? (required for Cainvest WS market data)",
            "prompt_on_new": False,
            "is_connect_key": True,
        },
    )
    cainvest_spot_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Cainvest API key (ak_...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    cainvest_spot_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Cainvest API secret (sk_...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    cainvest_spot_environment: str = Field(
        default="production",
        json_schema_extra={
            "prompt": "Enter the environment (sandbox or production)",
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    class Config:
        title = "cainvest_spot"


KEYS = CainvestSpotConfigMap.construct()

OTHER_DOMAINS = []
OTHER_DOMAINS_PARAMETER = {}
OTHER_DOMAINS_EXAMPLE_PAIR = {}
OTHER_DOMAINS_DEFAULT_FEES = {}
OTHER_DOMAINS_KEYS = {}
