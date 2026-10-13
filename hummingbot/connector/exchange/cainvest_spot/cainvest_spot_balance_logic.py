"""
Pure helpers for mapping Cainvest trading_line BRL into notional base balances.

Production path: CainvestSpotExchange.get_available_balance() uses
reference_mid_brl_per_base_for_trading_line() with live WS prices each time the
BudgetChecker asks, so balance is not stuck on wallet USDC between REST polls.

Kept dependency-free so unit tests can run without the full compiled Hummingbot stack.
"""
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple


def reference_mid_brl_per_base_for_trading_line(
    trading_pairs: List[str],
    last_prices: Dict[str, Dict[str, Any]],
) -> Optional[Tuple[Decimal, str]]:
    """
    Mid BRL price per 1 unit of base and base symbol, from WS last_prices (buy/sell).

    Uses the first *-BRL pair in trading_pairs that has valid buy/sell quotes.
    """
    if not last_prices:
        return None
    for pair in trading_pairs:
        if not pair.endswith("-BRL"):
            continue
        prices = last_prices.get(pair)
        if not prices:
            continue
        buy_p = prices.get("buy")
        sell_p = prices.get("sell")
        if buy_p is None or sell_p is None:
            continue
        buy_d = Decimal(str(buy_p))
        sell_d = Decimal(str(sell_p))
        if buy_d <= 0 or sell_d <= 0:
            continue
        base_symbol = pair.split("-")[0].upper()
        mid = (buy_d + sell_d) / Decimal("2")
        return (mid, base_symbol)
    return None


def apply_trading_line_to_base_balance(
    local_balances: Dict[str, Decimal],
    available_brl: Decimal,
    mid_and_base: Optional[Tuple[Decimal, str]],
) -> bool:
    """
    Set local_balances[base] = max(wallet_base, available_brl / mid) when inputs allow.

    :returns: True if a synthetic base bump was applied (synthetic > wallet before).
    """
    if available_brl <= 0 or mid_and_base is None:
        return False
    mid_brl_per_base, base_symbol = mid_and_base
    if mid_brl_per_base <= 0:
        return False
    wallet_base = local_balances.get(base_symbol, Decimal("0"))
    synthetic_base = available_brl / mid_brl_per_base
    effective = max(wallet_base, synthetic_base)
    local_balances[base_symbol] = effective
    return synthetic_base > wallet_base
