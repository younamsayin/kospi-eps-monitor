"""
Fetches current price and approximate listed share count from Naver's
mobile stock API. Used for target-price and EPS plausibility checks.

Share count is derived as market cap / price, which is precise enough for
catching order-of-magnitude extraction errors (the only thing it is used for).
"""

import os
import re
import time
import logging
from typing import Optional

import httpx

BASIC_URL = "https://m.stock.naver.com/api/stock/{ticker}/basic"
INTEGRATION_URL = "https://m.stock.naver.com/api/stock/{ticker}/integration"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
HEADERS = {"User-Agent": os.environ.get("SCRAPER_USER_AGENT", DEFAULT_USER_AGENT)}

logger = logging.getLogger(__name__)

_CACHE: dict = {}
_CACHE_TTL_SECONDS = 6 * 60 * 60

_KOREAN_UNIT_VALUES = {"조": 1_0000_0000_0000, "억": 1_0000_0000, "만": 1_0000}


def _parse_number(text) -> Optional[float]:
    if text is None:
        return None
    cleaned = re.sub(r"[,\s원]", "", str(text))
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_korean_amount(text) -> Optional[float]:
    """Parse amounts like '1,490조 8,010억' into a float (KRW)."""
    if not text:
        return None
    total = 0.0
    matched = False
    for number, unit in re.findall(r"([\d,\.]+)\s*(조|억|만)?", str(text)):
        if not number:
            continue
        value = _parse_number(number)
        if value is None:
            continue
        total += value * _KOREAN_UNIT_VALUES.get(unit, 1)
        matched = True
    return total if matched else None


def _fetch_json(client: httpx.Client, url: str) -> Optional[dict]:
    try:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Quote fetch failed (%s): %s", url, exc)
        return None


def get_quote(ticker: str) -> dict:
    """
    Returns {"price": Optional[float], "shares": Optional[float]} for a KRX ticker.
    Results are cached in-process; failures return None fields without raising.
    """
    ticker = str(ticker or "").strip().zfill(6)
    cached = _CACHE.get(ticker)
    if cached and time.time() - cached["fetched_at"] < _CACHE_TTL_SECONDS:
        return cached["quote"]

    quote = {"price": None, "shares": None}
    with httpx.Client(timeout=15, headers=HEADERS) as client:
        basic = _fetch_json(client, BASIC_URL.format(ticker=ticker))
        if basic:
            quote["price"] = _parse_number(basic.get("closePrice"))

        integration = _fetch_json(client, INTEGRATION_URL.format(ticker=ticker))
        if integration and quote["price"]:
            market_value = None
            for info in integration.get("totalInfos", []):
                if info.get("code") == "marketValue":
                    market_value = parse_korean_amount(info.get("value"))
                    break
            if market_value:
                quote["shares"] = market_value / quote["price"]

    _CACHE[ticker] = {"fetched_at": time.time(), "quote": quote}
    return quote
