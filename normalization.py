"""
Canonicalization helpers for broker names and analyst recommendations.

Broker aliases exist because Korean brokerages rename or merge; revision
tracking is keyed on the broker string, so every rename would otherwise
sever the per-broker revision chain.
"""

from typing import Optional

# alias -> canonical current name
BROKER_ALIASES = {
    # E*TRADE Korea -> 이베스트투자증권 (2015) -> LS증권 (2024)
    "이트레이드증권": "LS증권",
    "이베스트투자증권": "LS증권",
    "이베스트증권": "LS증권",
    # 하이투자증권 -> iM증권 (2024)
    "하이투자증권": "iM증권",
    # (KDB)대우증권 -> 미래에셋대우 (2016) -> 미래에셋증권 (2021)
    "대우증권": "미래에셋증권",
    "KDB대우증권": "미래에셋증권",
    "미래에셋대우": "미래에셋증권",
    # 메리츠종합금융증권 -> 메리츠증권 (2020)
    "메리츠종금증권": "메리츠증권",
    "메리츠종합금융증권": "메리츠증권",
    # 동부증권 -> DB금융투자 (2017) -> DB증권 (2025)
    "동부증권": "DB증권",
    "DB금융투자": "DB증권",
    # 현대증권 -> KB증권 (2017 merger)
    "현대증권": "KB증권",
    # 우리투자증권 -> NH투자증권 (2015 merger)
    "우리투자증권": "NH투자증권",
    # 동양증권 -> 유안타증권 (2014)
    "동양증권": "유안타증권",
    # 하나대투증권 -> 하나금융투자 (2015) -> 하나증권 (2022)
    "하나대투증권": "하나증권",
    "하나금융투자": "하나증권",
    # 신한금융투자 -> 신한투자증권 (2022)
    "신한금융투자": "신한투자증권",
    # 한화증권 -> 한화투자증권 (2013)
    "한화증권": "한화투자증권",
}

RECOMMENDATION_BUY = "BUY"
RECOMMENDATION_HOLD = "HOLD"
RECOMMENDATION_SELL = "SELL"
RECOMMENDATION_NOT_RATED = "NOT_RATED"

# matched against lowercase input with all whitespace removed
_RECOMMENDATION_MAP = {
    RECOMMENDATION_BUY: {
        "buy", "strongbuy", "tradingbuy", "outperform", "overweight",
        "accumulate", "add", "매수", "적극매수", "강력매수", "비중확대",
    },
    RECOMMENDATION_HOLD: {
        "hold", "neutral", "marketperform", "equalweight", "sectorperform",
        "inline", "중립", "보유", "시장수익률",
    },
    RECOMMENDATION_SELL: {
        "sell", "strongsell", "reduce", "underperform", "underweight",
        "매도", "비중축소",
    },
    RECOMMENDATION_NOT_RATED: {
        "notrated", "not-rated", "not_rated", "nr", "n/a", "na", "none",
        "norating", "-", "없음", "투자의견없음", "미제시",
    },
}

_RECOMMENDATION_LOOKUP = {
    token: norm
    for norm, tokens in _RECOMMENDATION_MAP.items()
    for token in tokens
}


def canonical_broker(broker: Optional[str]) -> Optional[str]:
    if not broker:
        return broker
    stripped = broker.strip()
    return BROKER_ALIASES.get(stripped, stripped)


def canonicalize_report_broker(report: dict) -> dict:
    """Rewrite report['broker'] to its canonical name, preserving the raw value."""
    raw = report.get("broker")
    canonical = canonical_broker(raw)
    if canonical != raw:
        report.setdefault("broker_raw", raw)
        report["broker"] = canonical
    return report


def normalize_recommendation(raw) -> Optional[str]:
    """Map a raw recommendation string to BUY/HOLD/SELL/NOT_RATED, or None if unknown."""
    if raw is None:
        return None
    key = "".join(str(raw).split()).lower()
    if not key:
        return None
    return _RECOMMENDATION_LOOKUP.get(key)
