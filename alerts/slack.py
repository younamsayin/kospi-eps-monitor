"""
Sends EPS upgrade alerts to Slack via webhook.
"""

import os
import httpx
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def send_eps_upgrade_alert(
    ticker: str,
    company: str,
    broker: str,
    fiscal_year: int,
    prev_eps: float,
    new_eps: float,
    target_price: Optional[int],
    recommendation: Optional[str],
    report_url: str,
):
    if not WEBHOOK_URL:
        print(f"[Alert] No Slack webhook configured. Skipping alert for {ticker}.")
        return

    pct_change = (new_eps - prev_eps) / abs(prev_eps) * 100 if prev_eps else 0
    arrow = "▲" if new_eps > prev_eps else "▼"

    text = (
        f"*EPS Upgrade Alert* {arrow}\n"
        f"*{company}* ({ticker}) — {fiscal_year}E FWD EPS\n"
        f"  {prev_eps:,.0f} → *{new_eps:,.0f}* KRW/share ({pct_change:+.1f}%)\n"
        f"  Broker: {broker}"
    )
    if target_price:
        text += f" | Target: {target_price:,}원"
    if recommendation:
        text += f" | {recommendation}"
    text += f"\n  <{report_url}|View Report>"

    payload = {"text": text}
    with httpx.Client(timeout=10) as client:
        resp = client.post(WEBHOOK_URL, json=payload)
        if resp.status_code != 200:
            print(f"[Alert] Slack error {resp.status_code}: {resp.text}")
