import os
import httpx
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[Alert] Telegram not configured. Skipping alert for {ticker}.")
        return

    pct_change = (new_eps - prev_eps) / abs(prev_eps) * 100 if prev_eps else 0

    lines = [
        f"📈 <b>EPS Upgrade</b>",
        f"<b>{company}</b> ({ticker}) — {fiscal_year}E FWD EPS",
        f"  {prev_eps:,.0f} → <b>{new_eps:,.0f}</b> KRW/share ({pct_change:+.1f}%)",
        f"  Broker: {broker}",
    ]
    if target_price:
        lines.append(f"  Target: {target_price:,}원")
    if recommendation:
        lines.append(f"  {recommendation}")
    lines.append(f'  <a href="{report_url}">View Report</a>')

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": "\n".join(lines),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    with httpx.Client(timeout=10) as client:
        resp = client.post(
            TELEGRAM_API.format(token=TELEGRAM_BOT_TOKEN),
            json=payload,
        )
        if resp.status_code != 200:
            print(f"[Alert] Telegram error {resp.status_code}: {resp.text}")
