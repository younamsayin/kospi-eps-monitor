import os
import html
import logging
import httpx
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
logger = logging.getLogger(__name__)


def _send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    with httpx.Client(timeout=10) as client:
        resp = client.post(TELEGRAM_API.format(token=TELEGRAM_BOT_TOKEN), json=payload)
        if resp.status_code != 200:
            logger.warning("Telegram error %s: %s", resp.status_code, resp.text)


def send_eps_change_alert(
    ticker: str,
    company: str,
    broker: str,
    fiscal_year: int,
    prev_eps: float,
    new_eps: float,
    prev_report_date: Optional[str],
    new_report_date: Optional[str],
    target_price: Optional[int],
    recommendation: Optional[str],
    report_url: str,
):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured. Skipping alert for %s.", ticker)
        return

    pct_change = (new_eps - prev_eps) / abs(prev_eps) * 100 if prev_eps else 0
    is_upgrade = new_eps > prev_eps
    icon = "\U0001f4c8" if is_upgrade else "\U0001f4c9"  # chart up / chart down
    label = "EPS Upgrade" if is_upgrade else "EPS Downgrade"

    lines = [
        f"{icon} <b>{label}</b>",
        f"<b>{company}</b> ({ticker}) — {fiscal_year}E FWD EPS",
        f"  {prev_eps:,.0f} → <b>{new_eps:,.0f}</b> KRW/share ({pct_change:+.1f}%)",
        f"  Broker: {broker}",
    ]
    if prev_report_date or new_report_date:
        lines.append(f"  Dates: {prev_report_date or '-'} → {new_report_date or '-'}")
    if target_price:
        lines.append(f"  Target: {target_price:,}원")
    if recommendation:
        lines.append(f"  {recommendation}")
    safe_url = html.escape(report_url, quote=True)
    lines.append(f'  <a href="{safe_url}">View Report</a>')

    _send("\n".join(lines))


def send_target_price_change_alert(
    ticker: str,
    company: str,
    broker: str,
    prev_tp: float,
    new_tp: float,
    prev_report_date: Optional[str],
    new_report_date: Optional[str],
    recommendation: Optional[str],
    report_url: str,
):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    pct_change = (new_tp - prev_tp) / abs(prev_tp) * 100 if prev_tp else 0
    is_upgrade = new_tp > prev_tp
    icon = "\U0001f3af" if is_upgrade else "\U0001f53b"  # target / down triangle
    label = "TP Raised" if is_upgrade else "TP Cut"

    lines = [
        f"{icon} <b>{label}</b>",
        f"<b>{company}</b> ({ticker}) — Target Price",
        f"  {prev_tp:,.0f} → <b>{new_tp:,.0f}</b>원 ({pct_change:+.1f}%)",
        f"  Broker: {broker}",
    ]
    if prev_report_date or new_report_date:
        lines.append(f"  Dates: {prev_report_date or '-'} → {new_report_date or '-'}")
    if recommendation:
        lines.append(f"  {recommendation}")
    safe_url = html.escape(report_url, quote=True)
    lines.append(f'  <a href="{safe_url}">View Report</a>')

    _send("\n".join(lines))
