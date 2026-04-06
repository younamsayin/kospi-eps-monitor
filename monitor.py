"""
Main monitoring loop.

Workflow:
  1. Refresh KOSPI 200 constituent list
  2. Scrape Naver Finance + Bondweb for new analyst reports
  3. Filter to KOSPI 200 tickers / match company names
  4. For each new report: download PDF → extract EPS via Gemini → detect changes → alert
"""

import os
import time
from dotenv import load_dotenv

from db.models import (
    get_conn, init_db, upsert_kospi200, get_kospi200,
    report_exists, insert_report, insert_eps,
    get_previous_eps, get_previous_target_price,
)
from scraper.krx import fetch_kospi200
from scraper.naver import fetch_recent_reports as naver_fetch, download_pdf as naver_download
from scraper.bondweb import fetch_recent_reports as bondweb_fetch, download_pdf as bondweb_download
from extractor.gemini import extract_eps_from_pdf
from alerts.telegram import send_eps_change_alert, send_target_price_change_alert

load_dotenv()

EPS_CHANGE_THRESHOLD = float(os.environ.get("EPS_CHANGE_THRESHOLD",
                             os.environ.get("EPS_UPGRADE_THRESHOLD", "0.02")))
TP_CHANGE_THRESHOLD = float(os.environ.get("TP_CHANGE_THRESHOLD", "0.02"))
SCRAPE_PAGES = int(os.environ.get("SCRAPE_PAGES", "10"))


def refresh_kospi200():
    print("[KRX] Fetching KOSPI 200 constituents...")
    constituents = fetch_kospi200()
    if constituents:
        upsert_kospi200(constituents)
        print(f"[KRX] Updated {len(constituents)} constituents.")
    else:
        print("[KRX] Warning: no constituents returned — check KRX scraper.")
    return constituents


def process_report(conn, report: dict, pdf_bytes: bytes, kospi200_tickers: set):
    """Extract EPS from a PDF, save to DB, and alert on changes."""

    extracted = extract_eps_from_pdf(pdf_bytes)
    if not extracted:
        print(f"    [!] Gemini extraction failed, skipping.")
        return

    # For bondweb reports, ticker/company come from Gemini extraction
    if not report.get("ticker") and extracted.get("ticker"):
        report["ticker"] = extracted["ticker"]
        report["company"] = extracted.get("company", "")

    # Skip if still no ticker, or not in KOSPI 200
    if not report.get("ticker") or report["ticker"] not in kospi200_tickers:
        return

    # --- Target price change detection (once per report, before inserting estimates) ---
    new_tp = extracted.get("target_price")
    if new_tp:
        prev_tp = get_previous_target_price(conn, report["ticker"], report["broker"])
        if prev_tp and abs(new_tp - prev_tp) / abs(prev_tp) > TP_CHANGE_THRESHOLD:
            direction = "RAISED" if new_tp > prev_tp else "CUT"
            print(f"    [TP {direction}] {report['ticker']}: {prev_tp:,.0f} → {new_tp:,.0f}")
            send_target_price_change_alert(
                ticker=report["ticker"],
                company=report["company"],
                broker=report["broker"],
                prev_tp=prev_tp,
                new_tp=new_tp,
                recommendation=extracted.get("recommendation"),
                report_url=report["report_url"],
            )

    # --- Insert report and estimates ---
    report_id = insert_report(conn, report)
    conn.commit()

    for est in extracted.get("estimates", []):
        fiscal_year = est.get("fiscal_year")
        new_eps = est.get("fwd_eps")

        if not fiscal_year or new_eps is None:
            continue

        prev_eps = get_previous_eps(conn, report["ticker"], fiscal_year, report["broker"])

        insert_eps(conn, {
            "report_id": report_id,
            "ticker": report["ticker"],
            "broker": report["broker"],
            "fiscal_year": fiscal_year,
            "fwd_eps": new_eps,
            "target_price": new_tp,
            "recommendation": extracted.get("recommendation"),
        })
        conn.commit()

        # Alert if EPS changed beyond threshold (upgrade OR downgrade)
        if prev_eps and abs(new_eps - prev_eps) / abs(prev_eps) > EPS_CHANGE_THRESHOLD:
            direction = "UPGRADE" if new_eps > prev_eps else "DOWNGRADE"
            print(f"    [{direction}] {report['ticker']} {fiscal_year}E: {prev_eps:,.0f} → {new_eps:,.0f}")
            send_eps_change_alert(
                ticker=report["ticker"],
                company=report["company"],
                broker=report["broker"],
                fiscal_year=fiscal_year,
                prev_eps=prev_eps,
                new_eps=new_eps,
                target_price=new_tp,
                recommendation=extracted.get("recommendation"),
                report_url=report["report_url"],
            )


def run_source(conn, source_name: str, reports: list, download_fn, kospi200_tickers: set):
    print(f"[{source_name}] Found {len(reports)} reports.")

    for report in reports:
        if report_exists(conn, report["report_url"]):
            continue

        label = f"{report['company']} ({report['ticker']})" if report.get("ticker") else report["title"][:40]
        print(f"  Processing: {label} — {report['broker']}")

        pdf_bytes = download_fn(report["report_url"])
        if not pdf_bytes:
            print(f"    [!] Could not download PDF, skipping.")
            continue

        process_report(conn, report, pdf_bytes, kospi200_tickers)
        time.sleep(1)  # be polite to Gemini API


def run_once():
    conn = get_conn()
    try:
        constituents = refresh_kospi200()
        kospi200_tickers = {c["ticker"] for c in constituents} if constituents else {
            r["ticker"] for r in get_kospi200(conn)
        }
        # Build name→ticker map for bondweb company name matching
        kospi200_name_map = {c["company"]: c["ticker"] for c in constituents} if constituents else {}

        # Naver Finance — pre-filtered to KOSPI 200 tickers
        naver_reports = naver_fetch(pages=SCRAPE_PAGES, ticker_whitelist=kospi200_tickers)
        run_source(conn, "Naver", naver_reports, naver_download, kospi200_tickers)

        # Bondweb — pre-filtered by company name match; remaining get Gemini extraction
        bondweb_reports = bondweb_fetch(pages=SCRAPE_PAGES, ticker_whitelist=kospi200_name_map)
        run_source(conn, "Bondweb", bondweb_reports, bondweb_download, kospi200_tickers)

    finally:
        conn.close()


def run_scheduled(interval_minutes: int = 1440):
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    scheduler.add_job(run_once, "interval", minutes=interval_minutes)
    print(f"[Monitor] Starting — checking every {interval_minutes} minutes.")
    run_once()  # run immediately on start
    scheduler.start()


if __name__ == "__main__":
    import sys

    init_db()

    if "--once" in sys.argv:
        run_once()
    else:
        interval = int(os.environ.get("CHECK_INTERVAL_MINUTES", "1440"))
        run_scheduled(interval)
