"""
Main monitoring loop.

Workflow:
  1. Refresh KOSPI 200 constituent list
  2. Scrape Naver Finance for new analyst reports
  3. Filter to KOSPI 200 tickers only
  4. For each new report: download PDF → extract EPS via Gemini → detect upgrades → alert
"""

import os
import time
from dotenv import load_dotenv

from db.models import (
    get_conn, init_db, upsert_kospi200, get_kospi200,
    report_exists, insert_report, insert_eps, get_previous_eps,
)
from scraper.krx import fetch_kospi200
from scraper.naver import fetch_recent_reports, download_pdf
from extractor.gemini import extract_eps_from_pdf
from alerts.telegram import send_eps_upgrade_alert

load_dotenv()

EPS_UPGRADE_THRESHOLD = float(os.environ.get("EPS_UPGRADE_THRESHOLD", "0.02"))


def refresh_kospi200():
    print("[KRX] Fetching KOSPI 200 constituents...")
    constituents = fetch_kospi200()
    if constituents:
        upsert_kospi200(constituents)
        print(f"[KRX] Updated {len(constituents)} constituents.")
    else:
        print("[KRX] Warning: no constituents returned — check KRX scraper.")
    return constituents


def process_reports(conn, kospi200_tickers: set[str]):
    print("[Naver] Fetching recent analyst reports...")
    reports = fetch_recent_reports(pages=3, ticker_whitelist=kospi200_tickers)
    print(f"[Naver] Found {len(reports)} KOSPI 200 reports.")

    for report in reports:
        if report_exists(conn, report["report_url"]):
            continue  # already processed

        print(f"  Processing: {report['company']} ({report['ticker']}) — {report['broker']}")

        # Download PDF
        pdf_bytes = download_pdf(report["report_url"])
        if not pdf_bytes:
            print(f"    [!] Could not download PDF, skipping.")
            continue

        # Extract via Gemini
        extracted = extract_eps_from_pdf(pdf_bytes)
        if not extracted:
            print(f"    [!] Gemini extraction failed, skipping.")
            continue

        # Insert report record
        report_id = insert_report(conn, report)
        conn.commit()

        # Process each year's estimate
        for est in extracted.get("estimates", []):
            fiscal_year = est.get("fiscal_year")
            new_eps = est.get("fwd_eps")

            if not fiscal_year or new_eps is None:
                continue

            # Check for upgrade
            prev_eps = get_previous_eps(conn, report["ticker"], fiscal_year, report["broker"])

            estimate_row = {
                "report_id": report_id,
                "ticker": report["ticker"],
                "broker": report["broker"],
                "fiscal_year": fiscal_year,
                "fwd_eps": new_eps,
                "target_price": extracted.get("target_price"),
                "recommendation": extracted.get("recommendation"),
            }
            insert_eps(conn, estimate_row)
            conn.commit()

            # Alert if upgrade exceeds threshold
            if prev_eps and new_eps > prev_eps * (1 + EPS_UPGRADE_THRESHOLD):
                print(f"    [UPGRADE] {report['ticker']} {fiscal_year}E: {prev_eps:,.0f} → {new_eps:,.0f}")
                send_eps_upgrade_alert(
                    ticker=report["ticker"],
                    company=report["company"],
                    broker=report["broker"],
                    fiscal_year=fiscal_year,
                    prev_eps=prev_eps,
                    new_eps=new_eps,
                    target_price=extracted.get("target_price"),
                    recommendation=extracted.get("recommendation"),
                    report_url=report["report_url"],
                )

        time.sleep(1)  # be polite to Gemini API


def run_once():
    conn = get_conn()
    try:
        constituents = refresh_kospi200()
        kospi200_tickers = {c["ticker"] for c in constituents} if constituents else {
            r["ticker"] for r in get_kospi200(conn)
        }
        process_reports(conn, kospi200_tickers)
    finally:
        conn.close()


def run_scheduled(interval_minutes: int = 60):
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
        interval = int(os.environ.get("CHECK_INTERVAL_MINUTES", "60"))
        run_scheduled(interval)
