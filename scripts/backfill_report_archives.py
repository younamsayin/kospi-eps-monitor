import argparse
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import get_conn
from scraper.naver import download_pdf as naver_download
from scraper.bondweb import download_pdf as bondweb_download


REPORTS_DIR = os.environ.get("REPORTS_DIR", os.path.join(REPO_ROOT, "reports"))
DOWNLOADERS = {
    "naver": naver_download,
    "bondweb": bondweb_download,
}


def _safe_filename_part(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^\w\-.]+", "_", (value or "").strip(), flags=re.UNICODE)
    cleaned = cleaned.strip("._")
    return cleaned or fallback


def _archive_report_pdf(report: dict, pdf_bytes: bytes) -> str:
    report_date = str(report.get("report_date") or "unknown-date")
    source = _safe_filename_part(str(report.get("source") or "unknown"), "unknown")
    ticker = _safe_filename_part(str(report.get("ticker") or "unknown"), "unknown")
    company = _safe_filename_part(str(report.get("company") or "unknown"), "unknown")
    broker = _safe_filename_part(str(report.get("broker") or "unknown"), "unknown")
    title = _safe_filename_part(str(report.get("title") or "report"), "report")

    target_dir = os.path.join(REPORTS_DIR, company, source, report_date)
    os.makedirs(target_dir, exist_ok=True)

    filename = f"{ticker}_{company}_{broker}_{title}.pdf"
    path = os.path.join(target_dir, filename)
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(pdf_bytes)
    return path


def _existing_archive_path(report: dict) -> str:
    report_date = str(report.get("report_date") or "unknown-date")
    source = _safe_filename_part(str(report.get("source") or "unknown"), "unknown")
    ticker = _safe_filename_part(str(report.get("ticker") or "unknown"), "unknown")
    company = _safe_filename_part(str(report.get("company") or "unknown"), "unknown")
    broker = _safe_filename_part(str(report.get("broker") or "unknown"), "unknown")
    title = _safe_filename_part(str(report.get("title") or "report"), "report")
    return os.path.join(REPORTS_DIR, company, source, report_date, f"{ticker}_{company}_{broker}_{title}.pdf")


def _load_reports(conn: sqlite3.Connection, source: Optional[str], limit: Optional[int]):
    sql = """
        SELECT id, ticker, company, broker, source, title, report_url, report_date
        FROM analyst_reports
        WHERE COALESCE(report_url, '') != ''
    """
    params = []
    if source:
        sql += " AND source = ?"
        params.append(source)
    sql += " ORDER BY report_date DESC, id DESC"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, tuple(params)).fetchall()


def _download_pdf(report: dict) -> Optional[bytes]:
    source = (report.get("source") or "").lower()
    downloader = DOWNLOADERS.get(source)
    if not downloader:
        return None
    try:
        return downloader(report["report_url"], report)
    except TypeError:
        return downloader(report["report_url"])


def main():
    parser = argparse.ArgumentParser(description="Backfill archived PDF files for existing analyst_reports rows.")
    parser.add_argument(
        "--source",
        choices=["naver", "bondweb"],
        help="Only backfill one source. Omit to process the entire DB.",
    )
    parser.add_argument("--limit", type=int, help="Only inspect the most recent N reports.")
    parser.add_argument("--sleep", type=float, default=0.25, help="Delay between downloads in seconds.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the expected archive path already exists.",
    )
    args = parser.parse_args()

    conn = get_conn()
    try:
        rows = _load_reports(conn, args.source, args.limit)
    finally:
        conn.close()

    total = len(rows)
    archived = 0
    skipped_existing = 0
    skipped_unsupported = 0
    failed = 0

    print(
        f"Backfilling archives for {total} report(s)"
        + (f" from source={args.source}" if args.source else " across all sources")
        + "."
    )

    for idx, row in enumerate(rows, start=1):
        report = dict(row)
        expected_path = _existing_archive_path(report)
        if not args.force and os.path.exists(expected_path):
            skipped_existing += 1
            if skipped_existing == 1 or skipped_existing % 25 == 0 or idx == total:
                print(
                    f"[{idx}/{total}] Already archived, skipping "
                    f"(archived={archived} existing={skipped_existing} failed={failed} unsupported={skipped_unsupported})"
                )
            continue

        if (report.get("source") or "").lower() not in DOWNLOADERS:
            skipped_unsupported += 1
            print(
                f"[{idx}/{total}] Unsupported source for report {report['id']}: {report.get('source')!r}"
            )
            continue

        label = f"{report.get('company', '')} ({report.get('ticker', '')}) — {report.get('broker', '')}"
        print(f"[{idx}/{total}] Downloading {label}")
        pdf_bytes = _download_pdf(report)
        if not pdf_bytes:
            failed += 1
            print("    [!] Could not download PDF for archiving.")
            continue

        path = _archive_report_pdf(report, pdf_bytes)
        archived += 1
        print(f"    Saved PDF: {path}")
        if args.sleep:
            time.sleep(args.sleep)

    print(
        f"Done. archived={archived} existing={skipped_existing} "
        f"failed={failed} unsupported={skipped_unsupported} total={total}"
    )


if __name__ == "__main__":
    main()
