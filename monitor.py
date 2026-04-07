"""
Main monitoring loop.

Workflow:
  1. Refresh KOSPI 200 constituent list
  2. Scrape Naver Finance + Bondweb for new analyst reports
  3. Filter to KOSPI 200 tickers / match company names
  4. For each new report: download PDF → extract EPS via Gemini → detect changes → alert
"""

import os
import hashlib
import re
import time
import logging
from dotenv import load_dotenv

from db.models import (
    get_conn, init_db, upsert_kospi200, get_kospi200,
    report_exists, insert_report, insert_eps,
    get_previous_eps_record, get_previous_target_price_record,
    get_latest_prior_report_estimates,
    report_exists_by_pdf_hash,
    gemini_retry_exists_by_pdf_hash,
    upsert_gemini_retry, get_due_gemini_retries,
    delete_gemini_retry, mark_gemini_retry_failed,
)
from scraper.krx import fetch_kospi200
from scraper.naver import fetch_recent_reports as naver_fetch, download_pdf as naver_download
from scraper.bondweb import (
    fetch_recent_reports as bondweb_fetch,
    download_pdf as bondweb_download,
    log_download_failure_summary as bondweb_log_download_failure_summary,
    reset_download_failure_samples as bondweb_reset_download_failure_samples,
)
from extractor.gemini import extract_eps_from_pdf
from alerts.telegram import send_eps_change_alert, send_target_price_change_alert

load_dotenv()

EPS_CHANGE_THRESHOLD = float(os.environ.get("EPS_CHANGE_THRESHOLD",
                             os.environ.get("EPS_UPGRADE_THRESHOLD", "0.02")))
TP_CHANGE_THRESHOLD = float(os.environ.get("TP_CHANGE_THRESHOLD", "0.02"))
SCRAPE_PAGES = int(os.environ.get("SCRAPE_PAGES", "10"))
NAVER_SCRAPE_PAGES = int(os.environ.get("NAVER_SCRAPE_PAGES", os.environ.get("SCRAPE_PAGES", "30")))
BONDWEB_SCRAPE_PAGES = int(os.environ.get("BONDWEB_SCRAPE_PAGES", os.environ.get("SCRAPE_PAGES", "10")))
PROCESS_DELAY_SECONDS = float(os.environ.get("PROCESS_DELAY_SECONDS", "1"))
GEMINI_RETRY_DELAY_MINUTES = int(os.environ.get("GEMINI_RETRY_DELAY_MINUTES", "30"))
GEMINI_RETRY_LIMIT = int(os.environ.get("GEMINI_RETRY_LIMIT", "25"))
REPORTS_DIR = os.environ.get(
    "REPORTS_DIR",
    os.path.join(os.path.dirname(__file__), "reports"),
)
logger = logging.getLogger(__name__)
SKIP_PROGRESS_INTERVAL = max(1, int(os.environ.get("SKIP_PROGRESS_INTERVAL", "25")))


def _relative_gap(current: float, previous: float) -> float:
    if previous in (None, 0):
        return float("inf")
    return abs(current - previous) / abs(previous)


def _estimate_shift_score(current_map: dict, previous_map: dict, shift: int) -> tuple[float, int]:
    score = 0.0
    close_matches = 0
    for fiscal_year, current_eps in current_map.items():
        previous_eps = previous_map.get(fiscal_year + shift)
        if previous_eps is None:
            continue
        gap = _relative_gap(current_eps, previous_eps)
        score += max(0.0, 1.0 - min(gap, 1.0))
        if gap <= 0.05:
            close_matches += 1
    return score, close_matches


def _normalize_estimates(conn, report: dict, extracted: dict) -> list[dict]:
    estimates = extracted.get("estimates") or []
    normalized = []

    for est in estimates:
        if not isinstance(est, dict):
            continue
        fiscal_year = est.get("fiscal_year")
        try:
            fiscal_year = int(fiscal_year)
        except (TypeError, ValueError):
            continue
        est = dict(est)
        est["fiscal_year"] = fiscal_year
        normalized.append(est)

    if not normalized:
        return []

    report_year = None
    report_date = report.get("report_date")
    if report_date:
        try:
            report_year = int(str(report_date)[:4])
        except (TypeError, ValueError):
            report_year = None

    current_map = {
        est["fiscal_year"]: float(est["fwd_eps"])
        for est in normalized
        if est.get("fwd_eps") is not None
    }
    previous_map = {}
    if report.get("ticker") and report.get("broker") and report_date:
        previous_map = get_latest_prior_report_estimates(
            conn,
            report["ticker"],
            report["broker"],
            report_date,
        )

    if len(current_map) >= 3 and len(previous_map) >= 2:
        no_shift_score, no_shift_matches = _estimate_shift_score(current_map, previous_map, 0)
        shift_down_score, shift_down_matches = _estimate_shift_score(current_map, previous_map, -1)
        if shift_down_matches >= 2 and shift_down_score > no_shift_score + 0.75:
            for est in normalized:
                est["fiscal_year"] -= 1
            print(
                f"    [!] Adjusted fiscal years down by 1 for {report['ticker']} / {report['broker']} "
                f"based on same-broker prior report alignment."
            )

    deduped = {}
    for est in normalized:
        fy = est["fiscal_year"]
        if report_year is not None and fy < report_year:
            continue
        deduped[fy] = est

    return [deduped[fy] for fy in sorted(deduped)]


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


def _format_eta(seconds_remaining: float) -> str:
    seconds = max(0, int(seconds_remaining))
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _log_progress(
    source_name: str,
    idx: int,
    total_reports: int,
    processed: int,
    skipped_existing: int,
    skipped_download: int,
    skipped_duplicate_pdf: int,
    eta: str,
    message: str,
):
    logger.info(
        "[%s] [%s/%s] %s | processed=%s existing=%s download_fail=%s dup_pdf=%s ETA=%s",
        source_name,
        idx,
        total_reports,
        message,
        processed,
        skipped_existing,
        skipped_download,
        skipped_duplicate_pdf,
        eta,
    )


def _apply_extracted_metadata(report: dict, extracted: dict):
    if not report.get("ticker") and extracted.get("ticker"):
        report["ticker"] = extracted["ticker"]
        report["company"] = extracted.get("company", "")
    if extracted.get("report_date"):
        report["report_date"] = extracted["report_date"]


def _alert_target_price_change(conn, report: dict, extracted: dict):
    new_tp = extracted.get("target_price")
    if not new_tp:
        return

    prev_tp_row = get_previous_target_price_record(
        conn,
        report["ticker"],
        report["broker"],
        report["report_date"],
    )
    prev_tp = float(prev_tp_row["target_price"]) if prev_tp_row else None
    if prev_tp and abs(new_tp - prev_tp) / abs(prev_tp) > TP_CHANGE_THRESHOLD:
        direction = "RAISED" if new_tp > prev_tp else "CUT"
        logger.info("[TP %s] %s: %s -> %s", direction, report["ticker"], f"{prev_tp:,.0f}", f"{new_tp:,.0f}")
        send_target_price_change_alert(
            ticker=report["ticker"],
            company=report["company"],
            broker=report["broker"],
            prev_tp=prev_tp,
            new_tp=new_tp,
            prev_report_date=prev_tp_row["report_date"] if prev_tp_row else None,
            new_report_date=report["report_date"],
            recommendation=extracted.get("recommendation"),
            report_url=report["report_url"],
        )


def _insert_report_estimates(conn, report_id: int, report: dict, extracted: dict):
    new_tp = extracted.get("target_price")
    for est in extracted.get("estimates", []):
        fiscal_year = est.get("fiscal_year")
        new_eps = est.get("fwd_eps")

        if not fiscal_year or new_eps is None:
            continue

        prev_eps_row = get_previous_eps_record(
            conn,
            report["ticker"],
            fiscal_year,
            report["broker"],
            report["report_date"],
        )
        prev_eps = float(prev_eps_row["fwd_eps"]) if prev_eps_row else None

        insert_eps(conn, {
            "report_id": report_id,
            "ticker": report["ticker"],
            "broker": report["broker"],
            "fiscal_year": fiscal_year,
            "fwd_eps": new_eps,
            "target_price": new_tp,
            "recommendation": extracted.get("recommendation"),
        })

        if prev_eps and abs(new_eps - prev_eps) / abs(prev_eps) > EPS_CHANGE_THRESHOLD:
            direction = "UPGRADE" if new_eps > prev_eps else "DOWNGRADE"
            logger.info("[%s] %s %sE: %s -> %s", direction, report["ticker"], fiscal_year, f"{prev_eps:,.0f}", f"{new_eps:,.0f}")
            send_eps_change_alert(
                ticker=report["ticker"],
                company=report["company"],
                broker=report["broker"],
                fiscal_year=fiscal_year,
                prev_eps=prev_eps,
                new_eps=new_eps,
                prev_report_date=prev_eps_row["report_date"] if prev_eps_row else None,
                new_report_date=report["report_date"],
                target_price=new_tp,
                recommendation=extracted.get("recommendation"),
                report_url=report["report_url"],
            )


def refresh_kospi200():
    logger.info("[KRX] Fetching KOSPI 200 constituents...")
    constituents = fetch_kospi200()
    if constituents:
        upsert_kospi200(constituents)
        logger.info("[KRX] Updated %s constituents.", len(constituents))
    else:
        logger.warning("[KRX] Warning: no constituents returned — check KRX scraper.")
    return constituents


def _extract_and_save_report(conn, report: dict, pdf_bytes: bytes, kospi200_tickers: set) -> str:
    """Extract EPS from a PDF and save to DB. Returns a compact status string."""

    archived_path = report.get("local_pdf_path")
    if not archived_path:
        archived_path = _archive_report_pdf(report, pdf_bytes)
        report["local_pdf_path"] = archived_path
        logger.info("    Saved PDF: %s", archived_path)

    extracted = extract_eps_from_pdf(pdf_bytes)
    if not extracted:
        return "gemini_failed"

    _apply_extracted_metadata(report, extracted)

    # Skip if still no ticker, or not in KOSPI 200
    if not report.get("ticker") or report["ticker"] not in kospi200_tickers:
        return "non_kospi"

    extracted["estimates"] = _normalize_estimates(conn, report, extracted)

    _alert_target_price_change(conn, report, extracted)

    report_id = insert_report(conn, report)
    if not report_id:
        logger.warning("    [!] Report insert was ignored due to duplicate key, skipping estimates.")
        conn.rollback()
        return "insert_duplicate"
    _insert_report_estimates(conn, report_id, report, extracted)
    conn.commit()
    return "saved"


def _queue_gemini_retry(conn, report: dict, last_error: str):
    upsert_gemini_retry(conn, report, GEMINI_RETRY_DELAY_MINUTES, last_error)
    conn.commit()
    logger.warning(
        "    [!] Gemini extraction failed; retry scheduled in %s minutes.",
        GEMINI_RETRY_DELAY_MINUTES,
    )


def process_report(conn, report: dict, pdf_bytes: bytes, kospi200_tickers: set) -> str:
    """Extract EPS from a PDF, save to DB, and alert on changes."""
    status = _extract_and_save_report(conn, report, pdf_bytes, kospi200_tickers)
    if status == "gemini_failed":
        _queue_gemini_retry(conn, report, "Gemini extraction returned no usable data")
    return status


def process_due_gemini_retries(conn, kospi200_tickers: set):
    retries = get_due_gemini_retries(conn, GEMINI_RETRY_LIMIT)
    if not retries:
        return

    logger.info("[Gemini Retry] Processing %s due extraction retry item(s).", len(retries))
    succeeded = 0
    failed = 0
    skipped = 0

    for retry in retries:
        report = {
            "ticker": retry["ticker"],
            "company": retry["company"],
            "broker": retry["broker"],
            "source": retry["source"],
            "title": retry["title"],
            "report_url": retry["report_url"],
            "report_date": retry["report_date"],
            "pdf_hash": retry["pdf_hash"],
            "local_pdf_path": retry["local_pdf_path"],
        }
        label = f"{report.get('company')} ({report.get('ticker')}) — {report.get('broker')}"
        logger.info("[Gemini Retry] Retrying %s | attempt=%s", label, int(retry["attempts"] or 0) + 1)

        local_pdf_path = retry["local_pdf_path"]
        if not local_pdf_path or not os.path.exists(local_pdf_path):
            skipped += 1
            mark_gemini_retry_failed(
                conn,
                retry["id"],
                GEMINI_RETRY_DELAY_MINUTES,
                f"Archived PDF missing: {local_pdf_path}",
            )
            conn.commit()
            logger.warning("[Gemini Retry] Archived PDF missing, retry rescheduled: %s", local_pdf_path)
            continue

        if report_exists_by_pdf_hash(conn, retry["pdf_hash"]):
            skipped += 1
            delete_gemini_retry(conn, retry["id"])
            conn.commit()
            logger.info("[Gemini Retry] PDF already inserted, removing retry: %s", label)
            continue

        with open(local_pdf_path, "rb") as f:
            pdf_bytes = f.read()

        status = _extract_and_save_report(conn, report, pdf_bytes, kospi200_tickers)
        if status == "saved":
            succeeded += 1
            delete_gemini_retry(conn, retry["id"])
            conn.commit()
            logger.info("[Gemini Retry] Saved retry result: %s", label)
        elif status == "gemini_failed":
            failed += 1
            mark_gemini_retry_failed(
                conn,
                retry["id"],
                GEMINI_RETRY_DELAY_MINUTES,
                "Gemini extraction returned no usable data",
            )
            conn.commit()
            logger.warning(
                "[Gemini Retry] Gemini still failed; retry rescheduled in %s minutes: %s",
                GEMINI_RETRY_DELAY_MINUTES,
                label,
            )
        else:
            skipped += 1
            delete_gemini_retry(conn, retry["id"])
            conn.commit()
            logger.info("[Gemini Retry] Removing retry after status=%s: %s", status, label)

        time.sleep(PROCESS_DELAY_SECONDS)

    logger.info(
        "[Gemini Retry] Done. succeeded=%s failed=%s skipped=%s total=%s",
        succeeded,
        failed,
        skipped,
        len(retries),
    )


def run_source(conn, source_name: str, reports: list, download_fn, kospi200_tickers: set):
    total_reports = len(reports)
    logger.info("[%s] Found %s reports.", source_name, total_reports)
    if source_name == "Bondweb":
        bondweb_reset_download_failure_samples()

    processed = 0
    skipped_existing = 0
    skipped_download = 0
    skipped_duplicate_pdf = 0
    started_at = time.time()

    for idx, report in enumerate(reports, start=1):
        elapsed = time.time() - started_at
        avg_seconds = elapsed / max(1, idx - 1) if idx > 1 else 0
        remaining = total_reports - idx + 1
        eta = _format_eta(avg_seconds * remaining) if avg_seconds else "estimating..."

        if report_exists(conn, report["report_url"]):
            skipped_existing += 1
            if (
                skipped_existing == 1
                or skipped_existing % SKIP_PROGRESS_INTERVAL == 0
                or idx == total_reports
            ):
                _log_progress(
                    source_name,
                    idx,
                    total_reports,
                    processed,
                    skipped_existing,
                    skipped_download,
                    skipped_duplicate_pdf,
                    eta,
                    "Already saved, skipping",
                )
            continue

        label = f"{report['company']} ({report['ticker']})" if report.get("ticker") else report["title"][:40]
        _log_progress(
            source_name,
            idx,
            total_reports,
            processed,
            skipped_existing,
            skipped_download,
            skipped_duplicate_pdf,
            eta,
            f"Processing: {label} — {report['broker']}",
        )

        try:
            pdf_bytes = download_fn(report["report_url"], report)
        except TypeError:
            pdf_bytes = download_fn(report["report_url"])
        if not pdf_bytes:
            skipped_download += 1
            logger.warning("    [!] Could not download PDF, skipping.")
            continue

        pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()
        if report_exists_by_pdf_hash(conn, pdf_hash):
            skipped_duplicate_pdf += 1
            logger.warning("    [!] Duplicate PDF content already saved, skipping.")
            continue
        if gemini_retry_exists_by_pdf_hash(conn, pdf_hash):
            logger.warning("    [!] Gemini retry already scheduled for this PDF, skipping until retry time.")
            continue
        report["pdf_hash"] = pdf_hash

        archived_path = _archive_report_pdf(report, pdf_bytes)
        report["local_pdf_path"] = archived_path
        logger.info("    Saved PDF: %s", archived_path)

        status = process_report(conn, report, pdf_bytes, kospi200_tickers)
        if status == "saved":
            processed += 1
        time.sleep(PROCESS_DELAY_SECONDS)

    total_elapsed = time.time() - started_at
    logger.info(
        "[%s] Done in %s. processed=%s existing=%s download_fail=%s dup_pdf=%s total=%s",
        source_name,
        _format_eta(total_elapsed),
        processed,
        skipped_existing,
        skipped_download,
        skipped_duplicate_pdf,
        total_reports,
    )
    if source_name == "Bondweb" and skipped_download:
        bondweb_log_download_failure_summary()


def run_once():
    conn = get_conn()
    try:
        constituents = refresh_kospi200()
        cached_constituents = constituents if constituents else [dict(r) for r in get_kospi200(conn)]
        kospi200_tickers = {c["ticker"] for c in cached_constituents}
        # Build name→ticker map for bondweb company name matching.
        # Fall back to the cached DB snapshot so a transient refresh failure
        # does not send every Bondweb report through Gemini.
        kospi200_name_map = {c["company"]: c["ticker"] for c in cached_constituents}

        process_due_gemini_retries(conn, kospi200_tickers)

        # Naver Finance — pre-filtered to KOSPI 200 tickers
        naver_reports = naver_fetch(pages=NAVER_SCRAPE_PAGES, ticker_whitelist=kospi200_tickers)
        run_source(conn, "Naver", naver_reports, naver_download, kospi200_tickers)

        # Bondweb — pre-filtered by company name match; remaining get Gemini extraction
        bondweb_reports = bondweb_fetch(pages=BONDWEB_SCRAPE_PAGES, ticker_whitelist=kospi200_name_map)
        run_source(conn, "Bondweb", bondweb_reports, bondweb_download, kospi200_tickers)

    finally:
        conn.close()


def run_gemini_retry_once():
    conn = get_conn()
    try:
        kospi200_tickers = {row["ticker"] for row in get_kospi200(conn)}
        process_due_gemini_retries(conn, kospi200_tickers)
    finally:
        conn.close()


def run_scheduled(interval_minutes: int = 1440):
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    scheduler.add_job(run_once, "interval", minutes=interval_minutes)
    scheduler.add_job(run_gemini_retry_once, "interval", minutes=GEMINI_RETRY_DELAY_MINUTES)
    logger.info("[Monitor] Starting — checking every %s minutes.", interval_minutes)
    logger.info("[Gemini Retry] Checking retry queue every %s minutes.", GEMINI_RETRY_DELAY_MINUTES)
    run_once()  # run immediately on start
    scheduler.start()


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("google_genai.models").setLevel(logging.WARNING)
    logging.getLogger("google_genai.types").setLevel(logging.ERROR)

    init_db()

    if "--once" in sys.argv:
        run_once()
    else:
        interval = int(os.environ.get("CHECK_INTERVAL_MINUTES", "1440"))
        run_scheduled(interval)
