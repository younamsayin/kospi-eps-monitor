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
import json
import re
import time
import logging
import sqlite3
import threading
from datetime import datetime
from typing import Optional, Tuple
from dotenv import load_dotenv

from db.models import (
    get_conn, init_db, upsert_kospi200, upsert_kosdaq150, get_active_universe,
    insert_report, insert_eps, insert_tp_event,
    get_report_by_url, count_eps_estimates_for_report,
    insert_ingestion_event, insert_gemini_extraction,
    get_existing_report_urls,
    get_previous_eps_record, get_previous_target_price_record,
    get_latest_prior_report_estimates, get_latest_prior_report_tp,
    has_newer_report,
    report_exists_by_pdf_hash,
    get_existing_pdf_hashes,
    get_pending_gemini_retry_hashes,
    upsert_gemini_retry, get_due_gemini_retries,
    delete_gemini_retry, mark_gemini_retry_failed,
    wal_checkpoint,
)
from normalization import canonicalize_report_broker, normalize_recommendation
from scraper.quote import get_quote
from scraper.krx import fetch_kospi200, fetch_kosdaq150
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
GEMINI_RETRY_MAX_ATTEMPTS = max(1, int(os.environ.get("GEMINI_RETRY_MAX_ATTEMPTS", "5")))
REPORTS_DIR = os.environ.get(
    "REPORTS_DIR",
    os.path.join(os.path.dirname(__file__), "reports"),
)
INGESTION_LOGS_DIR = os.environ.get(
    "INGESTION_LOGS_DIR",
    os.path.join(os.path.dirname(__file__), "logs"),
)
logger = logging.getLogger(__name__)
WAL_CHECKPOINT_INTERVAL = max(1, int(os.environ.get("WAL_CHECKPOINT_INTERVAL", "50")))
DB_OPERATION_RETRIES = max(1, int(os.environ.get("DB_OPERATION_RETRIES", "3")))
DB_OPERATION_RETRY_DELAY_SECONDS = float(os.environ.get("DB_OPERATION_RETRY_DELAY_SECONDS", "2"))
DB_FAILURE_ABORT_THRESHOLD = max(1, int(os.environ.get("DB_FAILURE_ABORT_THRESHOLD", "3")))
ENABLE_KOSDAQ150 = os.environ.get("ENABLE_KOSDAQ150", "false").strip().lower() in {"1", "true", "yes", "on"}
GEMINI_RETRY_LOCK = threading.Lock()

# ── Quality-check configuration ──────────────────────────────────────────────
# Master kill-switch: set QUALITY_CHECKS_ENABLED=false to restore pre-quality
# ingestion behavior without any code rollback.
QUALITY_CHECKS_ENABLED = os.environ.get("QUALITY_CHECKS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
# A same-broker change larger than this triggers a confirmation re-extraction.
OUTLIER_CONFIRM_THRESHOLD = float(os.environ.get("OUTLIER_CONFIRM_THRESHOLD", "0.5"))
# Two extraction passes must agree within this relative tolerance.
OUTLIER_AGREE_TOLERANCE = float(os.environ.get("OUTLIER_AGREE_TOLERANCE", "0.02"))
# Target price plausibility bounds relative to the current market price.
TP_PRICE_MIN_RATIO = float(os.environ.get("TP_PRICE_MIN_RATIO", "0.2"))
TP_PRICE_MAX_RATIO = float(os.environ.get("TP_PRICE_MAX_RATIO", "5.0"))
# EPS vs net-profit/share-count cross-check: flag when the best unit
# interpretation is still off by more than this factor.
EPS_NP_MISMATCH_FACTOR = float(os.environ.get("EPS_NP_MISMATCH_FACTOR", "2.0"))
# Keep the scraper listing date when the Gemini-extracted date drifts further than this.
REPORT_DATE_MAX_DRIFT_DAYS = int(os.environ.get("REPORT_DATE_MAX_DRIFT_DAYS", "7"))
# Stronger model used for Gemini retries (attempt >= 2) and outlier confirmation.
GEMINI_ESCALATION_MODEL = os.environ.get("GEMINI_ESCALATION_MODEL", "gemini-3.1-pro-preview").strip() or None


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

    # Keep FY(N-1) for Jan-Mar reports: Korean annual results are announced in
    # Feb-Mar, so the prior fiscal year is still a live forward estimate then.
    report_month = None
    if report_date:
        try:
            report_month = int(str(report_date)[5:7])
        except (TypeError, ValueError):
            report_month = None
    min_fiscal_year = report_year
    if report_year is not None and report_month is not None and report_month <= 3:
        min_fiscal_year = report_year - 1

    deduped = {}
    for est in normalized:
        fy = est["fiscal_year"]
        if min_fiscal_year is not None and fy < min_fiscal_year:
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


def _report_label(report: dict) -> str:
    company = report.get("company") or "Unknown company"
    ticker = report.get("ticker")
    broker = report.get("broker") or "Unknown broker"
    report_date = report.get("report_date") or "unknown date"
    title = (report.get("title") or "untitled").strip()
    ticker_part = f" ({ticker})" if ticker else ""
    return f"{company}{ticker_part} — {broker} — {report_date} — {title}"


def _is_disk_io_error(exc: Exception) -> bool:
    return "disk i/o error" in str(exc).lower()


def _reopen_conn(conn, source_name: str = "SQLite", context: str = "reopening DB connection"):
    try:
        conn.close()
    except Exception:
        pass
    for attempt in range(DB_OPERATION_RETRIES):
        try:
            return get_conn()
        except sqlite3.OperationalError as exc:
            if not _is_disk_io_error(exc) or attempt == DB_OPERATION_RETRIES - 1:
                raise
            delay = DB_OPERATION_RETRY_DELAY_SECONDS * (attempt + 1)
            logger.warning(
                "[%s] SQLite disk I/O error while %s; retrying in %ss: %s",
                source_name,
                context,
                delay,
                exc,
            )
            time.sleep(delay)
    return get_conn()


def _load_skip_sets(conn, source_name: str) -> tuple[dict, object]:
    for attempt in range(DB_OPERATION_RETRIES):
        try:
            skip_sets = {
                "report_urls": get_existing_report_urls(conn),
                "pdf_hashes": get_existing_pdf_hashes(conn),
                "retry_hashes": get_pending_gemini_retry_hashes(conn),
            }
            logger.info(
                "[%s] Loaded skip cache: urls=%s pdf_hashes=%s retry_hashes=%s",
                source_name,
                len(skip_sets["report_urls"]),
                len(skip_sets["pdf_hashes"]),
                len(skip_sets["retry_hashes"]),
            )
            return skip_sets, conn
        except sqlite3.OperationalError as exc:
            if not _is_disk_io_error(exc) or attempt == DB_OPERATION_RETRIES - 1:
                raise
            delay = DB_OPERATION_RETRY_DELAY_SECONDS * (attempt + 1)
            logger.warning(
                "[%s] SQLite disk I/O error while loading skip cache; reopening DB connection and retrying in %ss: %s",
                source_name,
                delay,
                exc,
            )
            conn = _reopen_conn(conn, source_name, "opening DB connection for skip-cache retry")
            time.sleep(delay)
    return {"report_urls": set(), "pdf_hashes": set(), "retry_hashes": set()}, conn


def _save_extracted_report_with_retry(conn, report: dict, extracted: dict, source_name: str, idx: int, total_reports: int, label: str) -> tuple[str, object, bool]:
    for attempt in range(DB_OPERATION_RETRIES):
        try:
            conn = _reopen_conn(conn, source_name, "opening DB connection before report save")
            wal_checkpoint(conn)  # clear stale WAL state before writing
            return _save_extracted_report(conn, report, extracted), conn, False
        except sqlite3.OperationalError as exc:
            if not _is_disk_io_error(exc):
                raise
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt == DB_OPERATION_RETRIES - 1:
                logger.exception(
                    "[%s] [%s/%s] SQLite disk I/O error while saving extracted report after %s attempt(s); archived PDF kept and report skipped for this run: %s",
                    source_name,
                    idx,
                    total_reports,
                    DB_OPERATION_RETRIES,
                    label,
                )
                return "db_failed", conn, True
            delay = DB_OPERATION_RETRY_DELAY_SECONDS * (attempt + 1)
            logger.warning(
                "[%s] [%s/%s] SQLite disk I/O error while saving extracted report; retrying in %ss with a fresh DB connection: %s",
                source_name,
                idx,
                total_reports,
                delay,
                exc,
            )
            time.sleep(delay)
    return "db_failed", conn, True


def _empty_source_stats(source_name: str) -> dict:
    return {
        "source": source_name,
        "scanned": 0,
        "downloaded": 0,
        "archived": 0,
        "saved": 0,
        "existing_url": 0,
        "duplicate_pdf": 0,
        "download_failed": 0,
        "retry_pending": 0,
        "gemini_failed": 0,
        "db_failed": 0,
        "subject_mismatch": 0,
        "non_kospi": 0,
        "insert_duplicate": 0,
        "other_not_saved": 0,
        "elapsed_seconds": 0,
    }


def _log_run_summary(source_stats: list[dict], retry_stats: dict, elapsed_seconds: float):
    totals = _empty_source_stats("Total")
    for stats in source_stats:
        for key, value in stats.items():
            if key in ("source",):
                continue
            totals[key] = totals.get(key, 0) + value

    logger.info("=" * 72)
    logger.info("[Run Summary] Completed in %s", _format_eta(elapsed_seconds))
    for stats in source_stats:
        logger.info(
            "[Run Summary] %s: scanned=%s downloaded=%s archived=%s saved=%s "
            "existing_url=%s duplicate_pdf=%s download_fail=%s gemini_fail=%s db_fail=%s "
            "retry_pending=%s subject_mismatch=%s non_kospi=%s insert_duplicate=%s other_not_saved=%s elapsed=%s",
            stats["source"],
            stats["scanned"],
            stats["downloaded"],
            stats["archived"],
            stats["saved"],
            stats["existing_url"],
            stats["duplicate_pdf"],
            stats["download_failed"],
            stats["gemini_failed"],
            stats["db_failed"],
            stats["retry_pending"],
            stats["subject_mismatch"],
            stats["non_kospi"],
            stats["insert_duplicate"],
            stats["other_not_saved"],
            _format_eta(stats["elapsed_seconds"]),
        )

    logger.info(
        "[Run Summary] Total source reports: scanned=%s downloaded=%s archived=%s saved=%s "
        "existing_url=%s duplicate_pdf=%s download_fail=%s gemini_fail=%s db_fail=%s "
        "retry_pending=%s subject_mismatch=%s",
        totals["scanned"],
        totals["downloaded"],
        totals["archived"],
        totals["saved"],
        totals["existing_url"],
        totals["duplicate_pdf"],
        totals["download_failed"],
        totals["gemini_failed"],
        totals["db_failed"],
        totals["retry_pending"],
        totals["subject_mismatch"],
    )
    logger.info(
        "[Run Summary] Gemini retries: due=%s succeeded=%s failed=%s skipped=%s",
        retry_stats["due"],
        retry_stats["succeeded"],
        retry_stats["failed"],
        retry_stats["skipped"],
    )
    logger.info("=" * 72)


def _date_drift_days(scraper_date, extracted_date) -> Optional[int]:
    """Days between the scraper listing date and the Gemini-extracted date."""
    try:
        scraper_day = datetime.strptime(str(scraper_date)[:10], "%Y-%m-%d").date()
        extracted_day = datetime.strptime(str(extracted_date)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None
    return abs((extracted_day - scraper_day).days)


def _apply_extracted_metadata(report: dict, extracted: dict):
    if not report.get("ticker") and extracted.get("ticker"):
        report["ticker"] = extracted["ticker"]
        report["company"] = extracted.get("company", "")
    extracted_date = extracted.get("report_date")
    if extracted_date:
        scraper_date = report.get("report_date")
        drift = _date_drift_days(scraper_date, extracted_date) if scraper_date else None
        if QUALITY_CHECKS_ENABLED and drift is not None and drift > REPORT_DATE_MAX_DRIFT_DAYS:
            # The revision chain is ordered by report_date; a hallucinated date
            # silently reorders it, so prefer the scraper listing date.
            report["_date_drift"] = f"scraper={scraper_date} extracted={extracted_date}"
            logger.warning(
                "    [!] Keeping scraper report_date %s; extracted date %s drifts %s day(s).",
                scraper_date,
                extracted_date,
                drift,
            )
        else:
            report["report_date"] = extracted_date
    if extracted.get("revision_reason"):
        report["revision_reason"] = extracted["revision_reason"]


def _json_safe_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _build_ingestion_event(report: dict, stage: str, status: str, message: str = "") -> dict:
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": _json_safe_value(report.get("source")),
        "stage": stage,
        "status": status,
        "ticker": _json_safe_value(report.get("ticker")),
        "company": _json_safe_value(report.get("company")),
        "broker": _json_safe_value(report.get("broker")),
        "title": _json_safe_value(report.get("title")),
        "report_url": _json_safe_value(report.get("report_url")),
        "pdf_hash": _json_safe_value(report.get("pdf_hash")),
        "report_date": _json_safe_value(report.get("report_date")),
        "local_pdf_path": _json_safe_value(report.get("local_pdf_path")),
        "message": _json_safe_value(message),
    }


def _append_ingestion_jsonl(event: dict):
    """Best-effort crash breadcrumb. SQLite remains the queryable audit store."""
    try:
        os.makedirs(INGESTION_LOGS_DIR, exist_ok=True)
        log_date = datetime.now().date().isoformat()
        path = os.path.join(INGESTION_LOGS_DIR, f"ingestion-events-{log_date}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        logger.warning("Could not append ingestion JSONL event stage=%s status=%s: %s", event.get("stage"), event.get("status"), exc)


def _record_ingestion_event(conn, event_buffer: Optional[list], report: dict, stage: str, status: str, message: str = ""):
    event = _build_ingestion_event(report, stage, status, message)
    _append_ingestion_jsonl(event)
    if event_buffer is not None:
        event_buffer.append(event)
        return
    try:
        insert_ingestion_event(conn, event)
    except sqlite3.OperationalError as exc:
        if not _is_disk_io_error(exc):
            raise
        logger.warning(
            "[%s] Could not record ingestion event stage=%s status=%s: %s",
            report.get("source") or "?",
            stage,
            status,
            exc,
        )


def _flush_ingestion_events(conn, event_buffer: list, source_name: str):
    if not event_buffer:
        return conn
    for attempt in range(DB_OPERATION_RETRIES):
        try:
            conn = _reopen_conn(conn, source_name, "opening DB connection to flush ingestion events")
            for event in event_buffer:
                insert_ingestion_event(conn, event)
            conn.commit()
            logger.info("[%s] Recorded %s buffered ingestion event(s).", source_name, len(event_buffer))
            event_buffer.clear()
            return conn
        except sqlite3.OperationalError as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if not _is_disk_io_error(exc):
                raise
            if attempt == DB_OPERATION_RETRIES - 1:
                logger.warning(
                    "[%s] Could not flush %s ingestion event(s); continuing without audit rows: %s",
                    source_name,
                    len(event_buffer),
                    exc,
                )
                return conn
            delay = DB_OPERATION_RETRY_DELAY_SECONDS * (attempt + 1)
            logger.warning(
                "[%s] SQLite disk I/O error while flushing ingestion events; retrying in %ss: %s",
                source_name,
                delay,
                exc,
            )
            time.sleep(delay)
    return conn


def _json_dumps_or_none(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _build_gemini_extraction_row(report: dict, status: str, report_id: Optional[int] = None) -> dict:
    metadata = report.get("_gemini_metadata") or {}
    return {
        "report_id": report_id,
        "ticker": report.get("ticker"),
        "company": report.get("company"),
        "broker": report.get("broker"),
        "source": report.get("source"),
        "title": report.get("title"),
        "report_url": report.get("report_url"),
        "report_date": report.get("report_date"),
        "pdf_hash": report.get("pdf_hash"),
        "local_pdf_path": report.get("local_pdf_path"),
        "model": metadata.get("model"),
        "prompt_version": metadata.get("prompt_version"),
        "status": status,
        "error": report.get("_gemini_error") or metadata.get("error"),
        "raw_response": metadata.get("raw_response"),
        "parsed_payload": _json_dumps_or_none(metadata.get("parsed_payload")),
        "normalized_payload": _json_dumps_or_none(metadata.get("normalized_payload")),
    }


def _record_gemini_extraction(conn, report: dict, status: str, report_id: Optional[int] = None):
    try:
        insert_gemini_extraction(conn, _build_gemini_extraction_row(report, status, report_id))
    except sqlite3.OperationalError as exc:
        if not _is_disk_io_error(exc):
            raise
        logger.warning(
            "[%s] Could not record Gemini extraction status=%s for %s: %s",
            report.get("source") or "?",
            status,
            report.get("pdf_hash") or report.get("report_url") or "?",
            exc,
        )


def _subjects_match_after_extraction(report: dict, extracted: dict) -> bool:
    if report.get("source") != "bondweb":
        return True
    intended_ticker = str(report.get("ticker") or "").strip().upper()
    extracted_ticker = str(extracted.get("ticker") or "").strip().upper()
    if intended_ticker and extracted_ticker and intended_ticker != extracted_ticker:
        return False
    return True


def _relative_change(new_value, prev_value) -> Optional[float]:
    if new_value is None or prev_value in (None, 0):
        return None
    return abs(float(new_value) - float(prev_value)) / abs(float(prev_value))


def _values_agree(first, second, tolerance: float) -> bool:
    """True when two extraction passes produced the same value within tolerance."""
    if first is None or second is None:
        return False
    first, second = float(first), float(second)
    denom = max(abs(first), abs(second))
    if denom == 0:
        return True
    return abs(first - second) / denom <= tolerance


def _eps_matches_net_profit(fwd_eps, net_profit, shares, mismatch_factor: float) -> Optional[bool]:
    """
    Cross-check extracted EPS against net_profit / share count.
    Report tables are printed in 억원 or 십억원 without saying which, so accept
    either unit; only order-of-magnitude errors should fail. None = not checkable.
    """
    if not fwd_eps or not net_profit or not shares:
        return None
    for unit in (1e8, 1e9):  # 억원, 십억원
        implied_eps = float(net_profit) * unit / float(shares)
        if implied_eps == 0 or implied_eps * float(fwd_eps) < 0:
            continue
        ratio = max(abs(implied_eps), abs(float(fwd_eps))) / min(abs(implied_eps), abs(float(fwd_eps)))
        if ratio <= mismatch_factor:
            return True
    return False


def _load_prior_quality_baseline(report: dict, extracted: dict):
    """Fetch same-broker prior TP/EPS using a short-lived connection."""
    prior_tp = None
    prior_eps = {}
    if not (report.get("ticker") and report.get("broker") and report.get("report_date")):
        return prior_tp, prior_eps
    qconn = _reopen_conn(
        None,
        report.get("source") or "Quality",
        "opening DB connection for quality baseline",
    )
    try:
        tp_row = get_previous_target_price_record(
            qconn, report["ticker"], report["broker"], report["report_date"],
        )
        prior_tp = float(tp_row["target_price"]) if tp_row else None
        for est in extracted.get("estimates") or []:
            fiscal_year = est.get("fiscal_year")
            if fiscal_year is None or est.get("fwd_eps") is None:
                continue
            try:
                fiscal_year = int(fiscal_year)
            except (TypeError, ValueError):
                continue
            eps_row = get_previous_eps_record(
                qconn, report["ticker"], fiscal_year, report["broker"], report["report_date"],
            )
            if eps_row:
                prior_eps[fiscal_year] = float(eps_row["fwd_eps"])
    finally:
        qconn.close()
    return prior_tp, prior_eps


def _run_quality_checks(report: dict, extracted: dict, pdf_bytes: bytes):
    """
    Flag implausible TP/EPS values as suspect before they enter the revision
    chain. Suspect rows are still saved but excluded from alerts, revision
    baselines, and dashboard consensus.
    """
    ticker = report.get("ticker")
    estimates = extracted.get("estimates") or []
    new_tp = extracted.get("target_price")
    quote = None

    # 1. Target price plausibility vs current market price.
    if new_tp and ticker:
        quote = get_quote(ticker)
        price = quote.get("price")
        if price:
            ratio = float(new_tp) / price
            if not (TP_PRICE_MIN_RATIO <= ratio <= TP_PRICE_MAX_RATIO):
                extracted["tp_suspect"] = 1
                extracted["tp_suspect_reason"] = f"tp_price_ratio={ratio:.2f}"
                logger.warning(
                    "    [!] Suspect target price %s: %.2fx the current price %s.",
                    new_tp, ratio, price,
                )

    # 2. EPS vs net-profit/share-count cross-check.
    if ticker and any(est.get("fwd_eps") and est.get("net_profit") for est in estimates):
        if quote is None:
            quote = get_quote(ticker)
        shares = quote.get("shares")
        if shares:
            for est in estimates:
                verdict = _eps_matches_net_profit(
                    est.get("fwd_eps"), est.get("net_profit"), shares, EPS_NP_MISMATCH_FACTOR,
                )
                if verdict is False:
                    est["suspect"] = 1
                    est["suspect_reason"] = "eps_np_mismatch"
                    logger.warning(
                        "    [!] Suspect EPS %s for FY%s: inconsistent with net profit %s and ~%.0f shares.",
                        est.get("fwd_eps"), est.get("fiscal_year"), est.get("net_profit"), shares,
                    )

    # 3. Outlier gate vs same-broker prior values, with a confirmation pass.
    prior_tp, prior_eps = _load_prior_quality_baseline(report, extracted)
    tp_change = None if extracted.get("tp_suspect") else _relative_change(new_tp, prior_tp)
    tp_outlier = tp_change is not None and tp_change > OUTLIER_CONFIRM_THRESHOLD
    outlier_years = set()
    for est in estimates:
        if est.get("suspect"):
            continue
        change = _relative_change(est.get("fwd_eps"), prior_eps.get(est.get("fiscal_year")))
        if change is not None and change > OUTLIER_CONFIRM_THRESHOLD:
            outlier_years.add(est.get("fiscal_year"))
    if not tp_outlier and not outlier_years:
        return

    logger.info(
        "    Large change vs prior broker values (tp_outlier=%s, eps_years=%s); running confirmation extraction.",
        tp_outlier, sorted(outlier_years),
    )
    confirm, _confirm_error, _confirm_meta = extract_eps_from_pdf(
        pdf_bytes,
        return_error=True,
        return_metadata=True,
        model=GEMINI_ESCALATION_MODEL,
    )
    confirm_tp = confirm.get("target_price") if confirm else None
    confirm_eps = {}
    if confirm:
        for est in confirm.get("estimates") or []:
            try:
                confirm_eps[int(est.get("fiscal_year"))] = est.get("fwd_eps")
            except (TypeError, ValueError):
                continue
    unconfirmed_reason = "outlier_unconfirmed" if confirm else "outlier_confirm_failed"

    if tp_outlier:
        if confirm and _values_agree(new_tp, confirm_tp, OUTLIER_AGREE_TOLERANCE):
            logger.info("    Outlier target price confirmed by second extraction; keeping.")
        else:
            extracted["tp_suspect"] = 1
            extracted["tp_suspect_reason"] = unconfirmed_reason
            logger.warning("    [!] Outlier target price NOT confirmed (%s); flagged suspect.", unconfirmed_reason)
    for est in estimates:
        if est.get("fiscal_year") not in outlier_years:
            continue
        if confirm and _values_agree(est.get("fwd_eps"), confirm_eps.get(est.get("fiscal_year")), OUTLIER_AGREE_TOLERANCE):
            logger.info("    Outlier EPS for FY%s confirmed by second extraction; keeping.", est.get("fiscal_year"))
            continue
        est["suspect"] = 1
        est["suspect_reason"] = unconfirmed_reason
        logger.warning(
            "    [!] Outlier EPS for FY%s NOT confirmed (%s); flagged suspect.",
            est.get("fiscal_year"), unconfirmed_reason,
        )


def refresh_primary_universe():
    logger.info("[KRX] Fetching KOSPI 200 constituents...")
    kospi200 = fetch_kospi200()
    kosdaq150 = []
    if ENABLE_KOSDAQ150:
        logger.info("[KRX] Fetching KOSDAQ 150 constituents...")
        kosdaq150 = fetch_kosdaq150()

    if kospi200:
        upsert_kospi200(kospi200)
        logger.info("[KRX] Updated %s KOSPI 200 constituents.", len(kospi200))
    else:
        logger.warning("[KRX] Warning: no constituents returned — check KRX scraper.")
    if ENABLE_KOSDAQ150:
        if kosdaq150:
            upsert_kosdaq150(kosdaq150)
            logger.info("[KRX] Updated %s KOSDAQ 150 constituents.", len(kosdaq150))
        else:
            logger.warning("[KRX] Warning: no KOSDAQ 150 constituents returned — check KRX scraper.")
    return kospi200, kosdaq150


def _extract_report_payload(
    report: dict,
    pdf_bytes: bytes,
    kospi200_tickers: set,
    extraction_model: Optional[str] = None,
) -> Tuple[str, Optional[dict]]:
    """Extract EPS from a PDF. Only opens short-lived SQLite connections."""
    canonicalize_report_broker(report)
    extracted, gemini_error, gemini_metadata = extract_eps_from_pdf(
        pdf_bytes,
        return_error=True,
        return_metadata=True,
        model=extraction_model,
    )
    report["_gemini_metadata"] = gemini_metadata
    if not extracted:
        report["_gemini_error"] = gemini_error
        return "gemini_failed", None

    _apply_extracted_metadata(report, extracted)

    if not _subjects_match_after_extraction(report, extracted):
        report["_gemini_error"] = "Gemini extracted a different subject company/ticker"
        report["_subject_mismatch"] = (
            f"intended_ticker={report.get('ticker')} extracted_ticker={extracted.get('ticker')} "
            f"extracted_company={extracted.get('company')}"
        )
        return "subject_mismatch", None

    # Skip if still no ticker, or not in KOSPI 200
    if not report.get("ticker") or report["ticker"] not in kospi200_tickers:
        report["_gemini_error"] = "Gemini extracted a non-KOSPI200 or missing ticker"
        return "non_kospi", None

    if QUALITY_CHECKS_ENABLED:
        # Normalize estimates now (short-lived connection) so quality checks
        # see post-shift, post-cutoff fiscal years; the save step skips
        # re-normalizing via the flag below. Everything here is fail-open:
        # quality checks must never block ingestion, and disk I/O errors are
        # retried the same way as every other DB touchpoint.
        try:
            qconn = _reopen_conn(
                None,
                report.get("source") or "Quality",
                "opening DB connection for pre-save estimate normalization",
            )
            try:
                extracted["estimates"] = _normalize_estimates(qconn, report, extracted)
                extracted["_estimates_normalized"] = True
            finally:
                qconn.close()
            _run_quality_checks(report, extracted, pdf_bytes)
        except Exception as exc:
            # Without the normalized flag, the save step re-runs
            # _normalize_estimates on its own retry-wrapped connection.
            logger.warning("    [!] Quality checks skipped; continuing without flags: %s", exc)

    return "ready", extracted


def _collect_pending_alerts(conn, report: dict, extracted: dict) -> list:
    """Collect alert data BEFORE the DB write, but don't send yet."""
    pending = []

    # Out-of-order ingestion guard: when a newer report from this broker is
    # already saved (backfill/retry), an alert would compare stale values.
    if (
        report.get("ticker") and report.get("broker") and report.get("report_date")
        and has_newer_report(conn, report["ticker"], report["broker"], report["report_date"])
    ):
        logger.info(
            "    Skipping alerts: a newer %s report for %s already exists.",
            report["broker"], report["ticker"],
        )
        return pending

    # Target price alert (skipped when the TP failed quality checks)
    new_tp = extracted.get("target_price")
    if extracted.get("tp_suspect"):
        new_tp = None
    if new_tp and report.get("ticker") and report.get("broker") and report.get("report_date"):
        prev_tp_row = get_previous_target_price_record(
            conn, report["ticker"], report["broker"], report["report_date"],
        )
        prev_tp = float(prev_tp_row["target_price"]) if prev_tp_row else None
        if prev_tp and abs(new_tp - prev_tp) / abs(prev_tp) > TP_CHANGE_THRESHOLD:
            pending.append(("tp", {
                "ticker": report["ticker"], "company": report["company"],
                "broker": report["broker"], "prev_tp": prev_tp, "new_tp": new_tp,
                "prev_report_date": prev_tp_row["report_date"] if prev_tp_row else None,
                "new_report_date": report["report_date"],
                "recommendation": extracted.get("recommendation"),
                "report_url": report["report_url"],
                "revision_reason": extracted.get("revision_reason"),
            }))

    # EPS alerts (per estimate; suspect estimates never alert)
    for est in extracted.get("estimates", []):
        if est.get("suspect"):
            continue
        fiscal_year = est.get("fiscal_year")
        new_eps = est.get("fwd_eps")
        if not fiscal_year or new_eps is None:
            continue
        if not (report.get("ticker") and report.get("broker") and report.get("report_date")):
            continue
        prev_eps_row = get_previous_eps_record(
            conn, report["ticker"], fiscal_year, report["broker"], report["report_date"],
        )
        prev_eps = float(prev_eps_row["fwd_eps"]) if prev_eps_row else None
        if prev_eps and abs(new_eps - prev_eps) / abs(prev_eps) > EPS_CHANGE_THRESHOLD:
            pending.append(("eps", {
                "ticker": report["ticker"], "company": report["company"],
                "broker": report["broker"], "fiscal_year": fiscal_year,
                "prev_eps": prev_eps, "new_eps": new_eps,
                "prev_report_date": prev_eps_row["report_date"] if prev_eps_row else None,
                "new_report_date": report["report_date"],
                "target_price": new_tp,
                "recommendation": extracted.get("recommendation"),
                "report_url": report["report_url"],
                "revision_reason": extracted.get("revision_reason"),
            }))

    return pending


def _send_pending_alerts(pending_alerts: list):
    """Send alerts that were collected before DB commit — only call after successful commit."""
    for alert_type, data in pending_alerts:
        if alert_type == "tp":
            direction = "RAISED" if data["new_tp"] > data["prev_tp"] else "CUT"
            logger.info("[TP %s] %s: %s -> %s", direction, data["ticker"], f"{data['prev_tp']:,.0f}", f"{data['new_tp']:,.0f}")
            send_target_price_change_alert(**data)
        elif alert_type == "eps":
            direction = "UPGRADE" if data["new_eps"] > data["prev_eps"] else "DOWNGRADE"
            logger.info("[%s] %s %sE: %s -> %s", direction, data["ticker"], data["fiscal_year"], f"{data['prev_eps']:,.0f}", f"{data['new_eps']:,.0f}")
            send_eps_change_alert(**data)


def _detect_tp_event(conn, report: dict, extracted: dict) -> Optional[dict]:
    """Detect target-price coverage initiation/withdrawal (recorded, not alerted)."""
    if not (report.get("ticker") and report.get("broker") and report.get("report_date")):
        return None
    if extracted.get("tp_suspect"):
        return None
    new_tp = extracted.get("target_price")
    prev_tp_row = get_previous_target_price_record(
        conn, report["ticker"], report["broker"], report["report_date"],
    )
    base_event = {
        "ticker": report["ticker"],
        "broker": report["broker"],
        "report_date": report["report_date"],
    }
    if new_tp and prev_tp_row is None:
        return {**base_event, "event_type": "initiation", "prev_tp": None, "new_tp": new_tp}
    if not new_tp and prev_tp_row is not None:
        # Only a withdrawal when the immediately prior report still carried a TP;
        # otherwise every TP-less report would re-fire the event.
        latest_prior = get_latest_prior_report_tp(
            conn, report["ticker"], report["broker"], report["report_date"],
        )
        if latest_prior and latest_prior["target_price"]:
            return {
                **base_event,
                "event_type": "withdrawal",
                "prev_tp": float(latest_prior["target_price"]),
                "new_tp": None,
            }
    return None


def _save_extracted_report(conn, report: dict, extracted: dict) -> str:
    """Save an already-extracted report to SQLite. Returns a compact status string."""
    if not extracted.pop("_estimates_normalized", False):
        extracted["estimates"] = _normalize_estimates(conn, report, extracted)

    # Report-level TP/recommendation (the TP is a report-level fact; the copy on
    # each estimate row is kept for backward compatibility).
    extracted["recommendation_norm"] = normalize_recommendation(extracted.get("recommendation"))
    report["target_price"] = extracted.get("target_price")
    report["tp_suspect"] = 1 if extracted.get("tp_suspect") else 0
    report["tp_suspect_reason"] = extracted.get("tp_suspect_reason")
    report["recommendation"] = extracted.get("recommendation")
    report["recommendation_norm"] = extracted["recommendation_norm"]

    # Collect alerts + TP events BEFORE writing, but don't send until commit succeeds
    pending_alerts = _collect_pending_alerts(conn, report, extracted)
    tp_event = _detect_tp_event(conn, report, extracted)

    report_id = insert_report(conn, report)
    if not report_id:
        existing_report = get_report_by_url(conn, report.get("report_url"))
        existing_report_id = existing_report["id"] if existing_report else None
        existing_estimates = count_eps_estimates_for_report(conn, existing_report_id) if existing_report_id else 0
        conn.rollback()
        _record_gemini_extraction(conn, report, "insert_duplicate", existing_report_id)
        if existing_report_id and existing_estimates > 0:
            logger.info(
                "    Duplicate report URL already has %s estimate row(s); treating retry as already saved.",
                existing_estimates,
            )
            return "existing_after_retry"
        logger.warning(
            "    [!] Report insert was ignored due to duplicate key, but no estimates were found; skipping estimates."
        )
        return "insert_duplicate"

    # Insert estimates (without sending alerts inline)
    _record_gemini_extraction(conn, report, "success", report_id)
    new_tp = extracted.get("target_price")
    for est in extracted.get("estimates", []):
        fiscal_year = est.get("fiscal_year")
        new_eps = est.get("fwd_eps")
        if not fiscal_year or new_eps is None:
            continue
        insert_eps(conn, {
            "report_id": report_id,
            "ticker": report["ticker"],
            "broker": report["broker"],
            "fiscal_year": fiscal_year,
            "fwd_eps": new_eps,
            "target_price": new_tp,
            "recommendation": extracted.get("recommendation"),
            "recommendation_norm": extracted.get("recommendation_norm"),
            "revenue": est.get("revenue"),
            "operating_profit": est.get("operating_profit"),
            "net_profit": est.get("net_profit"),
            "suspect": 1 if est.get("suspect") else 0,
            "suspect_reason": est.get("suspect_reason"),
        })

    if tp_event:
        tp_event["report_id"] = report_id
        insert_tp_event(conn, tp_event)
        logger.info(
            "    [TP %s] %s / %s", tp_event["event_type"].upper(),
            tp_event["ticker"], tp_event["broker"],
        )

    conn.commit()

    # Only send alerts AFTER successful commit
    _send_pending_alerts(pending_alerts)

    return "saved"


def _extract_and_save_report(
    conn,
    report: dict,
    pdf_bytes: bytes,
    kospi200_tickers: set,
    extraction_model: Optional[str] = None,
) -> str:
    """Extract EPS from a PDF and save to DB. Returns a compact status string."""

    archived_path = report.get("local_pdf_path")
    if not archived_path:
        archived_path = _archive_report_pdf(report, pdf_bytes)
        report["local_pdf_path"] = archived_path
        logger.info("    Saved PDF: %s", archived_path)

    status, extracted = _extract_report_payload(
        report, pdf_bytes, kospi200_tickers, extraction_model=extraction_model,
    )
    if status != "ready":
        return status
    return _save_extracted_report(conn, report, extracted)


def _queue_gemini_retry(conn, report: dict, last_error: str):
    upsert_gemini_retry(conn, report, GEMINI_RETRY_DELAY_MINUTES, last_error)
    conn.commit()
    logger.warning(
        "    [!] Gemini extraction failed; retry scheduled in %s minutes.",
        GEMINI_RETRY_DELAY_MINUTES,
    )


def _should_retry_gemini_failure(error) -> bool:
    message = (error or "").lower()
    if "document has no pages" in message or "no pages" in message:
        return False
    if "multi-company" in message or "ambiguous extraction" in message:
        return False
    if "invalid_argument" in message or "invalid argument" in message:
        return False
    return True


def process_report(conn, report: dict, pdf_bytes: bytes, kospi200_tickers: set) -> str:
    """Extract EPS from a PDF, save to DB, and alert on changes."""
    status = _extract_and_save_report(conn, report, pdf_bytes, kospi200_tickers)
    if status == "gemini_failed":
        error = report.get("_gemini_error") or "Gemini extraction returned no usable data"
        if _should_retry_gemini_failure(error):
            _queue_gemini_retry(conn, report, error)
        else:
            logger.warning("    [!] Gemini extraction failed permanently, not retrying: %s", error)
    return status


def process_due_gemini_retries(conn, kospi200_tickers: set, trigger: str = "scheduled"):
    if not GEMINI_RETRY_LOCK.acquire(blocking=False):
        logger.info("[Gemini Retry] Skipping %s retry run because another retry batch is already in progress.", trigger)
        return {"due": 0, "succeeded": 0, "failed": 0, "skipped": 0}

    retries = get_due_gemini_retries(conn, GEMINI_RETRY_LIMIT)
    stats = {"due": len(retries), "succeeded": 0, "failed": 0, "skipped": 0}
    try:
        if not retries:
            return stats

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
            canonicalize_report_broker(report)
            current_attempts = int(retry["attempts"] or 0)
            next_attempt = current_attempts + 1
            label = f"{report.get('company')} ({report.get('ticker')}) — {report.get('broker')}"

            if current_attempts >= GEMINI_RETRY_MAX_ATTEMPTS:
                skipped += 1
                delete_gemini_retry(conn, retry["id"])
                conn.commit()
                logger.warning(
                    "[Gemini Retry] Removing retry after reaching max attempts (%s): %s",
                    GEMINI_RETRY_MAX_ATTEMPTS,
                    label,
                )
                continue

            logger.info("[Gemini Retry] Retrying %s | attempt=%s", label, next_attempt)

            local_pdf_path = retry["local_pdf_path"]
            if not local_pdf_path or not os.path.exists(local_pdf_path):
                if next_attempt >= GEMINI_RETRY_MAX_ATTEMPTS:
                    skipped += 1
                    delete_gemini_retry(conn, retry["id"])
                    conn.commit()
                    logger.warning(
                        "[Gemini Retry] Archived PDF missing and max attempts reached; removing retry: %s",
                        label,
                    )
                else:
                    failed += 1
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

            # First retry re-runs the default model; later attempts escalate to
            # a stronger model — repeating the identical call mostly reproduces
            # the identical failure.
            extraction_model = GEMINI_ESCALATION_MODEL if current_attempts >= 1 else None
            if extraction_model:
                logger.info("[Gemini Retry] Escalating to model %s for %s", extraction_model, label)
            status = _extract_and_save_report(
                conn, report, pdf_bytes, kospi200_tickers, extraction_model=extraction_model,
            )
            if status == "saved":
                succeeded += 1
                delete_gemini_retry(conn, retry["id"])
                conn.commit()
                logger.info("[Gemini Retry] Saved retry result: %s", label)
            elif status == "gemini_failed":
                _record_gemini_extraction(conn, report, "gemini_failed")
                error = report.get("_gemini_error") or "Gemini extraction returned no usable data"
                if not _should_retry_gemini_failure(error):
                    skipped += 1
                    delete_gemini_retry(conn, retry["id"])
                    conn.commit()
                    logger.warning("[Gemini Retry] Permanent extraction failure, removing retry: %s | %s", label, error)
                elif next_attempt >= GEMINI_RETRY_MAX_ATTEMPTS:
                    failed += 1
                    delete_gemini_retry(conn, retry["id"])
                    conn.commit()
                    logger.warning(
                        "[Gemini Retry] Max retry attempts reached after Gemini failure, removing retry: %s | %s",
                        label,
                        error,
                    )
                else:
                    failed += 1
                    mark_gemini_retry_failed(
                        conn,
                        retry["id"],
                        GEMINI_RETRY_DELAY_MINUTES,
                        error,
                    )
                    conn.commit()
                    logger.warning(
                        "[Gemini Retry] Gemini still failed; retry rescheduled in %s minutes: %s",
                        GEMINI_RETRY_DELAY_MINUTES,
                        label,
                    )
            else:
                _record_gemini_extraction(conn, report, status)
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
        stats.update({"succeeded": succeeded, "failed": failed, "skipped": skipped})
        return stats
    finally:
        GEMINI_RETRY_LOCK.release()


def run_source(conn, source_name: str, reports: list, download_fn, kospi200_tickers: set):
    total_reports = len(reports)
    stats = _empty_source_stats(source_name)
    stats["scanned"] = total_reports
    logger.info("[%s] Found %s reports.", source_name, total_reports)
    if source_name == "Bondweb":
        bondweb_reset_download_failure_samples()

    processed = 0
    skipped_existing = 0
    skipped_download = 0
    skipped_duplicate_pdf = 0
    consecutive_db_failures = 0
    started_at = time.time()
    skip_sets, conn = _load_skip_sets(conn, source_name)
    existing_report_urls = skip_sets["report_urls"]
    existing_pdf_hashes = skip_sets["pdf_hashes"]
    pending_retry_hashes = skip_sets["retry_hashes"]
    ingestion_events = []

    for idx, report in enumerate(reports, start=1):
        canonicalize_report_broker(report)
        elapsed = time.time() - started_at
        avg_seconds = elapsed / max(1, idx - 1) if idx > 1 else 0
        remaining = total_reports - idx + 1
        eta = _format_eta(avg_seconds * remaining) if avg_seconds else "estimating..."

        if report["report_url"] in existing_report_urls:
            skipped_existing += 1
            stats["existing_url"] += 1
            _record_ingestion_event(conn, ingestion_events, report, "skip", "existing_url")
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

        label = _report_label(report)
        _record_ingestion_event(conn, ingestion_events, report, "candidate", "found")
        _log_progress(
            source_name,
            idx,
            total_reports,
            processed,
            skipped_existing,
            skipped_download,
            skipped_duplicate_pdf,
            eta,
            f"Found: {label}",
        )
        logger.info("[%s] [%s/%s] Downloading PDF...", source_name, idx, total_reports)

        try:
            try:
                pdf_bytes = download_fn(report["report_url"], report)
            except TypeError:
                pdf_bytes = download_fn(report["report_url"])
        except Exception as exc:
            skipped_download += 1
            stats["download_failed"] += 1
            report["_download_error"] = str(exc)
            _record_ingestion_event(conn, ingestion_events, report, "download", "download_failed")
            logger.warning(
                "[%s] [%s/%s] Download errored, skipping: %s | %s",
                source_name,
                idx,
                total_reports,
                label,
                exc,
            )
            continue
        if not pdf_bytes:
            skipped_download += 1
            stats["download_failed"] += 1
            _record_ingestion_event(conn, ingestion_events, report, "download", "download_failed")
            logger.warning("[%s] [%s/%s] Download failed, skipping: %s", source_name, idx, total_reports, label)
            continue
        stats["downloaded"] += 1

        pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()
        if pdf_hash in existing_pdf_hashes:
            skipped_duplicate_pdf += 1
            stats["duplicate_pdf"] += 1
            report["pdf_hash"] = pdf_hash
            _record_ingestion_event(conn, ingestion_events, report, "skip", "duplicate_pdf")
            logger.warning("[%s] [%s/%s] Duplicate PDF content already saved, skipping: %s", source_name, idx, total_reports, label)
            continue
        if pdf_hash in pending_retry_hashes:
            stats["retry_pending"] += 1
            report["pdf_hash"] = pdf_hash
            _record_ingestion_event(conn, ingestion_events, report, "skip", "retry_pending")
            logger.warning("[%s] [%s/%s] Gemini retry already scheduled for this PDF, skipping until retry time: %s", source_name, idx, total_reports, label)
            continue
        report["pdf_hash"] = pdf_hash

        archived_path = _archive_report_pdf(report, pdf_bytes)
        report["local_pdf_path"] = archived_path
        stats["archived"] += 1
        _record_ingestion_event(conn, ingestion_events, report, "archive", "archived")
        logger.info("[%s] [%s/%s] Archived PDF: %s", source_name, idx, total_reports, archived_path)
        _record_ingestion_event(conn, ingestion_events, report, "gemini", "started")
        logger.info("[%s] [%s/%s] Sending to Gemini: %s", source_name, idx, total_reports, label)

        status, extracted = _extract_report_payload(report, pdf_bytes, kospi200_tickers)
        if status == "ready":
            _record_ingestion_event(conn, ingestion_events, report, "gemini", "succeeded")
        if status == "ready":
            # Do not hold a SQLite connection open across network-bound Gemini work.
            # Reopen immediately before the DB phase so stale WAL/SHM state cannot
            # poison the save path.
            status, conn, db_failed = _save_extracted_report_with_retry(
                conn,
                report,
                extracted,
                source_name,
                idx,
                total_reports,
                label,
            )
            if db_failed:
                stats["db_failed"] += 1
                _record_gemini_extraction(conn, report, "db_failed")
                _record_ingestion_event(conn, ingestion_events, report, "db", "db_failed")
                consecutive_db_failures += 1
                if consecutive_db_failures >= DB_FAILURE_ABORT_THRESHOLD:
                    logger.error(
                        "[%s] Stopping source after %s consecutive DB save failure(s) to avoid wasting Gemini calls.",
                        source_name,
                        consecutive_db_failures,
                    )
                    break
                time.sleep(PROCESS_DELAY_SECONDS)
                continue
            consecutive_db_failures = 0
        elif status == "gemini_failed":
            consecutive_db_failures = 0
            error = report.get("_gemini_error") or "Gemini extraction returned no usable data"
            if _should_retry_gemini_failure(error):
                try:
                    conn = _reopen_conn(conn, source_name, "opening DB connection to queue Gemini retry")
                    _queue_gemini_retry(conn, report, error)
                    pending_retry_hashes.add(pdf_hash)
                    _record_ingestion_event(conn, ingestion_events, report, "gemini", "retry_queued", error)
                    logger.warning(
                        "[%s] [%s/%s] Gemini failed retryable; retry queued: %s | %s",
                        source_name,
                        idx,
                        total_reports,
                        error,
                        label,
                    )
                except sqlite3.OperationalError as exc:
                    if not _is_disk_io_error(exc):
                        raise
                    stats["db_failed"] += 1
                    consecutive_db_failures += 1
                    _record_ingestion_event(conn, ingestion_events, report, "gemini", "retry_queue_failed", str(exc))
                    logger.exception(
                        "[%s] [%s/%s] Could not queue Gemini retry because SQLite could not open; archived PDF kept and source will continue unless DB failures repeat: %s",
                        source_name,
                        idx,
                        total_reports,
                        label,
                    )
                    if consecutive_db_failures >= DB_FAILURE_ABORT_THRESHOLD:
                        logger.error(
                            "[%s] Stopping source after %s consecutive DB failure(s).",
                            source_name,
                            consecutive_db_failures,
                        )
                        break
                    time.sleep(PROCESS_DELAY_SECONDS)
                    continue
        else:
            consecutive_db_failures = 0
        if status == "saved":
            processed += 1
            stats["saved"] += 1
            existing_report_urls.add(report["report_url"])
            existing_pdf_hashes.add(pdf_hash)
            _record_ingestion_event(conn, ingestion_events, report, "db", "saved")
            logger.info("[%s] [%s/%s] Saved to DB: %s", source_name, idx, total_reports, label)
            if processed % WAL_CHECKPOINT_INTERVAL == 0:
                wal_checkpoint(conn)
        elif status == "gemini_failed":
            stats["gemini_failed"] += 1
            _record_gemini_extraction(conn, report, "gemini_failed")
            _record_ingestion_event(conn, ingestion_events, report, "gemini", "gemini_failed", report.get("_gemini_error") or "")
            logger.warning("[%s] [%s/%s] Not saved to DB after Gemini step: status=%s | %s", source_name, idx, total_reports, status, label)
        elif status == "subject_mismatch":
            stats["subject_mismatch"] += 1
            _record_gemini_extraction(conn, report, "subject_mismatch")
            _record_ingestion_event(conn, ingestion_events, report, "gemini", "subject_mismatch", report.get("_subject_mismatch") or "")
            logger.warning("[%s] [%s/%s] Not saved to DB after Gemini step: status=%s | %s", source_name, idx, total_reports, status, label)
        elif status == "non_kospi":
            stats["non_kospi"] += 1
            _record_gemini_extraction(conn, report, "non_kospi")
            _record_ingestion_event(conn, ingestion_events, report, "gemini", "non_kospi")
            logger.warning("[%s] [%s/%s] Not saved to DB after Gemini step: status=%s | %s", source_name, idx, total_reports, status, label)
        elif status == "insert_duplicate":
            stats["insert_duplicate"] += 1
            _record_ingestion_event(conn, ingestion_events, report, "db", "insert_duplicate")
            logger.warning("[%s] [%s/%s] Not saved to DB after Gemini step: status=%s | %s", source_name, idx, total_reports, status, label)
        elif status == "existing_after_retry":
            stats["existing_url"] += 1
            existing_report_urls.add(report["report_url"])
            existing_pdf_hashes.add(pdf_hash)
            _record_ingestion_event(conn, ingestion_events, report, "db", "existing_after_retry")
            logger.info("[%s] [%s/%s] Already present after retry; treating as saved: %s", source_name, idx, total_reports, label)
        else:
            stats["other_not_saved"] += 1
            _record_ingestion_event(conn, ingestion_events, report, "unknown", str(status or "unknown"))
            logger.warning("[%s] [%s/%s] Not saved to DB after Gemini step: status=%s | %s", source_name, idx, total_reports, status, label)
        time.sleep(PROCESS_DELAY_SECONDS)

    total_elapsed = time.time() - started_at
    stats["elapsed_seconds"] = total_elapsed
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
    conn = _flush_ingestion_events(conn, ingestion_events, source_name)
    return stats, conn


def run_once():
    conn = get_conn()
    run_started_at = time.time()
    source_stats = []
    retry_stats = {"due": 0, "succeeded": 0, "failed": 0, "skipped": 0}
    try:
        refreshed_kospi200, refreshed_kosdaq150 = refresh_primary_universe()
        refreshed_universe = list(refreshed_kospi200)
        if ENABLE_KOSDAQ150:
            seen_tickers = {row["ticker"] for row in refreshed_universe}
            for row in refreshed_kosdaq150:
                if row["ticker"] not in seen_tickers:
                    refreshed_universe.append(row)
                    seen_tickers.add(row["ticker"])
        cached_constituents = (
            refreshed_universe if refreshed_universe else get_active_universe(conn, include_kosdaq150=ENABLE_KOSDAQ150)
        )
        active_tickers = {c["ticker"] for c in cached_constituents}
        active_name_map = {c["company"]: c["ticker"] for c in cached_constituents}

        retry_stats = process_due_gemini_retries(conn, active_tickers, trigger="run_once")
        conn.close()
        conn = None

        # Naver Finance — pre-filtered to the active ticker universe
        naver_reports = naver_fetch(pages=NAVER_SCRAPE_PAGES, ticker_whitelist=active_tickers)
        conn = get_conn()
        stats, conn = run_source(conn, "Naver", naver_reports, naver_download, active_tickers)
        source_stats.append(stats)
        conn.close()
        conn = None

        # Bondweb — pre-filtered by company name match; remaining get Gemini extraction
        bondweb_reports = bondweb_fetch(pages=BONDWEB_SCRAPE_PAGES, ticker_whitelist=active_name_map)
        conn = get_conn()
        stats, conn = run_source(conn, "Bondweb", bondweb_reports, bondweb_download, active_tickers)
        source_stats.append(stats)

    finally:
        if source_stats:
            _log_run_summary(source_stats, retry_stats, time.time() - run_started_at)
        if conn is not None:
            conn.close()


def run_gemini_retry_once():
    conn = get_conn()
    try:
        active_tickers = {row["ticker"] for row in get_active_universe(conn, include_kosdaq150=ENABLE_KOSDAQ150)}
        process_due_gemini_retries(conn, active_tickers, trigger="scheduled")
    finally:
        conn.close()


def run_scheduled(interval_minutes: int = 1440):
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    scheduler.add_job(run_once, "interval", minutes=interval_minutes, max_instances=1, coalesce=True)
    scheduler.add_job(
        run_gemini_retry_once,
        "interval",
        minutes=GEMINI_RETRY_DELAY_MINUTES,
        max_instances=1,
        coalesce=True,
    )
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
