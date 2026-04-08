import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import (
    get_conn,
    get_kospi200,
    insert_eps,
    insert_gemini_extraction,
    insert_report,
    upsert_gemini_retry,
)

try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass


REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", REPO_ROOT / "reports"))
GEMINI_RETRY_DELAY_MINUTES = int(os.environ.get("GEMINI_RETRY_DELAY_MINUTES", "30"))


def _should_retry_gemini_failure(error: str) -> bool:
    message = (error or "").lower()
    if "document has no pages" in message or "no pages" in message:
        return False
    if "multi-company" in message or "ambiguous extraction" in message:
        return False
    return True


def _safe_filename_part(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^\w\-.]+", "_", (value or "").strip(), flags=re.UNICODE)
    cleaned = cleaned.strip("._")
    return cleaned or fallback


def _archive_files(
    source: Optional[str],
    ticker: Optional[str],
    company: Optional[str],
    limit: Optional[int],
) -> list[Path]:
    if not REPORTS_DIR.exists():
        return []

    files = []
    for path in sorted(REPORTS_DIR.glob("*/*/*/*.pdf")):
        # reports/company/source/date/file.pdf
        if len(path.relative_to(REPORTS_DIR).parts) != 4:
            continue
        if source and path.parent.parent.name != source:
            continue
        if company and path.parent.parent.parent.name != company:
            continue
        if ticker and not path.name.startswith(f"{ticker}_"):
            continue
        files.append(path)
        if limit and len(files) >= limit:
            break
    return files


def _parse_report_from_path(path: Path) -> Optional[dict]:
    try:
        relative = path.relative_to(REPORTS_DIR)
        company, source, report_date, filename = relative.parts
    except ValueError:
        return None

    stem = Path(filename).stem
    parts = stem.split("_")
    if not parts or not re.fullmatch(r"[0-9A-Z]{6}", parts[0], flags=re.IGNORECASE):
        return None

    ticker = parts[0].upper()
    safe_company = _safe_filename_part(company, "unknown")
    prefix = f"{ticker}_{safe_company}_"
    if stem.startswith(prefix):
        rest = stem[len(prefix):]
    else:
        rest = "_".join(parts[1:])

    if "_" in rest:
        broker, title = rest.split("_", 1)
    else:
        broker, title = rest, "report"

    return {
        "ticker": ticker,
        "company": company,
        "broker": broker,
        "source": source,
        "title": title.replace("_", " "),
        "report_url": f"archive://{relative.as_posix()}",
        "report_date": report_date,
    }


def _existing_hash_row(conn, pdf_hash: str):
    return conn.execute(
        """
        SELECT id, source, ticker, company, broker, report_date, title
        FROM analyst_reports
        WHERE pdf_hash = ?
        """,
        (pdf_hash,),
    ).fetchone()


def _insert_extracted_report(conn, report: dict, pdf_hash: str, extracted: dict, normalize_estimates) -> int:
    if extracted.get("ticker"):
        report["ticker"] = extracted["ticker"]
    if extracted.get("company"):
        report["company"] = extracted["company"]
    if extracted.get("broker"):
        report["broker"] = extracted["broker"]
    if extracted.get("report_date"):
        report["report_date"] = extracted["report_date"]

    report["pdf_hash"] = pdf_hash
    extracted["estimates"] = normalize_estimates(conn, report, extracted)

    report_id = insert_report(conn, report)
    if not report_id:
        return 0

    target_price = extracted.get("target_price")
    recommendation = extracted.get("recommendation")
    for estimate in extracted.get("estimates", []):
        fiscal_year = estimate.get("fiscal_year")
        fwd_eps = estimate.get("fwd_eps")
        if not fiscal_year or fwd_eps is None:
            continue
        insert_eps(
            conn,
            {
                "report_id": report_id,
                "ticker": report["ticker"],
                "broker": report["broker"],
                "fiscal_year": fiscal_year,
                "fwd_eps": fwd_eps,
                "target_price": target_price,
                "recommendation": recommendation,
            },
        )

    return report_id


def _json_dumps_or_none(value) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _record_gemini_extraction(conn, report: dict, status: str, metadata: dict, report_id: Optional[int] = None):
    insert_gemini_extraction(
        conn,
        {
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
            "error": metadata.get("error"),
            "raw_response": metadata.get("raw_response"),
            "parsed_payload": _json_dumps_or_none(metadata.get("parsed_payload")),
            "normalized_payload": _json_dumps_or_none(metadata.get("normalized_payload")),
        },
    )


def main():
    parser = argparse.ArgumentParser(
        description="Reprocess archived report PDFs into analyst_reports and eps_estimates."
    )
    parser.add_argument(
        "--source",
        choices=["naver", "bondweb"],
        help="Only reprocess one source. Omit to scan both Naver and Bondweb archives.",
    )
    parser.add_argument("--ticker", help="Only reprocess one ticker, e.g. 278470.")
    parser.add_argument("--company", help="Only reprocess one company folder, e.g. 에이피알.")
    parser.add_argument("--limit", type=int, help="Only inspect the first N archived PDFs.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Delay after each Gemini call.")
    parser.add_argument("--apply", action="store_true", help="Write extracted reports into the DB.")
    args = parser.parse_args()

    ticker = str(args.ticker).zfill(6) if args.ticker else None
    files = _archive_files(args.source, ticker, args.company, args.limit)
    print(
        f"Found {len(files)} archived PDF(s)"
        + (f" for source={args.source}" if args.source else " across naver and bondweb")
        + "."
    )
    if not args.apply:
        print("Dry run only. Add --apply to extract and insert missing reports.")
        for path in files[:20]:
            report = _parse_report_from_path(path)
            label = (
                f"{report['report_date']} | {report['source']} | {report['ticker']} | "
                f"{report['company']} | {report['broker']}"
                if report
                else str(path)
            )
            print(f"  {label}")
        if len(files) > 20:
            print(f"  ... and {len(files) - 20} more.")
        return

    from extractor.gemini import extract_eps_from_pdf
    from monitor import _normalize_estimates

    conn = get_conn()
    try:
        kospi200_tickers = {row["ticker"] for row in get_kospi200(conn)}
        inserted = 0
        skipped_existing_hash = 0
        skipped_parse = 0
        skipped_non_kospi = 0
        extraction_failed = 0
        insert_failed = 0

        for idx, path in enumerate(files, start=1):
            report = _parse_report_from_path(path)
            if not report:
                skipped_parse += 1
                print(f"[{idx}/{len(files)}] Could not parse archive path: {path}")
                continue
            if kospi200_tickers and report["ticker"] not in kospi200_tickers:
                skipped_non_kospi += 1
                continue

            pdf_bytes = path.read_bytes()
            pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()
            report["pdf_hash"] = pdf_hash
            report["local_pdf_path"] = str(path)
            existing = _existing_hash_row(conn, pdf_hash)
            if existing:
                skipped_existing_hash += 1
                if skipped_existing_hash == 1 or skipped_existing_hash % 25 == 0:
                    print(
                        f"[{idx}/{len(files)}] Duplicate PDF hash, skipping "
                        f"{report['source']} {report['ticker']} {report['broker']} "
                        f"({existing['source']} report_id={existing['id']} already exists)"
                    )
                continue

            print(
                f"[{idx}/{len(files)}] Extracting {report['source']} "
                f"{report['company']} ({report['ticker']}) — {report['broker']}"
            )
            extracted, gemini_error, gemini_metadata = extract_eps_from_pdf(
                pdf_bytes,
                return_error=True,
                return_metadata=True,
            )
            if not extracted:
                extraction_failed += 1
                gemini_error = gemini_error or "Gemini extraction returned no usable data during archive reprocess"
                print(f"    [!] Gemini extraction failed: {gemini_error}")
                _record_gemini_extraction(conn, report, "gemini_failed", gemini_metadata or {})
                if not _should_retry_gemini_failure(gemini_error):
                    conn.commit()
                    print("    Not retrying because Gemini reported a permanent extraction failure.")
                else:
                    upsert_gemini_retry(
                        conn,
                        report,
                        GEMINI_RETRY_DELAY_MINUTES,
                        gemini_error,
                    )
                    conn.commit()
                    print(f"    Retry scheduled in {GEMINI_RETRY_DELAY_MINUTES} minutes.")
                continue

            report_id = _insert_extracted_report(conn, report, pdf_hash, extracted, _normalize_estimates)
            if not report_id:
                insert_failed += 1
                print("    [!] DB insert was ignored.")
                conn.rollback()
                _record_gemini_extraction(conn, report, "insert_duplicate", gemini_metadata or {})
                conn.commit()
                continue

            _record_gemini_extraction(conn, report, "success", gemini_metadata or {}, report_id)
            conn.commit()
            inserted += 1
            print(f"    Inserted report_id={report_id}")
            if args.sleep:
                time.sleep(args.sleep)

        print(
            "Done. "
            f"inserted={inserted} existing_hash={skipped_existing_hash} "
            f"parse_fail={skipped_parse} non_kospi={skipped_non_kospi} "
            f"extract_fail={extraction_failed} insert_fail={insert_failed} total={len(files)}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
