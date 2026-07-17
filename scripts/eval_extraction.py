"""
Golden-set evaluation harness for the Gemini EPS/TP extractor.

Turns prompt/model changes from guesswork into measured field-level accuracy.

Workflow:
  1. Seed a candidate golden set from already-ingested reports:
         python3 scripts/eval_extraction.py --seed 40
     This samples archived PDFs and writes tests/golden/golden_set.json with
     the CURRENT extractor's values and "verified": false.
  2. Hand-verify each entry against the PDF (fix values where the extractor
     was wrong!) and set "verified": true. Entries left unverified are skipped.
  3. Evaluate any model/prompt against the verified set:
         python3 scripts/eval_extraction.py
         python3 scripts/eval_extraction.py --model gemini-3.1-pro-preview

Comparison tolerances: ticker exact; TP and per-FY EPS within 0.5%.
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.models import get_conn  # noqa: E402
from extractor.gemini import extract_eps_from_pdf, MODEL, PROMPT_VERSION  # noqa: E402

GOLDEN_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "golden", "golden_set.json",
)
RELATIVE_TOLERANCE = 0.005


def _within_tolerance(expected, actual) -> bool:
    if expected is None and actual is None:
        return True
    if expected is None or actual is None:
        return False
    expected, actual = float(expected), float(actual)
    denom = max(abs(expected), abs(actual))
    if denom == 0:
        return True
    return abs(expected - actual) / denom <= RELATIVE_TOLERANCE


def seed(sample_size: int):
    conn = get_conn()
    # analyst_reports has no local_pdf_path column; resolve paths via gemini_extractions
    rows = conn.execute(
        """
        SELECT r.id AS report_id, r.ticker, r.company, r.broker, r.report_date, r.title,
               r.target_price,
               (SELECT ge.local_pdf_path FROM gemini_extractions ge
                WHERE ge.report_id = r.id AND ge.local_pdf_path IS NOT NULL
                ORDER BY ge.id DESC LIMIT 1) AS local_pdf_path
        FROM analyst_reports r
        WHERE EXISTS (SELECT 1 FROM eps_estimates e WHERE e.report_id = r.id)
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (sample_size * 3,),
    ).fetchall()

    entries = []
    for row in rows:
        if len(entries) >= sample_size:
            break
        pdf_path = row["local_pdf_path"]
        if not pdf_path or not os.path.exists(pdf_path):
            continue
        estimates = conn.execute(
            """
            SELECT fiscal_year, fwd_eps FROM eps_estimates
            WHERE report_id = ? AND fwd_eps IS NOT NULL
            ORDER BY fiscal_year
            """,
            (row["report_id"],),
        ).fetchall()
        entries.append({
            "pdf_path": os.path.relpath(
                pdf_path,
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            ),
            "company": row["company"],
            "broker": row["broker"],
            "report_date": row["report_date"],
            "title": row["title"],
            "verified": False,
            "expected": {
                "ticker": row["ticker"],
                "target_price": row["target_price"],
                "estimates": {str(e["fiscal_year"]): e["fwd_eps"] for e in estimates},
            },
        })
    conn.close()

    os.makedirs(os.path.dirname(GOLDEN_PATH), exist_ok=True)
    with open(GOLDEN_PATH, "w", encoding="utf-8") as f:
        json.dump({"entries": entries}, f, ensure_ascii=False, indent=2)
    print(f"Seeded {len(entries)} candidate entries at {GOLDEN_PATH}")
    print("Hand-verify each entry against its PDF, correct any wrong values,")
    print('then set "verified": true. Unverified entries are skipped by the eval.')


def evaluate(model, include_unverified: bool, limit: int):
    with open(GOLDEN_PATH, encoding="utf-8") as f:
        golden = json.load(f)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    entries = [
        e for e in golden.get("entries", [])
        if (e.get("verified") or include_unverified)
    ][: limit or None]
    if not entries:
        print("No usable golden entries. Run --seed first, then verify entries.")
        return

    print(f"Evaluating model={model or MODEL} prompt={PROMPT_VERSION} on {len(entries)} entries")
    stats = {"ticker": [0, 0], "target_price": [0, 0], "eps": [0, 0], "failed": 0}
    failures = []

    for i, entry in enumerate(entries, start=1):
        pdf_path = entry["pdf_path"]
        if not os.path.isabs(pdf_path):
            pdf_path = os.path.join(project_root, pdf_path)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        result = extract_eps_from_pdf(pdf_bytes, model=model)
        label = f"{entry.get('company')} / {entry.get('broker')} / {entry.get('report_date')}"
        if not result:
            stats["failed"] += 1
            failures.append(f"[extraction failed] {label}")
            print(f"  [{i}/{len(entries)}] FAIL(extract) {label}")
            continue

        expected = entry["expected"]
        ticker_ok = str(result.get("ticker") or "") == str(expected.get("ticker") or "")
        stats["ticker"][0] += int(ticker_ok)
        stats["ticker"][1] += 1
        if not ticker_ok:
            failures.append(f"[ticker] {label}: expected {expected.get('ticker')} got {result.get('ticker')}")

        tp_ok = _within_tolerance(expected.get("target_price"), result.get("target_price"))
        stats["target_price"][0] += int(tp_ok)
        stats["target_price"][1] += 1
        if not tp_ok:
            failures.append(
                f"[tp] {label}: expected {expected.get('target_price')} got {result.get('target_price')}"
            )

        actual_eps = {}
        for est in result.get("estimates") or []:
            if est.get("fiscal_year") is not None and est.get("fwd_eps") is not None:
                actual_eps[str(est["fiscal_year"])] = est["fwd_eps"]
        for fy, expected_eps in (expected.get("estimates") or {}).items():
            eps_ok = _within_tolerance(expected_eps, actual_eps.get(fy))
            stats["eps"][0] += int(eps_ok)
            stats["eps"][1] += 1
            if not eps_ok:
                failures.append(
                    f"[eps FY{fy}] {label}: expected {expected_eps} got {actual_eps.get(fy)}"
                )
        print(f"  [{i}/{len(entries)}] done {label}")
        time.sleep(1)

    print("\n=== Results ===")
    print(f"extraction failures: {stats['failed']}/{len(entries)}")
    for field in ("ticker", "target_price", "eps"):
        ok, total = stats[field]
        pct = 100.0 * ok / total if total else 0.0
        print(f"{field:>13}: {ok}/{total} ({pct:.1f}%)")
    if failures:
        print("\n=== Mismatches ===")
        for line in failures:
            print(" -", line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, metavar="N", help="seed N candidate entries from the DB")
    parser.add_argument("--model", help="model override for evaluation")
    parser.add_argument("--include-unverified", action="store_true",
                        help="also evaluate unverified entries (measures drift, not accuracy)")
    parser.add_argument("--limit", type=int, default=0, help="evaluate at most N entries")
    args = parser.parse_args()

    if args.seed:
        seed(args.seed)
    else:
        evaluate(args.model, args.include_unverified, args.limit)


if __name__ == "__main__":
    main()
