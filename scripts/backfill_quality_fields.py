"""
Backfill quality-tracking fields added by the TP/EPS quality upgrade:

  1. Canonicalize broker names (rename/merger aliases) in analyst_reports,
     eps_estimates, and gemini_extraction_retries, preserving the raw name
     in analyst_reports.broker_raw.
  2. Populate eps_estimates.recommendation_norm from the raw recommendation.
  3. Populate report-level analyst_reports.target_price / recommendation /
     recommendation_norm from each report's estimate rows.

Dry-run by default; pass --apply to write. Rollback: restore the DB backup
taken before the migration (see ROLLBACK.md).

Usage:
    python3 scripts/backfill_quality_fields.py            # dry run
    python3 scripts/backfill_quality_fields.py --apply
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.models import get_conn, init_db  # noqa: E402
from normalization import BROKER_ALIASES, normalize_recommendation  # noqa: E402


def backfill_brokers(conn, apply: bool) -> int:
    total = 0
    for alias, canonical in sorted(BROKER_ALIASES.items()):
        report_count = conn.execute(
            "SELECT COUNT(*) FROM analyst_reports WHERE broker = ?", (alias,)
        ).fetchone()[0]
        eps_count = conn.execute(
            "SELECT COUNT(*) FROM eps_estimates WHERE broker = ?", (alias,)
        ).fetchone()[0]
        retry_count = conn.execute(
            "SELECT COUNT(*) FROM gemini_extraction_retries WHERE broker = ?", (alias,)
        ).fetchone()[0]
        if not (report_count or eps_count or retry_count):
            continue
        total += report_count + eps_count + retry_count
        print(f"  {alias} -> {canonical}: reports={report_count} estimates={eps_count} retries={retry_count}")
        if apply:
            conn.execute(
                """
                UPDATE analyst_reports
                SET broker_raw = COALESCE(broker_raw, broker), broker = ?
                WHERE broker = ?
                """,
                (canonical, alias),
            )
            conn.execute("UPDATE eps_estimates SET broker = ? WHERE broker = ?", (canonical, alias))
            conn.execute("UPDATE gemini_extraction_retries SET broker = ? WHERE broker = ?", (canonical, alias))
    return total


def backfill_recommendation_norm(conn, apply: bool) -> int:
    rows = conn.execute(
        """
        SELECT DISTINCT recommendation FROM eps_estimates
        WHERE recommendation IS NOT NULL AND recommendation_norm IS NULL
        """
    ).fetchall()
    total = 0
    for row in rows:
        raw = row["recommendation"]
        norm = normalize_recommendation(raw)
        if norm is None:
            continue
        count = conn.execute(
            "SELECT COUNT(*) FROM eps_estimates WHERE recommendation = ? AND recommendation_norm IS NULL",
            (raw,),
        ).fetchone()[0]
        total += count
        print(f"  {raw!r} -> {norm}: {count} estimate row(s)")
        if apply:
            conn.execute(
                "UPDATE eps_estimates SET recommendation_norm = ? WHERE recommendation = ? AND recommendation_norm IS NULL",
                (norm, raw),
            )
    return total


def backfill_report_level_tp(conn, apply: bool) -> int:
    count = conn.execute(
        """
        SELECT COUNT(*) FROM analyst_reports r
        WHERE r.target_price IS NULL
          AND EXISTS (
              SELECT 1 FROM eps_estimates e
              WHERE e.report_id = r.id AND e.target_price IS NOT NULL
          )
        """
    ).fetchone()[0]
    print(f"  reports needing report-level TP: {count}")
    if apply and count:
        conn.execute(
            """
            UPDATE analyst_reports
            SET target_price = (
                SELECT MAX(e.target_price) FROM eps_estimates e
                WHERE e.report_id = analyst_reports.id
            )
            WHERE target_price IS NULL
              AND EXISTS (
                  SELECT 1 FROM eps_estimates e
                  WHERE e.report_id = analyst_reports.id AND e.target_price IS NOT NULL
              )
            """
        )
    rec_count = conn.execute(
        """
        SELECT COUNT(*) FROM analyst_reports r
        WHERE r.recommendation IS NULL
          AND EXISTS (
              SELECT 1 FROM eps_estimates e
              WHERE e.report_id = r.id AND e.recommendation IS NOT NULL
          )
        """
    ).fetchone()[0]
    print(f"  reports needing report-level recommendation: {rec_count}")
    if apply and rec_count:
        conn.execute(
            """
            UPDATE analyst_reports
            SET recommendation = (
                SELECT e.recommendation FROM eps_estimates e
                WHERE e.report_id = analyst_reports.id AND e.recommendation IS NOT NULL
                ORDER BY e.id LIMIT 1
            ),
            recommendation_norm = (
                SELECT e.recommendation_norm FROM eps_estimates e
                WHERE e.report_id = analyst_reports.id AND e.recommendation_norm IS NOT NULL
                ORDER BY e.id LIMIT 1
            )
            WHERE recommendation IS NULL
              AND EXISTS (
                  SELECT 1 FROM eps_estimates e
                  WHERE e.report_id = analyst_reports.id AND e.recommendation IS NOT NULL
              )
            """
        )
    return count + rec_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write changes (default is dry run)")
    args = parser.parse_args()

    init_db()
    conn = get_conn()
    try:
        mode = "APPLY" if args.apply else "DRY RUN"
        print(f"[{mode}] 1/3 Broker canonicalization")
        brokers = backfill_brokers(conn, args.apply)
        print(f"[{mode}] 2/3 Recommendation normalization")
        recs = backfill_recommendation_norm(conn, args.apply)
        print(f"[{mode}] 3/3 Report-level target price / recommendation")
        tps = backfill_report_level_tp(conn, args.apply)
        if args.apply:
            conn.commit()
            print(f"Done. Updated rows: brokers={brokers} recommendations={recs} report_level={tps}")
        else:
            print(f"Dry run only. Would update: brokers={brokers} recommendations={recs} report_level={tps}")
            print("Re-run with --apply to write changes.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
