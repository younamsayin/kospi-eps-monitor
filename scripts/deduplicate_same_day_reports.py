import argparse
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import get_conn


SOURCE_PRIORITY = {
    "naver": 2,
    "bondweb": 1,
    "": 0,
    None: 0,
}


def _get_duplicate_groups(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        WITH report_stats AS (
            SELECT
                r.id,
                r.ticker,
                r.company,
                r.broker,
                r.source,
                r.title,
                r.report_url,
                r.report_date,
                COUNT(e.id) AS eps_rows,
                SUM(CASE WHEN e.fwd_eps IS NOT NULL THEN 1 ELSE 0 END) AS eps_nonnull,
                MAX(CASE WHEN e.target_price IS NOT NULL THEN 1 ELSE 0 END) AS has_tp,
                MAX(e.extracted_at) AS latest_extracted_at
            FROM analyst_reports r
            LEFT JOIN eps_estimates e ON e.report_id = r.id
            WHERE COALESCE(r.ticker, '') != ''
              AND COALESCE(r.broker, '') != ''
              AND COALESCE(r.report_date, '') != ''
            GROUP BY r.id
        )
        SELECT *
        FROM report_stats
        WHERE (ticker, broker, report_date) IN (
            SELECT ticker, broker, report_date
            FROM report_stats
            GROUP BY ticker, broker, report_date
            HAVING COUNT(*) > 1
        )
        ORDER BY ticker, broker, report_date, id
        """
    ).fetchall()


def _group_rows(rows: list[sqlite3.Row]) -> list[dict]:
    groups = {}
    for row in rows:
        key = (row["ticker"], row["broker"], row["report_date"])
        groups.setdefault(
            key,
            {
                "ticker": row["ticker"],
                "company": row["company"],
                "broker": row["broker"],
                "report_date": row["report_date"],
                "rows": [],
            },
        )["rows"].append(row)
    return list(groups.values())


def _row_score(row: sqlite3.Row) -> tuple:
    eps_nonnull = int(row["eps_nonnull"] or 0)
    eps_rows = int(row["eps_rows"] or 0)
    has_tp = int(row["has_tp"] or 0)
    source_priority = SOURCE_PRIORITY.get(row["source"], 0)
    latest_extracted_at = row["latest_extracted_at"] or ""
    return (
        1 if eps_nonnull > 0 else 0,
        has_tp,
        eps_nonnull,
        eps_rows,
        source_priority,
        latest_extracted_at,
        -int(row["id"]),
    )


def _choose_keeper(group: dict) -> sqlite3.Row:
    return max(group["rows"], key=_row_score)


def _build_plan(groups: list[dict]) -> list[dict]:
    plan = []
    for group in groups:
        keeper = _choose_keeper(group)
        losers = [row for row in group["rows"] if row["id"] != keeper["id"]]
        if not losers:
            continue
        plan.append(
            {
                "ticker": group["ticker"],
                "company": group["company"],
                "broker": group["broker"],
                "report_date": group["report_date"],
                "keeper": keeper,
                "losers": losers,
            }
        )
    return plan


def _apply_plan(conn: sqlite3.Connection, plan: list[dict]) -> tuple[int, int]:
    deleted_reports = 0
    deleted_eps = 0
    for item in plan:
        for loser in item["losers"]:
            deleted_eps += conn.execute(
                "DELETE FROM eps_estimates WHERE report_id = ?",
                (loser["id"],),
            ).rowcount
            deleted_reports += conn.execute(
                "DELETE FROM analyst_reports WHERE id = ?",
                (loser["id"],),
            ).rowcount
    return deleted_reports, deleted_eps


def _print_plan(plan: list[dict], limit: int):
    if not plan:
        print("No same-broker same-date duplicate groups found.")
        return

    total_duplicate_reports = sum(len(item["losers"]) for item in plan)
    print(
        f"Found {len(plan)} duplicate group(s); "
        f"{total_duplicate_reports} report row(s) would be deleted."
    )
    print()

    for item in plan[:limit]:
        keeper = item["keeper"]
        print(
            f"{item['report_date']} | {item['ticker']} | {item['company']} | {item['broker']}"
        )
        print(
            "  keep : "
            f"id={keeper['id']} source={keeper['source']} "
            f"eps={int(keeper['eps_nonnull'] or 0)}/{int(keeper['eps_rows'] or 0)} "
            f"tp={int(keeper['has_tp'] or 0)}"
        )
        print(f"         {keeper['title']}")
        for loser in item["losers"]:
            print(
                "  drop : "
                f"id={loser['id']} source={loser['source']} "
                f"eps={int(loser['eps_nonnull'] or 0)}/{int(loser['eps_rows'] or 0)} "
                f"tp={int(loser['has_tp'] or 0)}"
            )
            print(f"         {loser['title']}")
        print()

    remaining = len(plan) - limit
    if remaining > 0:
        print(f"... and {remaining} more.")


def main():
    parser = argparse.ArgumentParser(
        description="Delete duplicate reports from the same broker on the same report date."
    )
    parser.add_argument("--apply", action="store_true", help="Apply the deduplication to the database.")
    parser.add_argument("--limit", type=int, default=20, help="How many groups to print.")
    args = parser.parse_args()

    conn = get_conn()
    try:
        groups = _group_rows(_get_duplicate_groups(conn))
        plan = _build_plan(groups)
        _print_plan(plan, limit=args.limit)
        if not args.apply:
            return

        deleted_reports, deleted_eps = _apply_plan(conn, plan)
        conn.commit()
        print(
            f"Applied deduplication: deleted {deleted_reports} report row(s) "
            f"and {deleted_eps} EPS row(s)."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
