import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import get_conn, init_db


def _print_rows(rows, empty_message: str):
    if not rows:
        print(empty_message)
        return
    for row in rows:
        print("  " + " | ".join("" if value is None else str(value) for value in row))


def main():
    parser = argparse.ArgumentParser(description="Audit scraper -> Gemini -> DB ingestion health.")
    parser.add_argument("--limit", type=int, default=25, help="Rows to show per detail section.")
    args = parser.parse_args()

    init_db()
    conn = get_conn()
    try:
        print("Source report counts")
        _print_rows(
            conn.execute(
                """
                SELECT COALESCE(source, 'unknown') AS source, COUNT(*) AS reports
                FROM analyst_reports
                GROUP BY COALESCE(source, 'unknown')
                ORDER BY reports DESC
                """
            ).fetchall(),
            "  No analyst reports found.",
        )

        print("\nReports with no EPS estimates")
        _print_rows(
            conn.execute(
                """
                SELECT r.id, r.source, r.report_date, r.ticker, r.company, r.broker, r.title
                FROM analyst_reports r
                LEFT JOIN eps_estimates e ON e.report_id = r.id
                GROUP BY r.id
                HAVING COUNT(e.id) = 0
                ORDER BY r.report_date DESC, r.id DESC
                LIMIT ?
                """,
                (args.limit,),
            ).fetchall(),
            "  None.",
        )

        print("\nGemini retry queue")
        _print_rows(
            conn.execute(
                """
                SELECT source, next_retry_at, attempts, ticker, company, broker, title, last_error
                FROM gemini_extraction_retries
                ORDER BY next_retry_at, id
                LIMIT ?
                """,
                (args.limit,),
            ).fetchall(),
            "  Empty.",
        )

        print("\nRecent ingestion failures / skips")
        _print_rows(
            conn.execute(
                """
                SELECT created_at, source, stage, status, ticker, company, broker, title, message
                FROM ingestion_events
                WHERE status NOT IN ('found', 'archived', 'saved', 'existing_url', 'existing_after_retry')
                ORDER BY id DESC
                LIMIT ?
                """,
                (args.limit,),
            ).fetchall(),
            "  None.",
        )

        print("\nFailure counts by source/status")
        _print_rows(
            conn.execute(
                """
                SELECT source, status, COUNT(*) AS count
                FROM ingestion_events
                WHERE status NOT IN ('found', 'archived', 'saved', 'existing_url', 'existing_after_retry')
                GROUP BY source, status
                ORDER BY count DESC, source, status
                LIMIT ?
                """,
                (args.limit,),
            ).fetchall(),
            "  None.",
        )

        print("\nDuplicate PDF hash groups")
        _print_rows(
            conn.execute(
                """
                SELECT pdf_hash, COUNT(*) AS reports
                FROM analyst_reports
                WHERE pdf_hash IS NOT NULL AND pdf_hash != ''
                GROUP BY pdf_hash
                HAVING COUNT(*) > 1
                ORDER BY reports DESC
                LIMIT ?
                """,
                (args.limit,),
            ).fetchall(),
            "  None.",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
