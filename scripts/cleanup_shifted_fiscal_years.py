import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import get_conn, get_latest_prior_report_estimates


MIN_SHIFT_MATCHES = 2
MIN_SCORE_DELTA = 0.75


def _relative_gap(current: float, previous: float) -> float:
    if previous in (None, 0):
        return float("inf")
    return abs(current - previous) / abs(previous)


def _estimate_shift_score(current_map: dict, reference_map: dict, shift: int) -> tuple[float, int]:
    score = 0.0
    close_matches = 0
    for fiscal_year, current_eps in current_map.items():
        reference_eps = reference_map.get(fiscal_year + shift)
        if reference_eps is None:
            continue
        gap = _relative_gap(current_eps, reference_eps)
        score += max(0.0, 1.0 - min(gap, 1.0))
        if gap <= 0.05:
            close_matches += 1
    return score, close_matches


def _iter_report_rows(conn: sqlite3.Connection):
    return conn.execute(
        """
        SELECT
            r.id AS report_id,
            r.ticker,
            r.company,
            r.broker,
            r.report_date,
            r.title
        FROM analyst_reports r
        WHERE r.ticker != ''
          AND COALESCE(r.broker, '') != ''
          AND EXISTS (
              SELECT 1
              FROM eps_estimates e
              WHERE e.report_id = r.id
                AND e.fwd_eps IS NOT NULL
          )
        ORDER BY r.ticker, r.broker, r.report_date, r.id
        """
    ).fetchall()


def _get_report_estimates(conn: sqlite3.Connection, report_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, fiscal_year, fwd_eps
        FROM eps_estimates
        WHERE report_id = ?
          AND fwd_eps IS NOT NULL
        ORDER BY fiscal_year, extracted_at, id
        """,
        (report_id,),
    ).fetchall()


def _build_current_map(rows: list[sqlite3.Row]) -> dict[int, float]:
    result = {}
    for row in rows:
        fiscal_year = row["fiscal_year"]
        fwd_eps = row["fwd_eps"]
        if fiscal_year is None or fwd_eps is None:
            continue
        result[int(fiscal_year)] = float(fwd_eps)
    return result


def _report_year(report_date: Optional[str]) -> Optional[int]:
    if not report_date:
        return None
    try:
        return int(str(report_date)[:4])
    except (TypeError, ValueError):
        return None


def _get_nearest_next_report_estimates(
    conn: sqlite3.Connection, ticker: str, broker: str, current_report_date: str
) -> dict[int, float]:
    next_row = conn.execute(
        """
        SELECT MIN(report_date) AS report_date
        FROM analyst_reports
        WHERE ticker = ? AND broker = ? AND report_date > ?
        """,
        (ticker, broker, current_report_date),
    ).fetchone()

    report_date = next_row["report_date"] if next_row else None
    if not report_date:
        return {}

    rows = conn.execute(
        """
        WITH latest_per_year AS (
            SELECT
                e.fiscal_year,
                e.fwd_eps,
                ROW_NUMBER() OVER (
                    PARTITION BY e.fiscal_year
                    ORDER BY e.extracted_at DESC, e.id DESC
                ) AS rn
            FROM eps_estimates e
            JOIN analyst_reports r ON e.report_id = r.id
            WHERE e.ticker = ?
              AND e.broker = ?
              AND r.report_date = ?
              AND e.fwd_eps IS NOT NULL
        )
        SELECT fiscal_year, fwd_eps
        FROM latest_per_year
        WHERE rn = 1
        """,
        (ticker, broker, report_date),
    ).fetchall()

    return {int(row["fiscal_year"]): float(row["fwd_eps"]) for row in rows if row["fiscal_year"] is not None}


def _shift_signal(current_map: dict, reference_map: dict) -> Optional[dict]:
    if len(current_map) < 3 or len(reference_map) < 2:
        return None

    no_shift_score, no_shift_matches = _estimate_shift_score(current_map, reference_map, 0)
    shift_down_score, shift_down_matches = _estimate_shift_score(current_map, reference_map, -1)
    if shift_down_matches < MIN_SHIFT_MATCHES:
        return None
    if shift_down_score <= no_shift_score + MIN_SCORE_DELTA:
        return None

    return {
        "reference_map": reference_map,
        "no_shift_score": no_shift_score,
        "no_shift_matches": no_shift_matches,
        "shift_down_score": shift_down_score,
        "shift_down_matches": shift_down_matches,
    }


def _find_shift_candidates(conn: sqlite3.Connection) -> list[dict]:
    candidates = []
    for report in _iter_report_rows(conn):
        rows = _get_report_estimates(conn, report["report_id"])
        current_map = _build_current_map(rows)
        if len(current_map) < 3:
            continue

        previous_map = get_latest_prior_report_estimates(
            conn,
            report["ticker"],
            report["broker"],
            report["report_date"],
        )
        next_map = _get_nearest_next_report_estimates(
            conn,
            report["ticker"],
            report["broker"],
            report["report_date"],
        )
        signals = []
        previous_signal = _shift_signal(current_map, previous_map)
        if previous_signal:
            signals.append(("previous", previous_signal))
        next_signal = _shift_signal(current_map, next_map)
        if next_signal:
            signals.append(("next", next_signal))
        if not signals:
            continue

        preferred_source, preferred_signal = max(
            signals,
            key=lambda item: (
                item[1]["shift_down_matches"],
                item[1]["shift_down_score"] - item[1]["no_shift_score"],
                item[1]["shift_down_score"],
            ),
        )

        year_floor = _report_year(report["report_date"])
        shifted_rows = []
        dropped_rows = []
        for row in rows:
            old_year = int(row["fiscal_year"])
            new_year = old_year - 1
            row_info = {
                "id": int(row["id"]),
                "old_year": old_year,
                "new_year": new_year,
                "fwd_eps": float(row["fwd_eps"]),
            }
            if year_floor is not None and new_year < year_floor:
                dropped_rows.append(row_info)
            else:
                shifted_rows.append(row_info)

        if not shifted_rows and not dropped_rows:
            continue

        candidates.append(
            {
                "report_id": int(report["report_id"]),
                "ticker": report["ticker"],
                "company": report["company"],
                "broker": report["broker"],
                "report_date": report["report_date"],
                "title": report["title"],
                "current_map": current_map,
                "signal_source": preferred_source,
                "reference_map": preferred_signal["reference_map"],
                "no_shift_score": preferred_signal["no_shift_score"],
                "no_shift_matches": preferred_signal["no_shift_matches"],
                "shift_down_score": preferred_signal["shift_down_score"],
                "shift_down_matches": preferred_signal["shift_down_matches"],
                "shifted_rows": shifted_rows,
                "dropped_rows": dropped_rows,
            }
        )
    return candidates


def _apply_candidates(conn: sqlite3.Connection, candidates: list[dict]) -> tuple[int, int]:
    updated = 0
    deleted = 0
    for candidate in candidates:
        for row in candidate["shifted_rows"]:
            conn.execute(
                "UPDATE eps_estimates SET fiscal_year = ? WHERE id = ?",
                (row["new_year"], row["id"]),
            )
            updated += 1
        for row in candidate["dropped_rows"]:
            conn.execute(
                "DELETE FROM eps_estimates WHERE id = ?",
                (row["id"],),
            )
            deleted += 1
    return updated, deleted


def _print_candidates(candidates: list[dict], limit: int = 20):
    if not candidates:
        print("No high-confidence shifted-year records found.")
        return

    print(f"Found {len(candidates)} high-confidence shifted-year report(s).")
    print()

    for candidate in candidates[:limit]:
        print(
            f"{candidate['report_date']} | {candidate['ticker']} | {candidate['company']} | "
            f"{candidate['broker']} | report_id={candidate['report_id']}"
        )
        print(f"  title: {candidate['title']}")
        print(
            "  score: "
            f"no-shift={candidate['no_shift_score']:.2f} ({candidate['no_shift_matches']} matches), "
            f"shift-down={candidate['shift_down_score']:.2f} ({candidate['shift_down_matches']} matches)"
        )
        print(f"  signal : {candidate['signal_source']}")
        print(f"  compare: {candidate['reference_map']}")
        print(f"  current : {candidate['current_map']}")
        if candidate["shifted_rows"]:
            print(
                "  updates : "
                + ", ".join(
                    f"{row['old_year']}->{row['new_year']} ({row['fwd_eps']:,.0f})"
                    for row in candidate["shifted_rows"]
                )
            )
        if candidate["dropped_rows"]:
            print(
                "  drops   : "
                + ", ".join(
                    f"{row['old_year']} ({row['fwd_eps']:,.0f})"
                    for row in candidate["dropped_rows"]
                )
            )
        print()

    remaining = len(candidates) - limit
    if remaining > 0:
        print(f"... and {remaining} more.")


def main():
    parser = argparse.ArgumentParser(description="Clean historical EPS rows with likely shifted fiscal years.")
    parser.add_argument("--apply", action="store_true", help="Apply the detected fixes to the database.")
    parser.add_argument("--limit", type=int, default=20, help="How many candidate summaries to print.")
    args = parser.parse_args()

    conn = get_conn()
    try:
        candidates = _find_shift_candidates(conn)
        _print_candidates(candidates, limit=args.limit)
        if not args.apply:
            return

        updated, deleted = _apply_candidates(conn, candidates)
        conn.commit()
        print(f"Applied cleanup: updated {updated} EPS row(s), deleted {deleted} row(s).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
