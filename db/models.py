import os
import sqlite3
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get("DB_PATH", "kospi_eps.db")

DDL = """
CREATE TABLE IF NOT EXISTS kospi200 (
    ticker      TEXT PRIMARY KEY,
    company     TEXT NOT NULL,
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS analyst_reports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    company     TEXT,
    broker      TEXT,
    source      TEXT,
    title       TEXT,
    report_url  TEXT UNIQUE,
    report_date TEXT,
    fetched_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS eps_estimates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id       INTEGER REFERENCES analyst_reports(id),
    ticker          TEXT NOT NULL,
    broker          TEXT,
    fiscal_year     INTEGER,
    fwd_eps         REAL,
    target_price    REAL,
    recommendation  TEXT,
    extracted_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_eps_ticker_year ON eps_estimates(ticker, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_reports_ticker   ON analyst_reports(ticker);
"""


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript(DDL)
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(analyst_reports)").fetchall()
        }
        if "source" not in columns:
            conn.execute("ALTER TABLE analyst_reports ADD COLUMN source TEXT")
        conn.execute(
            """
            UPDATE analyst_reports
            SET source = CASE
                WHEN report_url LIKE '%bondweb.co.kr%' THEN 'bondweb'
                WHEN report_url LIKE '%stock.pstatic.net%' THEN 'naver'
                WHEN report_url LIKE '%finance.naver.com%' THEN 'naver'
                ELSE source
            END
            WHERE source IS NULL OR source = ''
            """
        )
    print(f"Database initialized at {DB_PATH}")


def upsert_kospi200(tickers: list):
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO kospi200 (ticker, company, updated_at)
            VALUES (:ticker, :company, datetime('now'))
            ON CONFLICT(ticker) DO UPDATE
                SET company = excluded.company, updated_at = excluded.updated_at
            """,
            tickers,
        )


def get_kospi200(conn) -> list:
    return conn.execute("SELECT ticker, company FROM kospi200 ORDER BY ticker").fetchall()


def report_exists(conn, report_url: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM analyst_reports WHERE report_url = ?", (report_url,)
    ).fetchone()
    return row is not None


def insert_report(conn, report: dict) -> int:
    cur = conn.execute(
        """
        INSERT INTO analyst_reports (ticker, company, broker, source, title, report_url, report_date)
        VALUES (:ticker, :company, :broker, :source, :title, :report_url, :report_date)
        """,
        report,
    )
    return cur.lastrowid


def insert_eps(conn, estimate: dict):
    conn.execute(
        """
        INSERT INTO eps_estimates (report_id, ticker, broker, fiscal_year, fwd_eps, target_price, recommendation)
        VALUES (:report_id, :ticker, :broker, :fiscal_year, :fwd_eps, :target_price, :recommendation)
        """,
        estimate,
    )


def get_previous_eps(conn, ticker: str, fiscal_year: int, broker: str) -> Optional[float]:
    row = conn.execute(
        """
        SELECT fwd_eps FROM eps_estimates
        WHERE ticker = ? AND fiscal_year = ? AND broker = ?
        ORDER BY extracted_at DESC
        LIMIT 1
        """,
        (ticker, fiscal_year, broker),
    ).fetchone()
    return float(row["fwd_eps"]) if row else None


def get_previous_eps_record(conn, ticker: str, fiscal_year: int, broker: str, current_report_date):
    return conn.execute(
        """
        SELECT
            e.fwd_eps,
            r.report_date,
            r.report_url
        FROM eps_estimates e
        JOIN analyst_reports r ON e.report_id = r.id
        WHERE e.ticker = ? AND e.fiscal_year = ? AND e.broker = ?
          AND r.report_date < ?
        ORDER BY r.report_date DESC, e.extracted_at DESC, e.id DESC
        LIMIT 1
        """,
        (ticker, fiscal_year, broker, current_report_date),
    ).fetchone()


def get_previous_target_price(conn, ticker: str, broker: str) -> Optional[float]:
    row = conn.execute(
        """
        SELECT target_price FROM eps_estimates
        WHERE ticker = ? AND broker = ? AND target_price IS NOT NULL
        ORDER BY extracted_at DESC
        LIMIT 1
        """,
        (ticker, broker),
    ).fetchone()
    return float(row["target_price"]) if row else None


def get_previous_target_price_record(conn, ticker: str, broker: str, current_report_date):
    return conn.execute(
        """
        SELECT
            e.target_price,
            r.report_date,
            r.report_url
        FROM eps_estimates e
        JOIN analyst_reports r ON e.report_id = r.id
        WHERE e.ticker = ? AND e.broker = ? AND e.target_price IS NOT NULL
          AND r.report_date < ?
        ORDER BY r.report_date DESC, e.extracted_at DESC, e.id DESC
        LIMIT 1
        """,
        (ticker, broker, current_report_date),
    ).fetchone()


def get_latest_prior_report_estimates(conn, ticker: str, broker: str, current_report_date):
    latest_prior = conn.execute(
        """
        SELECT MAX(report_date) AS report_date
        FROM analyst_reports
        WHERE ticker = ? AND broker = ? AND report_date < ?
        """,
        (ticker, broker, current_report_date),
    ).fetchone()

    report_date = latest_prior["report_date"] if latest_prior else None
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
