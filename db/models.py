import os
import sqlite3
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("DB_PATH", os.path.join(PROJECT_ROOT, "kospi_eps.db"))
if not os.path.isabs(DB_PATH):
    DB_PATH = os.path.join(PROJECT_ROOT, DB_PATH)

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
    pdf_hash    TEXT,
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

CREATE TABLE IF NOT EXISTS favorite_companies (
    ticker      TEXT PRIMARY KEY,
    company     TEXT NOT NULL,
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS gemini_extraction_retries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    company         TEXT,
    broker          TEXT,
    source          TEXT,
    title           TEXT,
    report_url      TEXT,
    report_date     TEXT,
    pdf_hash        TEXT NOT NULL,
    local_pdf_path  TEXT NOT NULL,
    attempts        INTEGER DEFAULT 0,
    last_error      TEXT,
    next_retry_at   TEXT NOT NULL,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ingestion_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT,
    stage           TEXT NOT NULL,
    status          TEXT NOT NULL,
    ticker          TEXT,
    company         TEXT,
    broker          TEXT,
    title           TEXT,
    report_url      TEXT,
    pdf_hash        TEXT,
    report_date     TEXT,
    local_pdf_path  TEXT,
    message         TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_eps_ticker_year ON eps_estimates(ticker, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_reports_ticker   ON analyst_reports(ticker);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gemini_retries_pdf_hash ON gemini_extraction_retries(pdf_hash);
CREATE INDEX IF NOT EXISTS idx_gemini_retries_next_retry ON gemini_extraction_retries(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_source_status ON ingestion_events(source, status, created_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_report_url ON ingestion_events(report_url);
CREATE INDEX IF NOT EXISTS idx_ingestion_events_pdf_hash ON ingestion_events(pdf_hash);
"""


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def wal_checkpoint(conn):
    """Run a passive WAL checkpoint to keep the WAL file from growing unbounded."""
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception:
        pass


def init_db():
    with get_conn() as conn:
        conn.executescript(DDL)
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(analyst_reports)").fetchall()
        }
        if "source" not in columns:
            conn.execute("ALTER TABLE analyst_reports ADD COLUMN source TEXT")
        if "pdf_hash" not in columns:
            conn.execute("ALTER TABLE analyst_reports ADD COLUMN pdf_hash TEXT")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_pdf_hash ON analyst_reports(pdf_hash) WHERE pdf_hash IS NOT NULL"
        )
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


def get_report_by_url(conn, report_url: str):
    return conn.execute(
        "SELECT * FROM analyst_reports WHERE report_url = ?",
        (report_url,),
    ).fetchone()


def count_eps_estimates_for_report(conn, report_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS count FROM eps_estimates WHERE report_id = ?",
        (report_id,),
    ).fetchone()
    return int(row["count"] if row else 0)


def get_existing_report_urls(conn) -> set[str]:
    return {
        row["report_url"]
        for row in conn.execute(
            "SELECT report_url FROM analyst_reports WHERE report_url IS NOT NULL AND report_url != ''"
        ).fetchall()
    }


def report_exists_by_pdf_hash(conn, pdf_hash: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM analyst_reports WHERE pdf_hash = ?",
        (pdf_hash,),
    ).fetchone()
    return row is not None


def get_existing_pdf_hashes(conn) -> set[str]:
    return {
        row["pdf_hash"]
        for row in conn.execute(
            "SELECT pdf_hash FROM analyst_reports WHERE pdf_hash IS NOT NULL AND pdf_hash != ''"
        ).fetchall()
    }


def gemini_retry_exists_by_pdf_hash(conn, pdf_hash: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM gemini_extraction_retries WHERE pdf_hash = ?",
        (pdf_hash,),
    ).fetchone()
    return row is not None


def get_pending_gemini_retry_hashes(conn) -> set[str]:
    return {
        row["pdf_hash"]
        for row in conn.execute(
            "SELECT pdf_hash FROM gemini_extraction_retries WHERE pdf_hash IS NOT NULL AND pdf_hash != ''"
        ).fetchall()
    }


def insert_report(conn, report: dict) -> int:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO analyst_reports (ticker, company, broker, source, title, report_url, pdf_hash, report_date)
        VALUES (:ticker, :company, :broker, :source, :title, :report_url, :pdf_hash, :report_date)
        """,
        report,
    )
    return cur.lastrowid or 0


def insert_eps(conn, estimate: dict):
    conn.execute(
        """
        INSERT INTO eps_estimates (report_id, ticker, broker, fiscal_year, fwd_eps, target_price, recommendation)
        VALUES (:report_id, :ticker, :broker, :fiscal_year, :fwd_eps, :target_price, :recommendation)
        """,
        estimate,
    )


def insert_ingestion_event(conn, event: dict):
    conn.execute(
        """
        INSERT INTO ingestion_events (
            source, stage, status, ticker, company, broker, title,
            report_url, pdf_hash, report_date, local_pdf_path, message
        )
        VALUES (
            :source, :stage, :status, :ticker, :company, :broker, :title,
            :report_url, :pdf_hash, :report_date, :local_pdf_path, :message
        )
        """,
        {
            "source": event.get("source"),
            "stage": event.get("stage"),
            "status": event.get("status"),
            "ticker": event.get("ticker"),
            "company": event.get("company"),
            "broker": event.get("broker"),
            "title": event.get("title"),
            "report_url": event.get("report_url"),
            "pdf_hash": event.get("pdf_hash"),
            "report_date": event.get("report_date"),
            "local_pdf_path": event.get("local_pdf_path"),
            "message": event.get("message"),
        },
    )


def upsert_gemini_retry(conn, report: dict, retry_after_minutes: int, last_error: str):
    conn.execute(
        """
        INSERT INTO gemini_extraction_retries (
            ticker, company, broker, source, title, report_url, report_date,
            pdf_hash, local_pdf_path, attempts, last_error, next_retry_at, updated_at
        )
        VALUES (
            :ticker, :company, :broker, :source, :title, :report_url, :report_date,
            :pdf_hash, :local_pdf_path, 0, :last_error,
            datetime('now', :retry_after), datetime('now')
        )
        ON CONFLICT(pdf_hash) DO UPDATE SET
            ticker = excluded.ticker,
            company = excluded.company,
            broker = excluded.broker,
            source = excluded.source,
            title = excluded.title,
            report_url = excluded.report_url,
            report_date = excluded.report_date,
            local_pdf_path = excluded.local_pdf_path,
            last_error = excluded.last_error,
            next_retry_at = excluded.next_retry_at,
            updated_at = datetime('now')
        """,
        {
            "ticker": report.get("ticker") or "",
            "company": report.get("company"),
            "broker": report.get("broker"),
            "source": report.get("source"),
            "title": report.get("title"),
            "report_url": report.get("report_url"),
            "report_date": report.get("report_date"),
            "pdf_hash": report["pdf_hash"],
            "local_pdf_path": report["local_pdf_path"],
            "last_error": last_error,
            "retry_after": f"+{int(retry_after_minutes)} minutes",
        },
    )


def get_due_gemini_retries(conn, limit: int) -> list:
    return conn.execute(
        """
        SELECT *
        FROM gemini_extraction_retries
        WHERE next_retry_at <= datetime('now')
        ORDER BY next_retry_at, id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def delete_gemini_retry(conn, retry_id: int):
    conn.execute("DELETE FROM gemini_extraction_retries WHERE id = ?", (retry_id,))


def mark_gemini_retry_failed(conn, retry_id: int, retry_after_minutes: int, last_error: str):
    conn.execute(
        """
        UPDATE gemini_extraction_retries
        SET attempts = attempts + 1,
            last_error = ?,
            next_retry_at = datetime('now', ?),
            updated_at = datetime('now')
        WHERE id = ?
        """,
        (last_error, f"+{int(retry_after_minutes)} minutes", retry_id),
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


def list_favorite_companies(conn) -> list:
    return conn.execute(
        "SELECT ticker, company FROM favorite_companies ORDER BY company"
    ).fetchall()


def add_favorite_company(conn, ticker: str, company: str):
    conn.execute(
        """
        INSERT INTO favorite_companies (ticker, company)
        VALUES (?, ?)
        ON CONFLICT(ticker) DO UPDATE SET company = excluded.company
        """,
        (ticker, company),
    )


def remove_favorite_company(conn, ticker: str):
    conn.execute(
        "DELETE FROM favorite_companies WHERE ticker = ?",
        (ticker,),
    )
