# KOSPI EPS Monitor

Monitors KOSPI 200 companies for new analyst reports, with optional KOSDAQ 150 expansion via feature flag, extracts forward EPS and target prices from PDFs, tracks revisions over time, and sends Telegram alerts when a broker changes estimates beyond configurable thresholds.

## How it works

```
Naver Finance / Bondweb → new analyst reports (PDF)
    → ingestion audit event
    → Gemini API (native PDF understanding)
    → structured EPS data {company, ticker, fiscal_year, fwd_eps, target_price}
    → validate source subject where needed
    → compare vs previous broker estimate in SQLite
    → commit report + estimates
    → Telegram alert if EPS or target price changes > threshold
```

## Features

- Automatically fetches the KOSPI 200 constituent list
- Optionally expands the monitored universe to include KOSDAQ 150 via `ENABLE_KOSDAQ150=true`
- Scrapes Naver Finance and Bondweb research for new analyst reports
- Uses Naver company-specific `itemCode` searches by default for monitored tickers, capped to recent results per company
- Downloads PDFs, archives them locally, and extracts EPS estimates using Gemini
- Persists ingestion events so skipped, failed, duplicate, and saved candidates are auditable after a run
- Stores raw Gemini extraction payloads in SQLite for future audits and DB rebuilds
- Validates Bondweb candidate reports against Gemini-extracted subject tickers before saving
- Detects EPS upgrades/downgrades per broker per fiscal year
- Detects target price raises/cuts per broker
- Streamlit dashboard with consensus views, recent reports, revision history, and persistent favorite companies
- Telegram alerts on EPS and target price changes, sent only after DB commit succeeds

## Setup

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Configure environment**
```bash
cp .env.example .env
```

Edit `.env`:
```
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-3.1-flash-lite-preview               # optional, this is the default
DB_PATH=kospi_eps.db
ENABLE_KOSDAQ150=false                                # set true to monitor KOSPI 200 + KOSDAQ 150
TELEGRAM_BOT_TOKEN=your_bot_token_here                   # from @BotFather
TELEGRAM_CHAT_ID=your_chat_id_here                       # channel or user ID
EPS_UPGRADE_THRESHOLD=0.02                               # 2% change triggers alert
TP_CHANGE_THRESHOLD=0.02                                 # 2% target price change triggers alert
NAVER_SCRAPE_PAGES=1                                     # company-search pages per ticker (default path; effectively capped to 1)
BONDWEB_SCRAPE_PAGES=10                                  # pages to scrape from Bondweb company-search/list mode
SCRAPE_PAGES=10                                          # fallback shared default if source-specific values are omitted
CHECK_INTERVAL_MINUTES=1440
REPORTS_DIR=reports                                      # local PDF archive root
```

Get a Gemini API key at [Google AI Studio](https://aistudio.google.com/).

**3. Initialize the database**
```bash
python3 monitor.py --once
```

## Usage

**Run monitor once (useful for testing)**
```bash
python3 monitor.py --once
```

**Run on a schedule (checks every 60 minutes)**
```bash
python3 monitor.py
```

**Launch dashboard**
```bash
python3 -m streamlit run dashboard.py
```
Then open [http://localhost:8501](http://localhost:8501).

**Audit ingestion health**
```bash
python3 scripts/audit_ingestion.py --limit 25
```

This summarizes reports with no EPS rows, queued Gemini retries, cached Gemini extraction payloads, recent ingestion failures/skips, and duplicate PDF hashes.

## Project structure

```
kospi-eps-monitor/
├── monitor.py          # main loop: scrape → extract → detect → alert
├── dashboard.py        # Streamlit dashboard
├── scraper/
│   ├── krx.py          # fetches KOSPI 200 / KOSDAQ 150 constituent lists (Naver Finance)
│   ├── naver.py        # scrapes analyst reports + downloads PDFs
│   └── bondweb.py      # scrapes Bondweb analyst reports + downloads PDFs
├── extractor/
│   └── gemini.py       # sends PDF to Gemini, returns structured JSON
├── db/
│   └── models.py       # SQLite schema and query helpers
├── alerts/
│   └── telegram.py     # Telegram bot notifications
├── scripts/
│   ├── audit_ingestion.py               # inspect ingestion failures, retry queue, empty EPS rows, and duplicate hashes
│   ├── backfill_report_archives.py      # backfill local PDF archive files for existing DB rows
│   ├── cleanup_shifted_fiscal_years.py  # repair high-confidence shifted fiscal-year EPS rows
│   ├── deduplicate_same_day_reports.py  # remove same ticker/broker/date duplicates while keeping the most complete row
│   ├── reprocess_archived_reports.py    # rerun archived PDFs through Gemini for Naver and Bondweb
│   └── reorganize_report_archives.py    # migrate archive folders to company/source/date layout
├── reports/             # local archived PDFs (company/source/date/file.pdf)
├── requirements.txt
└── .env.example
```

## Dashboard

| Tab | What it shows |
|-----|--------------|
| Consensus | Company-level or all-company consensus, weekly 1-year trend chart, and latest broker report table |
| Recent Reports | All collected reports with direct PDF links |
| Revisions | EPS and target price changes, with per-company EPS trend chart |

## Notes

- KOSPI 200 list is sourced from Naver Finance
- Optional KOSDAQ 150 expansion is sourced from the PLUS 코스닥150 ETF holdings export, with non-equity rows filtered out
- Analyst reports are scraped from Naver Finance research (`finance.naver.com/research`) and Bondweb research center
- Archived PDFs are stored locally under `reports/company/source/date/file.pdf`
- Existing DB rows can be backfilled into the archive with `python3 scripts/backfill_report_archives.py`
- Archived PDFs that never made it into the DB can be reprocessed with `python3 scripts/reprocess_archived_reports.py`
- Ingestion health can be checked with `python3 scripts/audit_ingestion.py --limit 25`
- Raw Gemini responses are stored in the SQLite `gemini_extractions` table, linked by report metadata and `pdf_hash`
- EPS revision detection is per broker — consensus-level revision tracking requires multiple brokers covering the same stock
- The `.db` file is portable; back it up to preserve history
