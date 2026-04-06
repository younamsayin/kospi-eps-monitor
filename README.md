# KOSPI EPS Monitor

Monitors KOSPI 200 companies for new analyst reports, extracts forward EPS and target prices from PDFs, tracks revisions over time, and sends Telegram alerts when a broker changes estimates beyond configurable thresholds.

## How it works

```
Naver Finance / Bondweb → new analyst reports (PDF)
    → Gemini API (native PDF understanding)
    → structured EPS data {company, ticker, fiscal_year, fwd_eps, target_price}
    → compare vs previous broker estimate in SQLite
    → Telegram alert if EPS or target price changes > threshold
```

## Features

- Automatically fetches the KOSPI 200 constituent list
- Scrapes Naver Finance and Bondweb research for new analyst reports
- Uses Naver company-specific `itemCode` searches by default for monitored tickers, capped to recent results per company
- Downloads PDFs, archives them locally, and extracts EPS estimates using Gemini
- Detects EPS upgrades/downgrades per broker per fiscal year
- Detects target price raises/cuts per broker
- Streamlit dashboard with consensus views, recent reports, revision history, and persistent favorite companies
- Telegram alerts on EPS and target price changes

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

## Project structure

```
kospi-eps-monitor/
├── monitor.py          # main loop: scrape → extract → detect → alert
├── dashboard.py        # Streamlit dashboard
├── scraper/
│   ├── krx.py          # fetches KOSPI 200 constituent list (Naver Finance)
│   ├── naver.py        # scrapes analyst reports + downloads PDFs
│   └── bondweb.py      # scrapes Bondweb analyst reports + downloads PDFs
├── extractor/
│   └── gemini.py       # sends PDF to Gemini, returns structured JSON
├── db/
│   └── models.py       # SQLite schema and query helpers
├── alerts/
│   └── telegram.py     # Telegram bot notifications
├── scripts/
│   ├── backfill_report_archives.py      # backfill local PDF archive files for existing DB rows
│   ├── cleanup_shifted_fiscal_years.py  # repair high-confidence shifted fiscal-year EPS rows
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

- KOSPI 200 list is sourced from Naver Finance (updated quarterly by KRX)
- Analyst reports are scraped from Naver Finance research (`finance.naver.com/research`) and Bondweb research center
- Archived PDFs are stored locally under `reports/company/source/date/file.pdf`
- Existing DB rows can be backfilled into the archive with `python3 scripts/backfill_report_archives.py`
- EPS revision detection is per broker — consensus-level revision tracking requires multiple brokers covering the same stock
- The `.db` file is portable; back it up to preserve history
