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
- Downloads PDFs and extracts EPS estimates using Gemini
- Detects EPS upgrades/downgrades per broker per fiscal year
- Detects target price raises/cuts per broker
- Streamlit dashboard with per-broker estimates, consensus, index-level aggregates, recent reports, and revision history
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
SCRAPE_PAGES=10                                          # pages to scrape per source (10 ≈ 1 week)
CHECK_INTERVAL_MINUTES=1440
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
│   └── naver.py        # scrapes analyst reports + downloads PDFs
├── extractor/
│   └── gemini.py       # sends PDF to Gemini, returns structured JSON
├── db/
│   └── models.py       # SQLite schema and query helpers
├── alerts/
│   └── telegram.py     # Telegram bot notifications
├── requirements.txt
└── .env.example
```

## Dashboard

| Tab | What it shows |
|-----|--------------|
| EPS Estimates | Bar chart + table of latest FWD EPS by company, broker, and fiscal year |
| Consensus | Average latest EPS / target price across brokers |
| Index Aggregate | Daily sum of consensus EPS across tracked companies |
| Recent Reports | All collected reports with direct PDF links |
| Revisions | EPS and target price changes, with per-company EPS trend chart |

## Notes

- KOSPI 200 list is sourced from Naver Finance (updated quarterly by KRX)
- Analyst reports are scraped from Naver Finance research (`finance.naver.com/research`) and Bondweb research center
- EPS revision detection is per broker — consensus-level revision tracking requires multiple brokers covering the same stock
- The `.db` file is portable; back it up to preserve history
