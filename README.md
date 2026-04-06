# KOSPI EPS Monitor

Monitors KOSPI 200 companies for new analyst reports and tracks Forward EPS estimate revisions. When a broker raises their EPS estimate by more than a configurable threshold, an alert is sent to Slack.

## How it works

```
Naver Finance → new analyst reports (PDF)
    → Gemini API (native PDF understanding)
    → structured EPS data {company, fiscal_year, fwd_eps, target_price}
    → compare vs previous estimate in SQLite
    → Slack alert if upgrade > threshold
```

## Features

- Automatically fetches the KOSPI 200 constituent list
- Scrapes Naver Finance research for new analyst reports daily
- Downloads PDFs and extracts EPS estimates using Gemini 2.5 Flash
- Detects EPS upgrades/downgrades per broker per fiscal year
- Streamlit dashboard with charts, report links, and revision history
- Telegram alerts on upgrades

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
| EPS Estimates | Bar chart + table of latest FWD EPS by company and fiscal year |
| Recent Reports | All collected reports with direct PDF links |
| EPS Revisions | Upgrades and downgrades, with per-company EPS trend chart |

## Notes

- KOSPI 200 list is sourced from Naver Finance (updated quarterly by KRX)
- Analyst reports are scraped from Naver Finance research (`finance.naver.com/research`)
- EPS revision detection is per broker — consensus-level revision tracking requires multiple brokers covering the same stock
- The `.db` file is portable; back it up to preserve history
