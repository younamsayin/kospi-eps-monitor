# Changelog

## [0.2.0] - 2026-04-06

### Changed
- Replaced Slack alerts with Telegram bot notifications (`TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`)
- Removed the legacy unused Slack alert module and dependency
- Fixed dashboard install/docs drift and aligned README with current Telegram-based behavior
- Fixed consensus aggregate calculations to use broker-level consensus instead of single latest estimates
- Fixed target price revision tracking to compare one report against the previous report from the same brokerage
- Added responsive dashboard layout styling so the UI adapts better to window size
- Removed the old `EPS Estimates` tab and updated the `Consensus` tab to show week-over-week change
- Added previous/current report dates to revision displays and Telegram revision alerts
- Fixed revision ordering to compare older report dates to newer report dates consistently
- Hardened Gemini extraction parsing so unexpected JSON list responses are normalized instead of crashing the monitor
- Fixed the company-filtered consensus dashboard query to bind the selected ticker correctly
- Updated report ingestion to prefer the publication date extracted from PDFs and sort recent reports newest-to-oldest by report date
- Added a `Source` column to recent reports, persisted report source metadata, and backfilled existing rows from stored URLs
- Improved Bondweb filtering by fixing row parsing, stripping leading bylines, and using safer normalized company matching
- Improved Naver report downloading by resolving PDF links from report detail pages and trying external broker page fallbacks when direct links are unavailable
- Added Bondweb company-search mode with per-company result caps, deduping, and console progress logging
- Updated the Index Aggregate chart to plot weekly average aggregate EPS levels instead of daily totals or percentage changes
- Hardened Bondweb PDF downloads so broker-side HTTP errors are logged and skipped instead of crashing the monitor
- Redesigned the dashboard so `Consensus` handles both single-company and `All` views, with weekly EPS trends and a latest broker report table
- Added monitor-side fiscal year sanity checks to correct one-year-shifted EPS series before saving and ignore pre-report-year rows
- Fixed Telegram EPS/TP alert date comparisons so duplicate same-day ingestions do not produce `same date → same date` messages
- Added persistent favorite companies in the dashboard sidebar with a star toggle and one-click shortcuts
- Tightened Bondweb company matching so shorter company names no longer match inside brokerage names like `현대차증권`
- Added PDF SHA-256 deduplication so identical reports are skipped even when they arrive under different URLs
- Fixed the `pdf_hash` schema migration path so older SQLite databases can add the new column and index safely
- Hardened Gemini extraction cleanup and payload validation, including temp/upload cleanup and rejecting ambiguous multi-company payloads
- Added scraper retries, configurable user agents, and parse-error logging for Naver, Bondweb, and KRX fetches
- Filtered consensus views to recent estimates, added cached dashboard queries, and exposed company-count context in the aggregate weekly chart
- Added a small pytest-based helper test suite and ignored the local `reports/` archive directory in git
- Reduced monitor terminal noise by suppressing verbose library request logs and batching repeated skip progress lines
- Fixed the filtered `Consensus` tab to use the correct SQL parameter order for selected-company views
- Added a reusable historical fiscal-year cleanup script for repairing high-confidence shifted EPS series in the existing DB
- Fixed the `Consensus` and per-company `Revisions` charts to keep a fixed one-year X-axis window
- Changed report archiving to happen immediately after successful download, even if Gemini extraction later fails
- Reorganized archived PDFs from `date/source/file` to `company/source/date/file` and added scripts to backfill missing archives and migrate existing files
- Added Bondweb failure diagnostics with exact `report_id` / `gn` logging, broker-level failure summaries, and fast-fail handling for deterministic ASP runtime-error records
- Switched Naver scraping to default to per-company `itemCode` search for monitored tickers while capping the search to the most recent 10 reports from a single page per company
- Added visible Naver per-ticker progress logging during company-search scraping
- Added Gemini retry queue handling for archived PDFs with retryable extraction failures, while permanently skipping unreadable and multi-company reports
- Added archive reprocessing support for both Naver and Bondweb PDFs, including alphanumeric ticker filename parsing
- Added same-day duplicate report cleanup tooling that keeps the most complete ticker/broker/date report
- Hardened SQLite WAL handling with aligned dashboard connection settings, `synchronous=NORMAL`, busy timeouts, periodic checkpoints, and DB retry recovery
- Changed Telegram alert delivery so EPS and target price alerts are sent only after the report and estimates commit successfully
- Added all-report lifecycle logging and full existing-report skip logging to make terminal progress easier to audit
- Increased Bondweb per-company collection from 5 to 10 reports and expanded title matching for no-space hyphen and bracketed ticker formats
- Added ingestion event auditing plus `scripts/audit_ingestion.py` to inspect empty EPS rows, Gemini retries, recent failures/skips, and duplicate PDF hashes
- Added Bondweb subject validation so Gemini-extracted ticker mismatches are skipped instead of being saved under the searched company

## [0.1.0] - 2026-04-06

Initial release.

### Added
- KOSPI 200 constituent list scraper via Naver Finance
- Analyst report scraper from Naver Finance research (direct PDF links)
- PDF → Gemini 2.5 Flash extraction pipeline for structured EPS data
- SQLite database with schema for reports, estimates, and constituent list
- EPS upgrade/downgrade detection per broker per fiscal year
- Slack webhook alerts for EPS upgrades above configurable threshold
- Streamlit dashboard with:
  - Top-level metrics (total reports, companies covered, reports today, upgrades detected)
  - FWD EPS bar chart by company for any fiscal year
  - EPS estimates table with target price, recommendation, broker, and report date
  - Recent reports tab with direct PDF links
  - EPS revisions tab showing upgrades and downgrades
  - Per-company EPS trend chart over time
- Scheduled monitoring loop via APScheduler (default: every 60 minutes)
- Company and fiscal year filters in dashboard sidebar
