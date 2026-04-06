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
