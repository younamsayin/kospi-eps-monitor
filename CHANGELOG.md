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
