# Changelog

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
