# Rollback guide — TP/EPS quality upgrade (2026-07-17)

The quality upgrade shipped on branch `quality/tp-eps-tracking` in incremental
commits. There are three rollback levels; use the lightest one that solves the
problem.

## Level 1 — Runtime kill-switch (no code or data changes)

Disable the new ingestion behavior without touching git or the DB:

```
QUALITY_CHECKS_ENABLED=false
```

in `.env`, then restart the monitor. This turns off: outlier confirmation
passes, TP-vs-price sanity checks, EPS/net-profit cross-checks, and the
report-date drift guard. Rows already flagged `suspect` stay flagged (clear
them manually if desired, see Level 3).

To disable only the retry model escalation, set `GEMINI_ESCALATION_MODEL=`
(empty).

## Level 2 — Code rollback (git)

Every logical change is its own commit on `quality/tp-eps-tracking`:

```
git log --oneline main..quality/tp-eps-tracking
```

- Revert one piece: `git revert <commit>`
- Abandon everything: `git checkout codex/kosdaq150-feature` (the branch this
  work was based on)

The schema migrations are **additive only** (new columns/tables, nothing
dropped or rewritten), so the old code runs fine against the migrated DB.

## Level 3 — Database rollback

A full pre-migration backup was taken before any change:

```
kospi_eps.db.backup-2026-07-17   (328 MB)
```

To restore it (stop the monitor and dashboard first):

```bash
cp kospi_eps.db.backup-2026-07-17 kospi_eps.db
rm -f kospi_eps.db-wal kospi_eps.db-shm
```

Partial cleanups instead of a full restore:

```sql
-- Clear all suspect flags (keeps the rows, re-enables them everywhere)
UPDATE eps_estimates SET suspect = 0, suspect_reason = NULL;
UPDATE analyst_reports SET tp_suspect = 0, tp_suspect_reason = NULL;

-- Undo broker canonicalization (raw name was preserved)
UPDATE analyst_reports SET broker = broker_raw WHERE broker_raw IS NOT NULL;
-- (eps_estimates rows were canonicalized in place; restore the backup if you
--  need the original strings there)
```

## What was applied to the live DB

1. Additive schema migration (`init_db()`): new columns on `eps_estimates`
   (`suspect`, `suspect_reason`, `revenue`, `operating_profit`, `net_profit`,
   `recommendation_norm`) and `analyst_reports` (`target_price`, `tp_suspect`,
   `tp_suspect_reason`, `recommendation`, `recommendation_norm`, `broker_raw`),
   plus the `tp_events` table.
2. `scripts/backfill_quality_fields.py --apply`: 356 broker rows canonicalized
   (raw names preserved in `broker_raw`), 15,316 recommendations normalized,
   6,311 reports given report-level TP.
