# Phase 3/4 staged integration (not deployed)

This branch is **not** a cron cutover, a new GitHub `main` release, or a
Streamlit deployment. The default App path remains the single `stock_data.db.xz`.

## Quarterly batches and dry-run schedule

- `local_stock_updater.sh --financials-only` uses a persistent, ignored
  `.financial-batch-state.json` cursor. An unchanged batch advances it;
  a changed batch advances it only after the local candidate is promoted and
  the archive push returns success. A failed push leaves a pinned pending
  archive; the next run (including a quote or revenue run) retries that exact
  archive **before another fetch**. A mismatched archive blocks all modes.
- `.financial-daily-budget.json` reserves up to 150 requests per Taipei day
  before any quarterly HTTP calls. A crashed/failed batch consumes the
  reservation. This cap covers **this runner only**; it does not enforce a
  global FinMind quota across other scripts. Neither file belongs in Git.
- Batch offsets wrap to zero if the stock universe shrinks. A pending archive
  that differs from the live archive blocks unattended continuation for manual
  review; it is never silently discarded. State and archives must be backed
  up together before a future cutover.
- `schedule_policy.proposed_jobs` is an offline calendar proposal: weekday
  quote attempts, monthly revenue on days 10–15 regardless of trading, and
  at most one quarterly batch per day in the 1st–15th of March/May/August/
  November. It is **not installed in cron**. Weekday does not prove an open
  exchange; `market_session.py` plus actual quote coverage remain gates.
- This 150/day proposal must be checked against the account's actual FinMind
  allocation and other callers before enabling it. Empty valid source data
  is distinct from API failure. Provider units for monetary quarterly fields
  still need independent confirmation before presentation as normalized units.

## Opt-in public-shard App path

- `phase4_bundle.py prepare SOURCE_DB OUTPUT_DIR` takes one SQLite snapshot,
  splits an explicit table allowlist, emits **only** `prices.db.xz`,
  `market.db.xz`, and `bundle.json`, and atomically renames the validated
  public output directory into place. The local `watchlist` and
  `user_presets` tables remain in the private temporary shard and are never
  packaged. Do not publish the temporary workspace or a full raw DB.
- Each public archive is below the GitHub 100 MiB individual-file limit or
  packaging fails. The manifest pins compressed bytes, decompressed bytes,
  table counts, and a joint release ID. The App's `database.ensure_database`
  uses this path **only if both** `STOCK_BUNDLE_DIR` and `STOCK_BUNDLE_ID` are
  set. The expected ID must come from a separately trusted release process,
  not be copied from an untrusted bundle being validated. SHA-256 prevents
  corruption/mixing; it is not a signature or provenance check.
- The App extracts private tables from its existing local DB (or creates
  empty private tables on first install), assembles one candidate DB,
  verifies integrity and foreign keys, then atomically switches the one DB
  file under an App-side lock. A WAL sidecar blocks switching. Failed swaps
  restore the previous DB; successful swaps retain versioned rollback DBs.
  These backups grow until an operator reviews/archives them; do not enable
  unattended high-frequency replacement without a retention plan.
- No network download or automatic GitHub publishing is enabled. A deployment
  must distribute the two archives and manifest as a trusted, version-paired
  release, pin the release ID separately, make room for snapshot/reassembly/
  backup disk usage, and perform a live Streamlit canary/rollback test.
  The format does **not** yet remove the expensive 2.6-million-row App-side
  reassembly. Existing App code still reads a single reconstructed DB.

## Tests and remaining gates

- Unit/fault tests cover cursor retries and cap, weekend revenue scheduling,
  compressed shard corruption/mismatch, two consecutive installs, private
  data preservation, and existing incremental-weekly boundaries.
- The full public-bundle build and opt-in App load are exercised on a
  read-only production-DB snapshot **only in a disposable local directory**.
  Nothing in this branch is automatically published or switched on.
- On the 2026-10-22 market DB snapshot, the staged full build produced an
  approximately 87 MiB price archive and 2.2 MiB market archive. An opt-in
  loader canary reconstructed 2,607,993 price rows and 2,358 snapshot rows;
  `PRAGMA integrity_check` was `ok`. The source DB/xz SHA-256 values remained
  identical before and after this rehearsal. This tests the local App data
  path, **not** Streamlit Cloud deployment, release provenance, live user
  sessions, or a normal-day cron run.
- Phase 2 still needs a normal-trading-day automatic quote→validation→push→
  report success. Phase 3 then needs a staged cron canary, source quota and
  financial-unit confirmation, and operator-verified official closure list.
  Phase 4 needs trusted artifact distribution, full App/Cloud deployment and
  rollback validation, disk/backup retention, and a capacity plan for the
  growing price archive. Do not merge/enable all phases in one cutover.
