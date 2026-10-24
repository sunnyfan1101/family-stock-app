# Phase 3/4 offline hardening (not deployed)

This branch extends `phase3-staged-split` and `phase4-offline-prototype`.
Nothing here changes the active `main` app, cron, or published database.

## Phase 3: independent tasks

- `local_stock_updater.sh --financials-only` takes a SQLite backup of the
  live DB, runs a bounded FinMind quarterly batch on that copy, and uses the
  existing validation/promotion gate **only when actual rows changed**.
  `local_financials_updater.sh` is a wrapper, not an installed cron job.
  Batches default to 200 symbols (one API request per symbol), with an
  explicit `STOCK_FINANCIALS_OFFSET` for later batches. **No persistent
  cursor/automatic full-universe scheduler exists yet**; do not activate
  this runner unattended until the cursor and rate-limit budget are managed.
- Raw quarter rows go to `financial_reports`. Four consecutive valid quarters
  form TTM EPS; eight consecutive quarters permit EPS growth. The latest
  quarter supplies margins. PE uses the last locally stored valid close.
  Monthly `revenue_growth`, monthly revenue rows, and daily prices remain
  untouched. Source HTTP/API/parse errors abort the stage; empty valid data
  is a no-change outcome. A real FinMind 2330 response was parsed offline.
  Monetary amounts are stored as received; the existing `financial_reports`
  schema comment says "thousands" but the provider unit has not yet been
  independently reconciled. Do not publish those monetary amounts as
  normalized thousands until that check is complete.
  A disposable copy of the 2026-10-22 database was used for a single-stock
  2330 replay: FinMind supplied six quarterly periods, and the archive-only
  DB/xz pair passed the existing complete release validator (2,607,993 price
  rows, 2,307 valid closes on the latest day). The later summary/snapshot
  step on that same disposable DB produced 2,358 snapshot rows, preserved
  monthly growth, and passed SQLite integrity. That later-mutated DB was
  **not** paired with the previously validated xz or published; a full
  end-to-end run of the final combined code remains a release gate.
- `market_session.py` skips weekends and dates explicitly listed in
  `market_closures.json` with **both** TWSE and TPEX source references. The
  repository list starts empty. An unknown weekday is always fetched;
  a zero/partial quote feed remains a data-quality failure, not proof of a
  holiday. References are operator-supplied metadata, not automatically
  authenticated live notices. They must be checked before use.
- `schedule_policy.monthly_revenue_due` is a proposed 10th–15th day window
  regardless of weekday. The separate `local_revenue_updater.sh` entrypoint
  already exists, and its no-change/copy-only behavior is tested. **Cron has
  not been changed**; the schedule policy does not itself install a job.

## Phase 4: private/public boundary and installation rehearsal

- `phase4_bundle.py` exports only `prices.db`, `market.db`, and a manifest
  binding their exact hashes, sizes and table counts. It never exports
  `local.db`. The local shard must be supplied by the installer.
- The installer verifies both public files, reconstructs a single candidate
  SQLite DB (so the existing app can still read one DB), checks integrity and
  foreign keys, embeds the bundle ID inside the candidate, and switches the
  **single DB file** with `os.replace`. It retains a coherent
  `target.previous` backup, and failure injection after replacement restores
  the old target. A pre-existing backup or WAL sidecar blocks replacement.
- This is an offline prototype: no download/authenticity mechanism, signed
  manifest, active-writer lock integration, deployment entrypoint, or
  automated rollback policy exists. SHA-256 detects accidental corruption
  and version mixing but does **not** authenticate an untrusted source.
  Reassembly still copies the 2.6-million-row price table; small-shard
  compression gains do not imply equally fast app installation.
- Weekly regression now includes multi-day backfill, week boundary, missing
  prices and pre-existing-history insertion; each compares all rows to a
  full rebuild in disposable SQLite fixtures. The prior real-data benchmark
  was 31s incremental vs 81s full for one synthetic day plus one correction.

## Release gates still open

1. Phase 2 normal-trading-day cron success, remote commit, app loading and
   report success, with no duplicate/missing data.
2. Phase 3 source-backed quarterly run with controlled batches and a
   persistent cursor; independent monthly revenue cron dry-run and then
   carefully staged cutover. Official holiday references must be verified.
3. Phase 4 a real app/deployment integration, authenticated release source,
   lock-aware install/rollback, private-shard handling on every host, capacity
   plan for the ~91 MB price shard, and wider real-data backfill tests.

Do not merge this branch or switch cron merely because offline tests pass.
