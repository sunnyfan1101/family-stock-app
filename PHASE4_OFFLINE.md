# Phase 4 offline prototype — do not deploy

`phase4_offline.py` is not imported by `fetch_data.py`, the report runner, or
the cron entrypoints. It does not fetch, publish, or change the live DB.
It requires new output paths and checks shard hashes, SQLite integrity,
foreign keys after assembly, and row content against an optional source.

The current explicit groups are:

- `prices.db`: `daily_prices` (large, historical quotes)
- `market.db`: stock metadata, monthly revenue, financial reports, market
  statistics, latest snapshot, correlation cache
- `local.db`: `watchlist` and `user_presets` — **never publish this shard**

Unknown tables and unsupported views/triggers are rejected. `prices.db` has a
foreign key to `stocks` in `market.db`, so foreign keys can only be verified
after assembly. The manifest pins individual shard hashes, but it is not
signed and not yet a production version/rollback protocol. Existing app code
expects one SQLite DB; it cannot consume these shards directly.

For an offline copy of the DB only:

1. `python phase4_offline.py split /path/to/copy.db /path/to/new-shards`
2. `python phase4_offline.py assemble /path/to/new-shards /path/to/new-assembled.db`
3. `python phase4_offline.py verify /path/to/copy.db /path/to/new-assembled.db`

`refresh_weekly_changed(connection, {stock_id: first_changed_date})` is a
separate prototype. It reads each affected stock's full valid-close history,
uses the same point-in-time `weekly_averages` formula, but only writes rows
from the first changed date onward. A daily updater must accurately track
the earliest changed row for each stock; failure to include a corrected
historical date would leave later weekly values stale. It also must rebuild
`latest_stock_snapshot` after writing, as the production full updater does.

Observed on the 2026-10-22 DB snapshot (2,607,993 daily rows): split 79 s;
assemble 15 s; full nine-table content verification 45 s. `prices.db` was
365,584,384 bytes (xz -9: 91,197,652 bytes, 475 s); `market.db` was
8,986,624 bytes (xz -6: 2,215,336 bytes, 4.4 s). An isolated one-row
monthly-revenue change recompressed only the market shard in 4.5 s, left the
price shard SHA-256 unchanged, and reassembled with all price rows equal.

With 2,307 synthetic next-day quote rows and one older price correction,
full weekly recalculation took 81 s. Incremental recalculation plus snapshot
refresh took 31 s and wrote 2,314 weekly rows. All 2,610,300 daily rows had
identical weekly values; both DB copies passed integrity checks. Synthetic
quotes are test data only, not market observations.

This is a feasibility result, **not** a release approval. Before use in cron:
pin a signed/immutable version manifest, atomically publish and restore a
coherent set, test private-data exclusion and download/rebuild behavior,
handle the existing ~100 MiB GitHub limit as prices grow, and compare
incremental weekly values across sparse histories, bad/missing prices,
corporate-action backfills, week boundaries, and multiple corrected dates.
