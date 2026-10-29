# Phase 4 Release transport — staged, default off

The App still uses the legacy single `stock_data.db.xz` by default. Setting
`STOCK_BUNDLE_RELEASE_TAG` **and** `STOCK_BUNDLE_ID` opts in to a public GitHub
Release containing exactly `bundle.json`, `prices.db.xz`, and `market.db.xz`.
`STOCK_BUNDLE_DIR` is an alternative local test source; it must not be set
with `STOCK_BUNDLE_RELEASE_TAG`.

Trust boundary: the tag locates files; it does **not** authenticate them.
Record the `BUNDLE_ID` from a validated *local build* in separately reviewed
deployment configuration. Never read the ID from the downloaded manifest and
then use it as its own expected value. The App limits download sizes, checks
the joint ID plus compressed/raw SHA-256 and table counts, checks disk headroom,
preserves local `watchlist`/`user_presets`, assembles a candidate DB, checks
SQLite/foreign keys, and switches one DB file under a lock. Missing, oversized,
corrupt or mismatched release assets fail closed. No credentials are embedded
in the public URL or bundle.

## Release canary checklist

1. Freeze a known-good `main` commit and run `stock_release.py validate` on the
   matching local DB/xz. Do not build a bundle from a DB being updated.
2. Run `phase4_bundle.py prepare` into a new staging directory. Confirm the
   three public files only, each hash and the output `BUNDLE_ID`; verify the
   source DB has no unsupported table. Do not upload the private shard or
   temporary snapshot.
3. Upload the three files as a *draft* GitHub Release tied to the frozen
   commit, then verify remote bytes independently. Only then publish the
   release. Keep the existing single-DB xz and prior tag for rollback.
4. On a disposable App deployment, configure an explicit release tag and
   the independently recorded ID. Check available disk, install, stock/price
   row counts, SQLite and foreign keys, private tables, startup time and
   concurrent sessions. Inject a wrong ID/corrupt asset and check that the
   old DB remains readable.
5. Test rollback to the prior single-DB App version and data, then enable
   production Cloud only after the canary passes. Do not delete backups until
   the new version has run through a normal update and a restart.

`STOCK_BUNDLE_ID` pins bytes but does not prove the operator chose the correct
market date. Review the release against the source commit/date and record the
two hashes before deploying. The App canary must be run through the protected
Streamlit Cloud instance; local success is not a substitute. Storage on that
instance and persistent private-data semantics remain operator checks.

The current price archive is near 100 MiB. This staging implementation keeps
the existing 100 MiB fail-closed limit; before it grows past that bound, move
to an explicitly tested partition scheme or a reviewed larger Release-asset
bound. Do not silently relax the limit during a live cutover.

## 2026-10-29 canary evidence

- The frozen source was remote `main` commit `2e3b3cecf04022fc3fc68ee30e87fdc9d63f52cc`.
  Its local DB and xz passed the existing release validator before packaging;
  source DB SHA-256 was `3e99648cf4f76aac2c3b23480d1807773470f4abaa6bb56b7951b23f7061a5c8`.
- The public GitHub Release tag is `phase4-canary-20261029-2e3b3ce`.
  Bundle ID: `becb47083799e4764c6242dc987cd24f94105cdd0e68908e0e16f852fe61b585`.
  It contains only the 949-byte manifest, 91,143,836-byte price archive and
  2,202,500-byte market archive. The private shard was not uploaded.
- A draft upload was downloaded back through GitHub CLI; each of the three
  SHA-256 values matched the local files. After publication the new downloader
  retrieved all three files anonymously from the public Release URL and
  verified the separately pinned bundle ID and contents.
- On a disposable App DB, the actual opt-in `database.ensure_database` path
  fetched the public Release, installed 2,607,488 price rows, retained a
  synthetic private watchlist entry, and passed SQLite integrity. A separate
  full-size local install matched all nine allowed source tables row-for-row.
  Seventy-two relevant stock/partition tests passed.

This proves release transport and local App behavior only. It does **not**
prove the protected Streamlit Cloud instance has enough disk, can fetch the
Release in its environment, keeps private tables across redeploys, handles
live concurrent sessions, or can execute a Cloud rollback. No Cloud setting,
production App code path, or stock-update cron was changed for this canary.
Also, this canary is a single snapshot: automatic publication and pinning for
future daily quote updates remain to be integrated and verified before a
production cutover.
