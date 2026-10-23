# Stock update release gate

`local_stock_updater.sh` is the cron entrypoint. It holds the shared DB lock,
creates a new `.stock-staging/run-*` directory, rebuilds a copy of the live DB,
runs the fetcher against that copy, then validates the entire compressed file
against the candidate SQLite DB. It promotes only after validation. The
report runner uses the same lock and never decompresses over the live DB.
Report delivery returns nonzero on webhook rejection; a multi-part report may
have sent earlier parts before failure, so inspect the channel before retrying.

Validation rejects a missing/corrupt/mismatched xz, failed SQLite integrity,
historical return or latest-snapshot metric disagreement, regressed latest
date/coverage, and files over GitHub's 100 MiB single-file limit. Normal daily
updates additionally require today's prices. Financial-only runs do not.

The release directory retains `previous.db` and `previous.db.xz`, a coherent
rollback pair. The live DB and xz are replaced under the updater lock. If the
second rename fails, the publisher restores both. The two-file promotion is
not a single atomic operation for unrelated readers outside the lock.

After local promotion, GitHub push can still fail independently. A failed push
has a nonzero exit and leaves the validated local release and backup intact;
do not assume Streamlit is updated until remote `main` and the app are checked.
The standalone GitHub push entrypoint also revalidates the live DB/xz pair, so
manual invocation cannot publish a truncated or mismatched archive.
Never run the old delete-then-decompress workflow or force-push to recover.

Known limits: close-to-close returns are not TWSE/TPEX official adjusted
reference-price returns; 17 historical rows had missing close in the source
snapshot and were left missing. Trading holidays currently fail the daily
`--require-today` gate rather than being recognized from a market calendar.
