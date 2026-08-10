# Built-in statistics service

The former `daily_stats_service` is available inside ScrapydWeb at `/stats/`.
It uses the same bind, port and HTTP basic authentication as ScrapydWeb. A
separate service on port 5001 is no longer required.

## Database compatibility

ScrapydWeb 1.4.0 and 1.6.0 use the same columns consumed by this subsystem in
`task`, `task_result`, `task_job_result`, and the per-server jobs tables. The
1.6.0 setting file is named `scrapydweb_settings_v11.py`, and `DATA_PATH` may
relocate all SQLite files. The integration therefore obtains paths from
ScrapydWeb 1.6.0 itself and never imports a version-named settings file.

At startup, the sync worker checks the required tables and columns read-only.
If they are incompatible, page serving remains available but synchronization
is disabled and the reason is logged. No automatic schema alteration is made.

The current statistics implementation supports ScrapydWeb's SQLite backend.
Set `ENABLE_DAILY_STATS = False` when using MySQL or PostgreSQL.

## Existing production data

Before upgrading, stop the old app and sync processes and back up the database
directory. Keep these files together under the 1.6.0 `DATA_PATH/database`:

- `timer_tasks.db`
- `jobs.db`
- `apscheduler.db`
- `daily_stats.db`

Do not run the old `sync.py` together with the built-in worker. Both would
write `daily_stats.db`.

## Synchronization architecture

The standalone `sync.py` has been replaced by three internal modules:

- `events.py` owns the in-process queue and event dispatch.
- `handlers.py` performs incremental updates for committed task and job events.
- `reconcile.py` performs bounded startup backfill, retry, and missed-event repair.

ScrapydWeb emits events only after its source transaction commits. A
`task_job_created` event registers a scheduled job, `task_result_updated`
updates its aggregate outcome, and the Jobs snapshot emits `job_finished` on
the transition to the finished state. The latter updates both manual jobs and
timer-child jobs and performs the final stats lookup. Reconciliation remains
necessary because Scrapyd has no push notification for remote state changes
and because events can be lost during process downtime.

Mail credentials are no longer embedded in source. Configure
`SPIDER_MONITOR_MAIL_APPKEY`, `SPIDER_MONITOR_MAIL_APP_SECRET`, and related
values as environment variables where that integration is required.
