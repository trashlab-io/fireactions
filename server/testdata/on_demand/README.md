# On-demand scan replay fixtures

Each file holds one workflow run and its jobs, trimmed to the fields the
on-demand scan reads. Both runs come from `trashlab-io/trashlab`. In both, a
`fire-1x2` job stayed `queued` while its run stayed `pending`, and no runner was
created.

- `run_41395664158_pending_queued_job.json`: captured from the GitHub API on
  2026-11-02 at about 06:53 UTC, while the job was still queued.
- `run_41115104748_pending_queued_job.json`: captured after a watchdog cancelled
  the run, then reset to the stuck state: run `status` set to `pending` and job
  `status` set to `queued`, with `conclusion` cleared. The job stayed queued for
  24 hours before the cancel. Only the job that was stuck is kept.
