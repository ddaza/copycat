# copycat

Python 3.10+ file-content recovery for macOS/Linux, managed with **uv**. Copy in batches,
exit completely, turn off the computer, and continue another day.

- **`staging_dir`** holds incomplete/in-progress files. Leave these alone so they can resume.
- **`destination`** holds completed files only. Offload everything here without checking a list.

Files are verified in staging before being published to destination. Saved successes are
not copied from the old drive again after you offload them.

## Configure and run

From the repository directory, create your private config:

```bash
cp config.example.json config.json
```

Edit **`config.json`** and set `source` to the folder to recover. The example uses
generic local output folders; adjust them as needed:

```json
{
  "source": null,
  "destination": "./recovered",
  "staging_dir": "./staging",
  "state_dir": "./state",
  "checkpoint_mib": 16,
  "timeout_seconds": 60,
  "retries": 1,
  "verify": true,
  "retry_failed": false,
  "check_completed": false,
  "batch_gb": 100,
  "min_free_gb": 5,
  "report_interval_seconds": 30
}
```

`source: null` deliberately prevents running until you configure a source.
`config.json` is ignored by Git; do not commit private paths or drive names.

Then start it:

```bash
cd copycat
uv run copycat.py
```

`uv run` installs the locked dependencies automatically (`filelock` handles job locking).
Nothing runs automatically in the background. Check the paths before running.
Destination and staging are created; their parents must already exist.
Source, destination, staging, and state must not overlap or contain one another.
Destination and staging path components cannot be symlinks.

**Staging and destination must be on the same filesystem**, supporting hard links.
Publication uses an atomic, no-overwrite hard link followed by removal of the staging
entry after the success is durably saved. This does not duplicate file contents or
require space for a second copy. Offload files only after the run exits.

| Setting                   | Meaning                                                                                                              |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `source`                  | Folder on the old drive. Opened read-only.                                                                           |
| `destination`             | Completed files on a healthy disk, ready to offload. Keep this path the same across runs.                            |
| `staging_dir`             | Incomplete files. Default if omitted/null: a sibling named `<destination-name>.staging`. Keep unchanged across runs. |
| `state_dir`               | Permanent checkpoints and reports. **Never clear or move this to free disk space.**                                  |
| `checkpoint_mib`          | Durable checkpoint interval in MiB; 16 MiB by default.                                                               |
| `timeout_seconds`         | Worker no-progress timeout, not a total file-duration limit.                                                         |
| `retries`                 | Extra attempts after the first; `1` means at most two attempts per file per run.                                     |
| `verify`                  | SHA-256 readback in staging before publication; leave enabled.                                                       |
| `retry_failed`            | Revisit failed/conflicting files; otherwise leave them for manual review.                                            |
| `check_completed`         | Normally **false**: trust saved successes even after destination files are moved.                                    |
| `batch_gb`                | Maximum newly checkpointed bytes per invocation, in decimal GB. `0` = unlimited.                                     |
| `min_free_gb`             | Stop before writes consume this free-space reserve on the shared staging/destination disk.                           |
| `report_interval_seconds` | Periodic JSON snapshot interval; reports are also refreshed at startup and exit.                                     |

`~` is supported. Relative paths **in the config** are relative to that config file,
not to the terminal's working directory. Unknown keys and invalid settings are rejected.

## Exit tonight; resume tomorrow

1. Press **Ctrl+C once** and wait for the summary and your shell prompt.
2. The process has now **exited**; there is no paused/background program to keep alive.
3. Eject the old drive normally. Close terminal tabs and shut down the computer normally.
4. Next time, reconnect the same drives at the same paths and run `uv run copycat.py` again.

No progress depends on keeping the terminal or computer running. SQLite checkpoints are
committed to disk throughout the copy. Only bytes after the last durable checkpoint may
need to be recopied. The retained destination prefix is checked against its saved hash
before resuming an incomplete file.

Terminal-close (`SIGHUP`) and normal shutdown (`SIGTERM`) signals also request an exit.
**Ctrl+C and waiting is preferred**: an OS shutdown may allow little cleanup time. A forced
kill/power failure can leave JSON snapshots stale; the next run rebuilds them from the
committed database. Hardware/OS caching and a damaged disk still prevent absolute
power-loss guarantees.

## Copy 100 GB, move it away, repeat

With the supplied config, each invocation stops at about **100 GB of newly written data**,
or sooner if the staging disk reaches the **5 GB free-space reserve**. These are per-run
limits, not a guarantee that 100 GB will fit. Already-existing partial data is not counted
again, and other applications can consume disk space concurrently.

1. Run the program and let it finish a batch, or exit with Ctrl+C.
2. Everything in **`destination`** is completed; no success-list review is needed.
3. Move the contents of `destination` to your other healthy drive.
   Preserve their relative folder layout to avoid mixing up equal filenames.
4. Verify that the copies on the other drive are readable/correct **before deleting local
   copies**. Copycat does not move files or verify your manual onward transfer.
5. Leave **`staging_dir`** and the entire state directory untouched.
6. Run the same command again. Saved successes are skipped **even if the destination files
   are now gone**. The next batch copies unfinished/new files.

A batch can stop **inside a large file**. That file is `partial`, not a success; leave it
in place for the next run. The staging disk must eventually have space for the largest
individual file. If a partial fills your available capacity, finish it on a sufficiently
large healthy disk rather than deleting it or repeatedly restarting with no added space.

Keep `check_completed: false` for this workflow. Turning it on explicitly checks destination
identity/size/mtime and reports missing or changed completed files as conflicts. It does
not verify where you archived them and is not needed for normal batch recovery.

## Successes, failures and durable state

By default:

```text
copycat/state/<job-id>/
  checkpoint.sqlite3       # authoritative resume ledger; keep this!
  success.json            # cumulative completed-file list, sizes and SHA-256 hashes
  fails.json              # failures/conflicts, plus separate incomplete/skipped lists
  latest-report.json      # full inventory and most recent exit/batch reason
  report-<timestamp>.json  # history of completed invocations
  lock
```

The job ID depends on **both source and destination paths**. Keep those paths,
`staging_dir`, and `state_dir` unchanged across runs. Changing `staging_dir` for an existing
job is rejected to avoid losing track of partial files. Changing source, destination, or
state root uses a different job/ledger; it does not inherit the old success list.
Do not run different jobs into the same destination or staging folder.

On the first run after upgrading, recorded legacy partials in destination are relocated
to staging before copying resumes. Legacy successes remain in destination and missing
successes remain trusted. Let that upgrade run finish before offloading. Unrelated
pre-existing files are never removed or overwritten.

The database is updated at each checkpoint and completed/failed file. JSON snapshots are
atomically replaced periodically and on exit. **JSON files are for inspection, not editing
resume state**; after an abrupt stop they may lag until the next startup. Back up the whole
state directory, especially once you have moved recovered files elsewhere. A success means
Copycat published the completed file to destination, **not** that another archived copy still exists.

`success.json` has a `files` array of saved successful records. `fails.json` separates:

- `files`: genuine read/write/checksum/timeout/conflict errors, with messages;
- `incomplete`: interrupted or batch/space-limited files that resume automatically;
- `skipped`: symlinks and special files intentionally not copied;
- `fatal_error`: any overall error from the last report, otherwise `null`.

The full reports use these statuses:

- `done`: published to destination; `verified` records whether staging readback was enabled.
- `partial` / `pending`: unfinished in staging; automatically eligible on the next run.
- `ready`: copied/verified in staging, awaiting publication; publication resumes without recopying.
- `failed` / `conflict`: persistent errors; skipped unless retries are explicitly requested.
- `skipped`: intentional symlink/special-file omission.

A batch/free-space stop is **not a failed file**. Free space and run again without needing
`--retry-failed`. Directory errors are recorded and revisited on later runs. Reports are
cumulative; previously observed items remain in the inventory even if source entries vanish.

## Command-line overrides

CLI options override config settings. Positional source/destination paths are relative
to the current terminal directory, unlike paths inside a config.

```bash
# Change a few settings just for this invocation:
uv run copycat.py --checkpoint-mib 8 --timeout 30 --retries 0 --batch-gb 50

# Revisit genuine file failures:
uv run copycat.py --retry-failed

# A different config:
uv run copycat.py --config /path/to/settings.json

# Ignore config and supply paths/settings explicitly:
uv run copycat.py --no-config SOURCE DESTINATION --staging-dir STAGING --batch-gb 100 --min-free-gb 5

uv run copycat.py --help
```

`--staging-dir` overrides the configured incomplete-files folder.
`--attempts` is also supported (total attempts, mutually exclusive with `--retries`).
`--no-verify`, `--verify`, `--no-retry-failed`, `--check-completed`, and
`--no-check-completed` override corresponding booleans.

Exit codes: `0` normal completion or batch/space exit without saved failures/conflicts
(intentional skips may remain), `1` saved failures/conflicts, `2` configuration/fatal error,
`130` user-requested exit. `latest-report.json` explains why a run stopped.

## Limitations and safety

- No automatic deletion of source or destination files; existing unrelated destination files
  are not overwritten. Only identified partial files are truncated back to their checkpoint.
- Partial files use their relative source paths **inside staging**, never destination.
  Failed or interrupted verification does not publish a file. Do not offload staging.
  With `verify: false`, publication skips readback verification; leave it enabled.
- In-progress source and destination files must not be modified by other applications.
  Source identity uses size/mtime, not a live-filesystem snapshot. Changed source files are
  reported rather than silently replacing archived results.
- Hidden files and empty directories are included. Symlinks, devices, sockets and other
  special files are skipped and reported.
- This preserves file contents and file modification times, **not** Mac resource forks,
  extended attributes, Finder metadata, ACLs, ownership, directory timestamps, or hard-link
  relationships. Some older Mac files and application bundles need full-fidelity recovery.
- On resume, the same partial file and its saved bytes must still exist. Missing/replaced
  partial files or damaged checkpoints require manual investigation. Safety checks are not
  bypassed by `--retry-failed`.
- Filesystem limits still apply. Staging and destination need hard-link support
  (for example APFS or ext4); FAT/exFAT do not support this publication method.
- A directory scan that stalls/fails can leave some entries undiscovered; the report records
  that failure. Most stuck filesystem calls can be stopped via worker timeouts, but a worker
  blocked in the kernel may be unkillable. The job stops rather than risking concurrent writes;
  a safe disconnect/reboot may be needed.
- This is not deleted-file recovery or a disk image. If the old drive clicks/disconnects or
  repeatedly produces hardware errors, consider **GNU ddrescue** or professional recovery.

## Privacy

Only `config.example.json` belongs in version control. Local configuration, default
staging/recovered folders, checkpoints, and reports are ignored. Reports and terminal
output can contain absolute paths and source filenames; review them before sharing.
If you choose other local output folders, add them to `.gitignore` too.

## Tests

Temporary test data only; your external drive is not touched:

```bash
cd copycat
uv run python -m unittest -v
```
