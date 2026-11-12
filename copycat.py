#!/usr/bin/env python3
"""Checkpointed folder copies: incomplete files in staging, completed files in destination."""
import argparse
import collections
import errno
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import signal
import shutil
import sqlite3
import stat
import sys
import time

from filelock import FileLock, Timeout as LockTimeout

DEFAULT_STATE = Path(__file__).resolve().parent / "state"
DEFAULT_CONFIG = Path(__file__).resolve().parent / "config.json"


class StopRun(Exception):
    """An intentional, resumable exit rather than a failed file."""


class NoSpaceError(OSError):
    pass


def atomic_json(path, content):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        json.dump(content, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def check_record(path, record):
    st = os.stat(path, follow_symlinks=False)
    if (not stat.S_ISREG(st.st_mode)
            or st.st_ino != record["identity"][1]
            or fingerprint(st) != record["destination"]):
        raise OSError("Previously copied destination changed; refusing to overwrite")
    return st


def fingerprint(st):
    return [st.st_size, st.st_mtime_ns]


def digest_prefix(handle, length, conn):
    digest = hashlib.sha256()
    remaining = length
    while remaining:
        block = handle.read(min(1024 * 1024, remaining))
        if not block:
            raise OSError("Checkpoint is longer than destination file")
        digest.update(block)
        remaining -= len(block)
        conn.send(("heartbeat",))
    return digest


def worker(conn, operation, args):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    try:
        if operation == "scan":
            with os.scandir(args[0]) as entries:
                for entry in entries:
                    try:
                        st = entry.stat(follow_symlinks=False)
                        conn.send(("entry", entry.name, st.st_mode, fingerprint(st)))
                    except OSError as exc:
                        conn.send(("scan_error", entry.name, str(exc)))
        elif operation == "check":
            check_record(*args)
        elif operation == "same_device":
            if os.stat(args[0]).st_dev != os.stat(args[1]).st_dev:
                raise OSError("Staging and destination folders must be on the same filesystem")
        elif operation == "cleanup":
            path, record = args
            try:
                check_record(path, record)
            except FileNotFoundError:
                pass
            else:
                os.unlink(path)
                sync_directory(Path(path).parent)
        elif operation in ("relocate", "publish"):
            partial, completed, record = args
            if operation == "publish":
                check_record(partial, record)
            else:
                try:
                    st = os.stat(partial, follow_symlinks=False)
                except FileNotFoundError:
                    if not record.get("legacy_cleanup"):
                        raise
                    st = os.stat(completed, follow_symlinks=False)
                    if not stat.S_ISREG(st.st_mode) or st.st_ino != record["identity"][1]:
                        raise OSError("Migrated partial was replaced")
                    conn.send(("ok",))
                    return
                if not stat.S_ISREG(st.st_mode) or st.st_ino != record["identity"][1]:
                    raise OSError("Legacy partial was replaced; refusing to move it")
            try:
                # Hard-link publication is atomic and never overwrites another file.
                os.link(partial, completed, follow_symlinks=False)
            except FileExistsError:
                st = os.stat(completed, follow_symlinks=False)
                if not stat.S_ISREG(st.st_mode) or not os.path.samefile(partial, completed):
                    raise FileExistsError("Target already exists; refusing to overwrite")
            sync_directory(Path(completed).parent)
            conn.send(("published" if operation == "publish" else "relocated",))
            if conn.recv() != "ok":
                raise OSError("Publication acknowledgement failed")
            # Keep the partial until the coordinator durably saves the success.
            os.unlink(partial)
            sync_directory(Path(partial).parent)
        elif operation == "mkdir":
            path = Path(args[0])
            path.mkdir(exist_ok=True)
            if path.is_symlink() or not path.is_dir():
                raise OSError("Destination directory is a symlink or not a directory")
            sync_directory(path.parent)
        else:
            src, dst, record, checkpoint, verify, *limits = args
            byte_limit, reserve = limits if limits else (None, 0)
            # Source is opened read-only; symlinks are never intentionally followed.
            fd = os.open(src, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(fd, "rb", buffering=0) as source:
                if fingerprint(os.fstat(source.fileno())) != record["source"]:
                    raise OSError("Source changed since it was scanned")
                identity = record.get("identity")
                flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
                flags |= 0 if identity else os.O_CREAT | os.O_EXCL
                fd = os.open(dst, flags, 0o600)
                with os.fdopen(fd, "r+b", buffering=0) as target:
                    st = os.fstat(target.fileno())
                    actual = [st.st_dev, st.st_ino]
                    if not stat.S_ISREG(st.st_mode):
                        raise OSError("Destination is not a regular file")
                    if identity and st.st_ino != identity[1]:
                        raise OSError("Destination was replaced; refusing to modify it")
                    if not identity:
                        sync_directory(Path(dst).parent)
                        conn.send(("created", actual))
                        if conn.recv() != "ok":
                            raise OSError("Checkpoint acknowledgement failed")
                    offset = record.get("offset", 0)
                    if identity and st.st_dev != identity[0] and not offset and st.st_size:
                        raise OSError("Disk identity changed before the first data checkpoint; inspect partial file")
                    digest = digest_prefix(target, offset, conn)
                    if offset and digest.hexdigest() != record["digest"]:
                        raise OSError("Destination checkpoint checksum mismatch")
                    if identity:
                        # Mount device numbers can change after a reboot. The stable inode
                        # and saved prefix checksum identify a checkpointed partial instead.
                        conn.send(("identified", actual))
                    target.truncate(offset)
                    source.seek(offset)
                    since_checkpoint = 0
                    started_offset = offset
                    limit_reason = None
                    while offset < record["source"][0]:
                        size = min(1024 * 1024, record["source"][0] - offset)
                        if byte_limit is not None:
                            size = min(size, byte_limit - (offset - started_offset))
                            if size <= 0:
                                limit_reason = "batch_limit"
                                break
                        if reserve:
                            available = shutil.disk_usage(Path(dst).parent).free - reserve
                            if available <= 0:
                                limit_reason = "free_space_limit"
                                break
                            size = min(size, available)
                        block = source.read(size)
                        if not block:
                            break
                        view = memoryview(block)
                        while view:
                            written = target.write(view)
                            if not written:
                                raise OSError("Destination write made no progress")
                            view = view[written:]
                        digest.update(block)
                        offset += len(block)
                        since_checkpoint += len(block)
                        conn.send(("heartbeat",))
                        if since_checkpoint >= checkpoint:
                            os.fsync(target.fileno())
                            conn.send(("checkpoint", offset, digest.hexdigest()))
                            since_checkpoint = 0
                    os.fsync(target.fileno())
                    conn.send(("checkpoint", offset, digest.hexdigest()))
                    if limit_reason:
                        conn.send(("limited", limit_reason))
                        conn.send(("ok",))
                        return
                    if fingerprint(os.fstat(source.fileno())) != record["source"]:
                        raise OSError("Source changed during copy")
                    if offset != record["source"][0]:
                        raise OSError("Unexpected source length")
                    if verify:
                        target.seek(0)
                        checked = digest_prefix(target, offset, conn)
                        if checked.digest() != digest.digest():
                            raise OSError("Destination verification failed")
                    # Preserve modification time; don't copy source permissions/ownership.
                    os.utime(target.fileno(), ns=(st.st_atime_ns, record["source"][1]))
                    os.fsync(target.fileno())
                    conn.send(("finished", fingerprint(os.fstat(target.fileno())),
                               digest.hexdigest()))
        conn.send(("ok",))
    except Exception as exc:
        try:
            conn.send(("error", f"{type(exc).__name__}: {exc}", getattr(exc, "errno", None)))
        except (OSError, EOFError):
            pass
    finally:
        conn.close()


def supervised(operation, args, timeout, on_event):
    """Keep stalled filesystem calls out of the coordinator process."""
    context = mp.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=worker, args=(child, operation, args), daemon=True)
    last_progress = time.monotonic()
    try:
        process.start()
        child.close()
        while True:
            if parent.poll(0.2):
                try:
                    event = parent.recv()
                except EOFError:
                    raise OSError(f"Worker exited unexpectedly ({process.exitcode})")
                last_progress = time.monotonic()
                if event[0] == "error":
                    if event[2] in (errno.ENOSPC, errno.EDQUOT):
                        raise NoSpaceError(event[1])
                    raise OSError(event[1])
                if event[0] == "ok":
                    return
                on_event(event)
                if event[0] in ("created", "published", "relocated"):
                    parent.send("ok")
            elif time.monotonic() - last_progress > timeout:
                raise TimeoutError(f"No progress for {timeout:g} seconds")
            elif not process.is_alive():
                raise OSError(f"Worker exited unexpectedly ({process.exitcode})")
    finally:
        parent.close()
        child.close()
        if process.pid is not None:
            process.join(timeout=0.2)
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join(timeout=1)
            if process.is_alive():
                # Continuing could race a worker still writing after a kernel stall.
                raise RuntimeError("Worker cannot be stopped; disconnect safely/reboot before resuming")


class State:
    def __init__(self, directory, export_interval=30, context=None):
        self.directory = directory
        self.export_interval = export_interval
        self.context = context or {}
        self.last_export = 0
        directory.mkdir(parents=True, exist_ok=True)
        self.lock = FileLock(directory / "lock")
        try:
            self.lock.acquire(timeout=0)
        except LockTimeout:
            raise ValueError("Another copycat process is using this job") from None
        self.db = sqlite3.connect(directory / "checkpoint.sqlite3")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, record TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT)")
        self.db.commit()

    def bind_staging(self, staging):
        row = self.db.execute("SELECT value FROM settings WHERE name='staging_dir'").fetchone()
        if row and row[0] != str(staging):
            raise ValueError(f"This job uses staging_dir {row[0]}; keep it unchanged to resume")
        self.db.execute("INSERT OR IGNORE INTO settings VALUES ('staging_dir', ?)", (str(staging),))
        self.db.commit()

    def get(self, path):
        row = self.db.execute("SELECT record FROM files WHERE path=?", (json.dumps(path),)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, path, record):
        self.db.execute("INSERT OR REPLACE INTO files VALUES (?, ?)",
                        (json.dumps(path), json.dumps(record)))
        self.db.commit()
        self.export_progress()

    def delete(self, path):
        self.db.execute("DELETE FROM files WHERE path=?", (json.dumps(path),))
        self.db.commit()

    def records(self):
        records = []
        for path, record in self.db.execute("SELECT path, record FROM files ORDER BY path"):
            path = json.loads(path)
            item = {"path": path, **json.loads(record)}
            if path.startswith("\0"):
                operation, relative = path[1:].split(":", 1)
                item.update(path=relative, operation=operation)
            records.append(item)
        return records

    def export_progress(self, force=False, records=None, fatal_error=None):
        if not force and time.monotonic() - self.last_export < self.export_interval:
            return
        records = self.records() if records is None else records
        common = {**self.context, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
        successes = [row for row in records if row["status"] == "done"]
        failures = [row for row in records if row["status"] in ("failed", "conflict")]
        try:
            atomic_json(self.directory / "success.json", {**common, "count": len(successes),
                                                         "files": successes})
            atomic_json(self.directory / "fails.json", {
                **common, "count": len(failures), "fatal_error": fatal_error, "files": failures,
                "incomplete": [row for row in records if row["status"] in ("pending", "partial", "ready")],
                "skipped": [row for row in records if row["status"] == "skipped"],
            })
        except OSError as exc:
            raise RuntimeError(f"Cannot save progress reports: {exc}") from exc
        self.last_export = time.monotonic()

    def report(self, source, destination, interrupted, fatal_error=None,
               stop_reason=None, bytes_copied=0):
        # Finish a safe checkpoint transaction if an exit signal interrupted its commit.
        self.db.commit()
        records = self.records()
        self.export_progress(force=True, records=records, fatal_error=fatal_error)
        counts = dict(collections.Counter(row["status"] for row in records))
        report = {**self.context, "source": str(source), "destination": str(destination),
                  "interrupted": interrupted, "fatal_error": fatal_error,
                  "stop_reason": stop_reason, "bytes_copied_this_run": bytes_copied,
                  "counts": counts, "files": records}
        stamp = time.strftime("%Y%m%d-%H%M%S") + f"-{time.time_ns() % 1000000000:09d}"
        path = self.directory / f"report-{stamp}.json"
        atomic_json(path, report)
        atomic_json(self.directory / "latest-report.json", report)
        print(f"\nSummary: {counts}\nReport: {path}\n"
              f"Successes: {self.directory / 'success.json'}\n"
              f"Failures: {self.directory / 'fails.json'}", flush=True)
        return counts


def within(path, parent):
    return path == parent or parent in path.parents


def run(args):
    source = Path(args.source).expanduser().resolve()
    destination = Path(args.destination).expanduser().absolute()
    # Reject symlink components rather than following them into unexpected folders.
    for part in [destination, *destination.parents]:
        if part.is_symlink():
            raise ValueError(f"Destination path contains a symlink: {part}")
    destination = destination.resolve()
    if within(destination, source) or within(source, destination):
        raise ValueError("Source and destination must not overlap")
    staging = (Path(args.staging_dir).expanduser().absolute() if args.staging_dir
               else destination.with_name(destination.name + ".staging"))
    for part in [staging, *staging.parents]:
        if part.is_symlink():
            raise ValueError(f"Staging path contains a symlink: {part}")
    staging = staging.resolve()
    if (within(staging, source) or within(source, staging)
            or within(staging, destination) or within(destination, staging)):
        raise ValueError("Staging, source and destination must not overlap")
    state_root = Path(args.state_dir).expanduser().resolve()
    if any(within(state_root, root) or within(root, state_root)
           for root in (source, destination, staging)):
        raise ValueError("State directory must be outside source, destination and staging")
    if not destination.parent.is_dir() or not staging.parent.is_dir():
        raise ValueError("Destination and staging parents must already exist (check the drives are mounted)")
    job = hashlib.sha256(os.fsencode(f"{source}\0{destination}")).hexdigest()[:20]
    state = State(state_root / job, args.report_interval,
                  {"source": str(source), "destination": str(destination),
                   "staging_dir": str(staging)})
    try:
        state.bind_staging(staging)
    except Exception:
        state.db.close()
        state.lock.release()
        raise
    print(f"Source: {source}\nStaging (incomplete): {staging}\n"
          f"Destination (ready to offload): {destination}\nState: {state.directory}", flush=True)
    interrupted = False
    fatal_error = None
    stop_reason = None
    bytes_copied = 0
    batch_bytes = int(args.batch_gb * 1_000_000_000) if args.batch_gb else None
    reserve_bytes = int(args.min_free_gb * 1_000_000_000)

    def ensure_dir(path):
        supervised("mkdir", (str(path),), args.timeout, lambda event: None)

    def ensure_parents(root, relative):
        current = root
        for component in Path(relative).parent.parts:
            current /= component
            ensure_dir(current)

    def publish(relative, record, target):
        ensure_parents(destination, relative)

        def event(message):
            if message[0] == "published":
                record.update(status="done")
                record.pop("error", None)
                state.put(relative, record)

        supervised("publish", (str(target), str(destination / relative), record),
                   args.timeout, event)
        print(f"READY {relative}", flush=True)

    def copy_file(relative, signature):
        old = state.get(relative)
        record = old or {"source": signature, "status": "pending", "offset": 0,
                         "layout": "separate"}
        target = staging / relative
        if old and old["source"] != signature:
            record.update(status="failed", error="Source changed since previous run; use a new destination")
            state.put(relative, record)
            print(f"FAILED {relative}: {record['error']}", flush=True)
            return
        if record["status"] == "done":
            if record.get("layout") == "separate":
                supervised("cleanup", (str(target), record), args.timeout, lambda event: None)
            if not args.check_completed:
                print(f"ALREADY COPIED {relative} (saved success; destination may have been moved)",
                      flush=True)
                return
            try:
                supervised("check", (str(destination / relative), record), args.timeout,
                           lambda event: None)
                print(f"ALREADY COPIED {relative}", flush=True)
            except OSError as exc:
                record.update(status="conflict", error=str(exc))
                state.put(relative, record)
                print(f"DESTINATION CHANGED {relative}: {exc}", flush=True)
            return
        if record["status"] in ("failed", "conflict") and not args.retry_failed:
            print(f"SKIP FAILED {relative} (use --retry-failed)", flush=True)
            return
        state.put(relative, record)
        if record.get("sha256"):
            try:
                publish(relative, record, target)
            except NoSpaceError as exc:
                if record["status"] == "done":
                    raise
                record.update(status="ready", error=str(exc))
                state.put(relative, record)
                raise StopRun("free_space_limit") from exc
            except OSError as exc:
                if record["status"] == "done":
                    raise
                record.update(status="conflict", error=str(exc))
                state.put(relative, record)
                print(f"PUBLISH FAILED {relative}: {exc}", flush=True)
            return
        ensure_parents(staging, relative)
        # Detect unrelated destination files before reading the source contents.
        if os.path.lexists(destination / relative):
            record.update(status="conflict", error="Destination already exists; refusing to overwrite")
            state.put(relative, record)
            print(f"CONFLICT {relative}: {record['error']}", flush=True)
            return
        for attempt in range(args.attempts):
            remaining = None if batch_bytes is None else batch_bytes - bytes_copied
            if remaining is not None and remaining <= 0:
                raise StopRun("batch_limit")
            limited = None
            record["status"] = "partial" if record.get("identity") else "pending"
            record["attempts"] = record.get("attempts", 0) + 1
            state.put(relative, record)

            def event(message):
                nonlocal bytes_copied, limited
                if message[0] in ("created", "identified"):
                    record.update(identity=message[1], status="partial")
                elif message[0] == "checkpoint":
                    bytes_copied += max(0, message[1] - record.get("offset", 0))
                    record.update(offset=message[1], digest=message[2], status="partial")
                    print(f"  {relative}: {message[1]:,}/{signature[0]:,} bytes", flush=True)
                elif message[0] == "finished":
                    record.update(status="ready", destination=message[1], sha256=message[2],
                                  verified=not args.no_verify)
                    record.pop("error", None)
                elif message[0] == "limited":
                    limited = message[1]
                else:
                    return
                state.put(relative, record)
            try:
                print(f"COPY {relative} (attempt {attempt + 1}/{args.attempts})", flush=True)
                supervised("copy", (str(source / relative), str(target),
                           record, args.checkpoint_mib * 1024 * 1024, not args.no_verify,
                           remaining, reserve_bytes), args.timeout, event)
                if limited:
                    raise StopRun(limited)
                publish(relative, record, target)
                return
            except NoSpaceError as exc:
                if record["status"] == "done":
                    raise
                record.update(status="ready" if record.get("sha256") else
                              ("partial" if record.get("identity") else "pending"),
                              error=str(exc))
                state.put(relative, record)
                raise StopRun("free_space_limit") from exc
            except OSError as exc:
                if record["status"] == "done":
                    raise
                record.update(status="conflict" if "FileExistsError" in str(exc) else "failed",
                              error=str(exc))
                state.put(relative, record)
                print(f"FAILED {relative}: {exc}", flush=True)
                if record["status"] == "conflict" or record.get("sha256"):
                    break

    try:
        state.export_progress(force=True)
        ensure_dir(destination)
        ensure_dir(staging)
        supervised("same_device", (str(destination), str(staging)), args.timeout,
                   lambda event: None)
        # Upgrade old checkpoints before exposing the destination as completed-only.
        for saved in state.records():
            if ("operation" in saved or saved["status"] in ("done", "skipped")
                    or saved.get("sha256")):
                continue
            relative = saved.pop("path")
            if saved.get("layout") == "separate" and not saved.get("legacy_cleanup"):
                continue
            if saved.get("identity"):
                ensure_parents(staging, relative)

                def relocated(event):
                    if event[0] == "relocated":
                        saved.update(layout="separate", legacy_cleanup=True)
                        state.put(relative, saved)

                supervised("relocate", (str(destination / relative), str(staging / relative), saved),
                           args.timeout, relocated)
            saved.update(layout="separate")
            saved.pop("legacy_cleanup", None)
            state.put(relative, saved)
        stack = [Path(".")]
        while stack:
            relative_dir = stack.pop()
            entries = []
            scan_key = f"\0scan:{relative_dir}"

            def scan_event(event):
                if event[0] == "entry":
                    entries.append(event[1:])
                elif event[0] == "scan_error":
                    key = str(relative_dir / event[1])
                    state.put(f"\0stat:{key}", {"status": "failed", "error": event[2]})
                    print(f"STAT FAILED {key}: {event[2]}", flush=True)

            try:
                supervised("scan", (str(source / relative_dir),), args.timeout, scan_event)
                # A successful later scan clears earlier directory errors.
                state.delete(scan_key)
            except OSError as exc:
                state.put(scan_key, {"status": "failed", "error": str(exc)})
                print(f"SCAN FAILED {relative_dir}: {exc}", flush=True)
                # Entries already received are still usable.
            for name, mode, signature in entries:
                relative = relative_dir / name
                key = str(relative)
                state.delete(f"\0stat:{key}")
                if stat.S_ISDIR(mode):
                    try:
                        ensure_parents(destination, str(relative / "placeholder"))
                        ensure_parents(staging, str(relative / "placeholder"))
                        stack.append(relative)
                        state.delete(f"\0mkdir:{key}")
                    except OSError as exc:
                        state.put(f"\0mkdir:{key}", {"status": "failed", "error": str(exc)})
                        print(f"DIRECTORY FAILED {relative}: {exc}", flush=True)
                elif stat.S_ISREG(mode):
                    copy_file(key, signature)
                else:
                    state.put(key, {"status": "skipped", "source": signature,
                                    "error": "Symlink or special file; not followed/copied"})
                    print(f"SKIP {relative}: symlink or special file", flush=True)
    except StopRun as exc:
        stop_reason = str(exc)
        print(f"\nExiting: {stop_reason}. Move completed files/free space, then run again.", flush=True)
    except KeyboardInterrupt:
        interrupted = True
        stop_reason = "user_exit"
        print("\nExiting. Progress is on disk; run the same command to resume after shutdown.", flush=True)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        fatal_error = str(exc)
        raise
    finally:
        try:
            counts = state.report(source, destination, interrupted, fatal_error,
                                  stop_reason, bytes_copied)
        finally:
            state.db.close()
            state.lock.release()
    return 130 if interrupted else (1 if counts.get("failed") or counts.get("conflict") else 0)


def parse_args(argv=None):
    pre = argparse.ArgumentParser(add_help=False)
    group = pre.add_mutually_exclusive_group()
    group.add_argument("--config", help=f"JSON config (default: {DEFAULT_CONFIG})")
    group.add_argument("--no-config", action="store_true", help="Ignore the default config")
    selected, _ = pre.parse_known_args(argv)
    parser = argparse.ArgumentParser(description=__doc__, parents=[pre])
    config = {"source": None, "destination": None, "staging_dir": None,
              "state_dir": str(DEFAULT_STATE),
              "timeout_seconds": 60, "retries": 1, "checkpoint_mib": 16,
              "verify": True, "retry_failed": False, "check_completed": False,
              "batch_gb": 0, "min_free_gb": 5, "report_interval_seconds": 30}
    path = Path(selected.config or DEFAULT_CONFIG).expanduser().resolve()
    if not selected.no_config and (selected.config or path.exists()):
        try:
            loaded = json.loads(path.read_text())
            if not isinstance(loaded, dict):
                raise ValueError("Config must be a JSON object")
            unknown = loaded.keys() - config.keys()
            if unknown:
                raise ValueError(f"Unknown config keys: {', '.join(sorted(unknown))}")
            for key, value in loaded.items():
                if key in ("source", "destination", "staging_dir", "state_dir"):
                    if value is None and key != "state_dir":
                        continue
                    if not isinstance(value, str) or not value:
                        raise ValueError(f"{key} must be a nonempty path string")
                    candidate = Path(value).expanduser()
                    loaded[key] = str(candidate if candidate.is_absolute() else path.parent / candidate)
                elif key in ("verify", "retry_failed", "check_completed"):
                    if type(value) is not bool:
                        raise ValueError(f"{key} must be true or false")
                elif key in ("retries", "checkpoint_mib"):
                    if type(value) is not int:
                        raise ValueError(f"{key} must be an integer")
                elif type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError(f"{key} must be a finite number")
            config.update(loaded)
        except (OSError, ValueError) as exc:
            parser.error(f"Cannot load {path}: {exc}")
    parser.add_argument("source", nargs="?", default=config["source"], help="Source folder (read-only)")
    parser.add_argument("destination", nargs="?", default=config["destination"], help="Completed files, ready to offload")
    parser.add_argument("--staging-dir", default=config["staging_dir"],
                        help="Incomplete files (default: DESTINATION.staging); same filesystem as destination")
    parser.add_argument("--state-dir", default=config["state_dir"], help="Permanent checkpoint/report root")
    parser.add_argument("--timeout", type=float, default=config["timeout_seconds"], help="No-progress timeout (seconds)")
    attempts = parser.add_mutually_exclusive_group()
    attempts.add_argument("--attempts", type=int, default=config["retries"] + 1, help="Total attempts per file")
    attempts.add_argument("--retries", type=int, help="Extra attempts after the first")
    parser.add_argument("--checkpoint-mib", type=int, default=config["checkpoint_mib"], help="Checkpoint interval (MiB)")
    parser.add_argument("--report-interval", type=float, default=config["report_interval_seconds"], help="JSON export interval (seconds)")
    parser.add_argument("--batch-gb", type=float, default=config["batch_gb"], help="New bytes per run in decimal GB; 0 = unlimited")
    parser.add_argument("--min-free-gb", type=float, default=config["min_free_gb"], help="Keep this much destination space free (GB)")
    parser.add_argument("--retry-failed", action=argparse.BooleanOptionalAction,
                        default=config["retry_failed"], help="Revisit persistently failed files")
    parser.add_argument("--check-completed", action=argparse.BooleanOptionalAction,
                        default=config["check_completed"], help="Check completed destination files; normally trust saved successes")
    verify = parser.add_mutually_exclusive_group()
    verify.add_argument("--no-verify", dest="no_verify", action="store_true", help="Skip final SHA-256 readback")
    verify.add_argument("--verify", dest="no_verify", action="store_false", help="Enable SHA-256 readback")
    parser.set_defaults(no_verify=not config["verify"])
    args = parser.parse_args(argv)
    if args.retries is not None:
        if args.retries < 0:
            parser.error("retries must be nonnegative")
        args.attempts = args.retries + 1
    if not args.source or not args.destination:
        parser.error("Set source and destination in config.json or supply both as arguments")
    numbers = (args.timeout, args.report_interval, args.batch_gb, args.min_free_gb)
    if not all(math.isfinite(number) for number in numbers):
        parser.error("Numeric settings must be finite")
    if (args.timeout <= 0 or args.attempts < 1 or args.checkpoint_mib < 1
            or args.report_interval <= 0 or args.batch_gb < 0 or args.min_free_gb < 0):
        parser.error("Timeout, attempts, checkpoints and report interval must be positive; GB limits nonnegative")
    if 0 < args.batch_gb < 1e-9:
        parser.error("batch-gb must be zero or at least one byte")
    return args


def request_exit(signum, frame):
    raise KeyboardInterrupt


def main():
    args = parse_args()
    previous = {sig: signal.signal(sig, request_exit) for sig in (signal.SIGHUP, signal.SIGTERM)}
    try:
        return run(args)
    except KeyboardInterrupt:
        print("Exited. Run again to resume saved progress.", file=sys.stderr)
        return 130
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    sys.exit(main())
