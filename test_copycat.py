import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import copycat


class CopycatTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "source"
        self.source.mkdir()
        self.destination = self.root / "destination"
        self.staging = self.root / "staging"
        self.state_root = self.root / "state"

    def command(self, *extra):
        return [sys.executable, str(Path(copycat.__file__)), str(self.source),
                str(self.destination), "--no-config", "--state-dir", str(self.state_root),
                "--staging-dir", str(self.staging),
                "--checkpoint-mib", "1", "--min-free-gb", "0", *extra]

    def run_in_process(self, *extra):
        """Run in this process so tests can count supervised worker operations."""
        args = copycat.parse_args(self.command(*extra)[2:])
        operations = []
        original = copycat.supervised

        def counting(operation, *rest):
            operations.append(operation)
            return original(operation, *rest)

        output = io.StringIO()
        with patch.object(copycat, "supervised", counting), contextlib.redirect_stdout(output):
            code = copycat.run(args)
        return code, operations, output.getvalue()

    def run_copy(self, *extra, expected=0):
        result = subprocess.run(self.command(*extra), capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def job_dir(self):
        job = hashlib.sha256(f"{self.source}\0{self.destination}".encode()).hexdigest()[:20]
        return self.state_root / job

    def report(self):
        return json.loads((self.job_dir() / "latest-report.json").read_text())

    def seed_partial(self, data, offset, digest=None):
        (self.source / "file").write_bytes(data)
        self.staging.mkdir()
        target = self.staging / "file"
        target.write_bytes(data[:offset] + b"uncommitted tail")
        st = target.stat()
        record = {"source": copycat.fingerprint((self.source / "file").stat()),
                  "status": "partial", "layout": "separate", "offset": offset,
                  "digest": digest or hashlib.sha256(data[:offset]).hexdigest(),
                  "identity": [st.st_dev, st.st_ino]}
        state = copycat.State(self.job_dir())
        state.put("file", record)
        state.db.close()
        state.lock.release()
        return target

    def test_copy_and_resume_completed_files(self):
        (self.source / "nested").mkdir()
        (self.source / "empty-dir").mkdir()
        payload = os.urandom(2 * 1024 * 1024 + 35)
        (self.source / "nested" / "binary").write_bytes(payload)
        (self.source / ".hidden").write_text("hidden")
        (self.source / "empty").touch()
        (self.source / "link").symlink_to(".hidden")
        self.run_copy()
        target = self.destination / "nested" / "binary"
        self.assertEqual(target.read_bytes(), payload)
        self.assertTrue((self.destination / "empty-dir").is_dir())
        self.assertEqual((self.destination / ".hidden").read_text(), "hidden")
        self.assertFalse((self.destination / "link").exists())
        self.assertEqual(self.report()["counts"], {"done": 3, "skipped": 1})
        record = next(r for r in self.report()["files"] if r["path"] == "nested/binary")
        self.assertTrue(record["verified"])
        self.assertEqual(record["sha256"], hashlib.sha256(payload).hexdigest())
        before = target.stat()
        result = self.run_copy()
        self.assertIn("ALREADY COPIED nested/binary", result.stdout)
        self.assertEqual(target.stat().st_ino, before.st_ino)
        self.assertEqual(target.stat().st_mtime_ns, before.st_mtime_ns)

    def test_conflict_is_not_overwritten_and_other_files_continue(self):
        (self.source / "conflict").write_text("source")
        (self.source / "good").write_text("good")
        self.destination.mkdir()
        target = self.destination / "conflict"
        target.write_text("precious existing data")
        self.run_copy(expected=1)
        self.assertEqual(target.read_text(), "precious existing data")
        self.assertEqual((self.destination / "good").read_text(), "good")
        result = self.run_copy(expected=1)
        self.assertIn("SKIP FAILED conflict", result.stdout)
        target.unlink()
        self.run_copy("--retry-failed")
        self.assertEqual(target.read_text(), "source")

    def test_resumes_checkpoint_and_discards_only_uncommitted_tail(self):
        data = os.urandom(3 * 1024 * 1024 + 77)
        target = self.seed_partial(data, 1024 * 1024)
        original_inode = target.stat().st_ino
        self.run_copy()
        self.assertFalse(target.exists())
        completed = self.destination / "file"
        self.assertEqual(completed.read_bytes(), data)
        self.assertEqual(completed.stat().st_ino, original_inode)
        self.assertEqual(self.report()["counts"], {"done": 1})

    def test_partial_resume_allows_mount_device_number_to_change_after_reboot(self):
        data = os.urandom(2 * 1024 * 1024)
        target = self.seed_partial(data, 1024 * 1024)
        state = copycat.State(self.job_dir())
        record = state.get("file")
        record["identity"][0] += 1  # Simulate a different mount device number next morning.
        state.put("file", record)
        state.db.close()
        state.lock.release()
        self.run_copy()
        self.assertFalse(target.exists())
        self.assertEqual((self.destination / "file").read_bytes(), data)
        self.assertEqual(self.report()["counts"], {"done": 1})

    def test_bad_checkpoint_does_not_modify_destination(self):
        target = self.seed_partial(b"abcdefghijk", 5, digest="bad")
        before = target.read_bytes()
        self.run_copy("--attempts", "1", expected=1)
        self.assertEqual(target.read_bytes(), before)
        self.assertIn("checksum mismatch", self.report()["files"][0]["error"])

    def test_changed_source_is_not_overwritten(self):
        src = self.source / "file"
        src.write_text("original")
        self.run_copy()
        src.write_text("changed source")
        self.run_copy(expected=1)
        self.assertEqual((self.destination / "file").read_text(), "original")

    def test_changed_completed_destination_is_reported(self):
        (self.source / "file").write_text("original")
        self.run_copy()
        (self.destination / "file").write_text("modified outside copycat")
        self.run_copy("--check-completed", expected=1)
        self.assertEqual(self.report()["counts"], {"conflict": 1})

    def test_unreadable_file_is_bounded_and_other_files_copy(self):
        bad = self.source / "bad"
        bad.write_text("unreadable")
        bad.chmod(0)
        self.addCleanup(lambda: bad.chmod(0o600))
        (self.source / "good").write_text("good")
        result = self.run_copy("--attempts", "2", expected=1)
        self.assertEqual(result.stdout.count("COPY bad (attempt"), 2)
        self.assertEqual((self.destination / "good").read_text(), "good")
        self.assertEqual(self.report()["counts"], {"failed": 1, "done": 1})

    def test_unreadable_directory_is_reported(self):
        bad = self.source / "blocked"
        bad.mkdir()
        bad.chmod(0)
        self.addCleanup(lambda: bad.chmod(0o700))
        (self.source / "good").write_text("good")
        self.run_copy(expected=1)
        self.assertEqual((self.destination / "good").read_text(), "good")
        bad.chmod(0o700)
        self.run_copy()
        self.assertEqual(self.report()["counts"], {"done": 1})

    def test_overlapping_paths_rejected(self):
        self.destination = self.source / "nested"
        result = self.run_copy(expected=2)
        self.assertIn("must not overlap", result.stderr)
        self.assertFalse(self.destination.exists())

    def test_destination_symlink_rejected(self):
        actual = self.root / "actual"
        actual.mkdir()
        self.destination.symlink_to(actual, target_is_directory=True)
        self.run_copy(expected=2)
        self.assertEqual(list(actual.iterdir()), [])

    def test_timeout_stops_blocked_worker(self):
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            copycat.supervised("copy", (str(fifo), str(self.root / "out"),
                               {"source": [0, 0]}, 1024, True), 0.3, lambda event: None)
        self.assertLess(time.monotonic() - started, 5)

    def test_unusual_filenames_do_not_collide_with_scan_reports(self):
        for name in ["@scan:.", '@stat:quote"', "snowman-☃", "line\nbreak"]:
            (self.source / name).write_text(name)
        self.run_copy()
        self.assertEqual(self.report()["counts"], {"done": 4})
        for source in self.source.iterdir():
            self.assertEqual((self.destination / source.name).read_text(), source.name)

    def test_verification_detects_destination_corruption(self):
        source = self.source / "file"
        source.write_bytes(b"source contents")
        self.destination.mkdir()
        target = self.destination / "file"
        messages = []

        class Connection:
            def send(self, event):
                messages.append(event)
                if event[0] == "checkpoint":
                    with target.open("r+b") as stream:
                        stream.write(b"X")

            def recv(self):
                return "ok"

            def close(self):
                pass

        record = {"source": copycat.fingerprint(source.stat()), "offset": 0}
        with patch.object(copycat.signal, "signal"):
            copycat.worker(Connection(), "copy", (str(source), str(target), record, 1024, True))
        self.assertTrue(any(event[0] == "error" and "verification failed" in event[1]
                            for event in messages))
        self.assertFalse(any(event[0] == "finished" for event in messages))

    def test_fatal_destination_error_is_in_report(self):
        self.destination.write_text("existing file, not a directory")
        self.run_copy(expected=2)
        self.assertIsNotNone(self.report()["fatal_error"])
        self.assertEqual(self.destination.read_text(), "existing file, not a directory")

    def test_one_job_at_a_time(self):
        first = copycat.State(self.job_dir())
        try:
            with self.assertRaisesRegex(ValueError, "Another copycat"):
                copycat.State(self.job_dir())
        finally:
            first.db.close()
            first.lock.release()

    def test_moved_successes_are_not_recopied(self):
        (self.source / "file").write_text("keep in archive")
        self.run_copy()
        archive = self.root / "archive"
        shutil.move(str(self.destination), archive)
        # A new process with no staging folder must still trust the saved ledger.
        result = self.run_copy()
        self.assertIn("saved success", result.stdout)
        self.assertFalse((self.destination / "file").exists())
        self.assertEqual((archive / "file").read_text(), "keep in archive")
        successes = json.loads((self.job_dir() / "success.json").read_text())
        self.assertEqual(successes["count"], 1)
        self.assertEqual(successes["files"][0]["path"], "file")
        (self.source / "new").write_text("next batch")
        self.run_copy()
        self.assertEqual((self.destination / "new").read_text(), "next batch")
        self.assertFalse((self.destination / "file").exists())

    def test_batch_limit_with_completed_files_moved_between_runs(self):
        (self.source / "one").write_bytes(b"a" * 1_000_000)
        (self.source / "two").write_bytes(b"b" * 1_000_000)
        self.run_copy("--batch-gb", "0.001")
        report = self.report()
        self.assertEqual(report["stop_reason"], "batch_limit")
        self.assertEqual(report["bytes_copied_this_run"], 1_000_000)
        successes = json.loads((self.job_dir() / "success.json").read_text())
        self.assertEqual(successes["count"], 1)
        completed = successes["files"][0]["path"]
        archive = self.root / "archive"
        archive.mkdir()
        shutil.move(str(self.destination / completed), archive / completed)
        self.run_copy("--batch-gb", "0.001")
        self.assertEqual(self.report()["counts"], {"done": 2})
        self.assertFalse((self.destination / completed).exists())
        self.assertEqual(sum(p.stat().st_size for p in self.destination.iterdir()), 1_000_000)

    def test_batch_limit_checkpoints_inside_large_file(self):
        data = os.urandom(2_200_000)
        (self.source / "file").write_bytes(data)
        for expected_offset in (1_000_000, 2_000_000):
            self.run_copy("--batch-gb", "0.001")
            report = self.report()
            self.assertEqual(report["stop_reason"], "batch_limit")
            self.assertEqual(report["files"][0]["offset"], expected_offset)
            self.assertEqual(report["counts"], {"partial": 1})
            self.assertEqual((self.staging / "file").read_bytes(), data[:expected_offset])
            self.assertFalse((self.destination / "file").exists())
        self.run_copy("--batch-gb", "0.001")
        self.assertEqual((self.destination / "file").read_bytes(), data)
        self.assertFalse((self.staging / "file").exists())
        self.assertEqual(self.report()["counts"], {"done": 1})

    def test_space_reserve_exits_without_poisoning_failure_list(self):
        (self.source / "file").write_text("copy after freeing space")
        self.run_copy("--min-free-gb", "1000000000")
        self.assertEqual(self.report()["stop_reason"], "free_space_limit")
        failures = json.loads((self.job_dir() / "fails.json").read_text())
        self.assertEqual(failures["count"], 0)
        self.assertEqual(len(failures["incomplete"]), 1)
        self.run_copy()
        self.assertEqual((self.destination / "file").read_text(), "copy after freeing space")

    def test_manifests_regenerated_from_durable_database(self):
        (self.source / "good").write_text("good")
        (self.source / "conflict").write_text("source")
        self.destination.mkdir()
        (self.destination / "conflict").write_text("preserve")
        self.run_copy(expected=1)
        for name, count in (("success.json", 1), ("fails.json", 1)):
            path = self.job_dir() / name
            self.assertEqual(json.loads(path.read_text())["count"], count)
            path.unlink()
        self.run_copy(expected=1)
        self.assertEqual(json.loads((self.job_dir() / "success.json").read_text())["count"], 1)
        failures = json.loads((self.job_dir() / "fails.json").read_text())
        self.assertEqual(failures["files"][0]["path"], "conflict")
        self.assertFalse(list(self.job_dir().glob("*.tmp")))

    def test_config_paths_and_cli_overrides(self):
        config = self.root / "settings.json"
        config.write_text(json.dumps({"source": "source", "destination": "destination",
                                      "staging_dir": "staging", "state_dir": "my-state", "checkpoint_mib": 3,
                                      "timeout_seconds": 11, "retries": 2,
                                      "batch_gb": 100, "min_free_gb": 5,
                                      "verify": True, "report_interval_seconds": 7}))
        args = copycat.parse_args(["--config", str(config)])
        self.assertEqual(args.source, str(self.source))
        self.assertEqual(args.destination, str(self.destination))
        self.assertEqual(args.staging_dir, str(self.staging))
        self.assertEqual(args.state_dir, str(self.root / "my-state"))
        self.assertEqual((args.checkpoint_mib, args.timeout, args.attempts), (3, 11, 3))
        self.assertEqual((args.batch_gb, args.min_free_gb, args.report_interval), (100, 5, 7))
        args = copycat.parse_args(["--config", str(config), "--timeout", "8", "--retries", "0",
                                   "--batch-gb", "0", "--no-verify", "--check-completed"])
        self.assertEqual((args.timeout, args.attempts, args.batch_gb), (8, 1, 0))
        self.assertTrue(args.no_verify)
        self.assertTrue(args.check_completed)
        # Actually run with config-only paths, not just positional arguments.
        result = subprocess.run([sys.executable, copycat.__file__, "--config", str(config),
                                 "--min-free-gb", "0"], cwd="/", capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_invalid_configs_fail_before_copying(self):
        config = self.root / "bad.json"
        for settings in ({"typo": 1}, {"retries": True}, {"verify": "false"},
                         {"timeout_seconds": float("nan")}, {"checkpoint_mib": 0},
                         {"retries": -1}, {"batch_gb": -1}):
            config.write_text(json.dumps({"source": str(self.source),
                                          "destination": str(self.destination), **settings}))
            with self.subTest(settings=settings), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    copycat.parse_args(["--config", str(config)])
                self.assertEqual(raised.exception.code, 2)
        self.assertFalse(self.destination.exists())

    def seed_ready(self, data, status="ready", linked=False):
        target = self.seed_partial(data, len(data))
        target.write_bytes(data)
        os.utime(target, ns=(target.stat().st_atime_ns, (self.source / "file").stat().st_mtime_ns))
        state = copycat.State(self.job_dir())
        try:
            record = state.get("file")
            record.update(status=status, destination=copycat.fingerprint(target.stat()),
                          sha256=hashlib.sha256(data).hexdigest(), verified=True)
            state.put("file", record)
        finally:
            state.db.close()
            state.lock.release()
        if linked:
            self.destination.mkdir()
            os.link(target, self.destination / "file")
        return target

    def test_ready_file_is_published_without_recopying(self):
        target = self.seed_ready(b"verified contents")
        result = self.run_copy()
        self.assertNotIn("COPY file (attempt", result.stdout)
        self.assertEqual((self.destination / "file").read_bytes(), b"verified contents")
        self.assertFalse(target.exists())
        self.assertEqual(self.report()["counts"], {"done": 1})

    def test_recovers_interruption_after_publication_before_success_commit(self):
        target = self.seed_ready(b"published contents", linked=True)
        inode = target.stat().st_ino
        self.run_copy()
        self.assertFalse(target.exists())
        self.assertEqual((self.destination / "file").stat().st_ino, inode)
        self.assertEqual(self.report()["counts"], {"done": 1})

    def test_cleans_staging_after_interruption_following_success_commit(self):
        target = self.seed_ready(b"durable success", status="done", linked=True)
        (self.destination / "file").unlink()  # Already offloaded.
        result = self.run_copy()
        self.assertFalse(target.exists())
        self.assertFalse((self.destination / "file").exists())
        self.assertIn("saved success", result.stdout)

    def test_publication_conflict_keeps_verified_staging_file(self):
        target = self.seed_ready(b"verified contents")
        self.destination.mkdir()
        completed = self.destination / "file"
        completed.write_text("unrelated contents")
        self.run_copy(expected=1)
        self.assertEqual(completed.read_text(), "unrelated contents")
        self.assertEqual(target.read_bytes(), b"verified contents")
        self.assertEqual(self.report()["counts"], {"conflict": 1})
        completed.unlink()
        result = self.run_copy("--retry-failed")
        self.assertNotIn("COPY file (attempt", result.stdout)
        self.assertEqual(completed.read_bytes(), b"verified contents")
        self.assertFalse(target.exists())

    def test_legacy_partials_are_relocated_before_resume(self):
        data = os.urandom(2_200_000)
        target = self.seed_partial(data, 1_000_000)
        self.destination.mkdir()
        target.rename(self.destination / "file")
        state = copycat.State(self.job_dir())
        try:
            record = state.get("file")
            record.pop("layout")
            state.put("file", record)
        finally:
            state.db.close()
            state.lock.release()
        self.run_copy("--batch-gb", "0.001")
        self.assertFalse((self.destination / "file").exists())
        self.assertEqual(target.read_bytes(), data[:2_000_000])
        self.run_copy()
        self.assertFalse(target.exists())
        self.assertEqual((self.destination / "file").read_bytes(), data)

    def test_legacy_completed_files_are_left_in_destination(self):
        (self.source / "file").write_text("legacy success")
        self.run_copy()
        state = copycat.State(self.job_dir())
        try:
            record = state.get("file")
            record.pop("layout")
            state.put("file", record)
        finally:
            state.db.close()
            state.lock.release()
        self.run_copy()
        self.assertEqual((self.destination / "file").read_text(), "legacy success")
        self.assertFalse((self.staging / "file").exists())

    def test_staging_path_cannot_change_for_existing_job(self):
        self.run_copy()
        other = self.root / "other-staging"
        result = self.run_copy("--staging-dir", str(other), expected=2)
        self.assertIn("keep it unchanged", result.stderr)
        self.assertFalse(other.exists())

    def test_staging_overlap_is_rejected(self):
        for path in (self.destination, self.destination / "nested", self.root,
                     self.source, self.source / "nested", self.state_root):
            with self.subTest(path=path):
                self.run_copy("--staging-dir", str(path), expected=2)
        self.assertFalse(self.destination.exists())

    def test_staging_symlinks_are_rejected(self):
        other = self.root / "other"
        other.mkdir()
        self.staging.symlink_to(other, target_is_directory=True)
        self.run_copy(expected=2)
        self.assertEqual(list(other.iterdir()), [])

    def test_default_staging_is_outside_destination(self):
        (self.source / "file").write_bytes(b"a" * 2_000_000)
        command = [sys.executable, copycat.__file__, "--no-config", str(self.source),
                   str(self.destination), "--state-dir", str(self.state_root),
                   "--batch-gb", "0.001", "--min-free-gb", "0"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        default_staging = self.destination.with_name(self.destination.name + ".staging")
        self.assertEqual((default_staging / "file").stat().st_size, 1_000_000)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_completed_files_do_not_start_workers_on_later_runs(self):
        for number in range(3):
            (self.source / f"file{number}").write_text(str(number))
        self.run_copy()
        code, operations, output = self.run_in_process()
        self.assertEqual(code, 0)
        self.assertNotIn("cleanup", operations)
        self.assertNotIn("copy", operations)
        self.assertEqual(output.count("ALREADY COPIED"), 3)

    def test_folders_are_created_once_per_run(self):
        nested = self.source / "a" / "b" / "c"
        nested.mkdir(parents=True)
        for number in range(5):
            (nested / f"file{number}").write_text(str(number))
        code, operations, _ = self.run_in_process()
        self.assertEqual(code, 0)
        # Destination and staging roots, plus a, a/b and a/b/c under each.
        self.assertEqual(operations.count("mkdir"), 8)

    def test_incomplete_files_do_not_create_destination_folders(self):
        nested = self.source / "a" / "b"
        nested.mkdir(parents=True)
        (nested / "large").write_bytes(b"x" * 2_000_000)
        self.run_copy("--batch-gb", "0.001")
        self.assertEqual(list(self.destination.iterdir()), [])
        self.assertEqual((self.staging / "a" / "b" / "large").stat().st_size, 1_000_000)
        self.run_copy()
        self.assertEqual((self.destination / "a" / "b" / "large").stat().st_size, 2_000_000)
        # Published partials do not leave empty staging folders behind.
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_empty_folders_are_not_recreated_after_offloading(self):
        (self.source / "empty" / "nested-empty").mkdir(parents=True)
        self.run_copy()
        self.assertTrue((self.destination / "empty" / "nested-empty").is_dir())
        shutil.rmtree(self.destination / "empty")
        self.run_copy()
        self.assertFalse((self.destination / "empty").exists())

    def test_unrecognized_staging_file_is_kept_and_run_continues(self):
        target = self.seed_ready(b"durable success", status="done", linked=True)
        target.unlink()
        target.write_text("not copycat's file")
        (self.source / "other").write_text("other")
        result = self.run_copy()
        self.assertIn("staging cleanup failed", result.stdout)
        self.assertEqual(target.read_text(), "not copycat's file")
        self.assertEqual((self.destination / "other").read_text(), "other")

    def test_missing_hard_link_support_fails_before_copying(self):
        self.staging.mkdir()
        self.destination.mkdir()
        messages = []

        class Connection:
            def send(self, event):
                messages.append(event)

            def close(self):
                pass

        unsupported = PermissionError(1, "Operation not permitted")
        with patch.object(copycat.signal, "signal"), patch.object(copycat.os, "link",
                                                                  side_effect=unsupported):
            copycat.worker(Connection(), "check_link", (str(self.staging), str(self.destination)))
        self.assertTrue(any(event[0] == "error" and "hard links" in event[1]
                            for event in messages))
        self.assertEqual(list(self.staging.iterdir()), [])
        self.assertEqual(list(self.destination.iterdir()), [])

    def exit_signal_and_resume(self, sig):
        source_file = self.source / "large"
        with source_file.open("wb") as stream:
            stream.truncate(64 * 1024 * 1024)
        process = subprocess.Popen(self.command(), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        try:
            for line in process.stdout:
                if "large: 1,048,576/" in line:
                    process.send_signal(sig)
                    break
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 130, stdout + stderr)
            self.assertEqual(self.report()["stop_reason"], "user_exit")
            self.run_copy()
            self.assertEqual(self.report()["counts"], {"done": 1})
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_terminal_close_exits_and_new_process_resumes(self):
        self.exit_signal_and_resume(signal.SIGHUP)

    def test_shutdown_signal_exits_and_new_process_resumes(self):
        self.exit_signal_and_resume(signal.SIGTERM)

    def test_ctrl_c_writes_report(self):
        # A large healthy file gives us a chance to pause during normal copying.
        source_file = self.source / "large"
        with source_file.open("wb") as stream:
            stream.truncate(128 * 1024 * 1024)
        process = subprocess.Popen(self.command(), stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        try:
            for line in process.stdout:
                if line.startswith("COPY large"):
                    process.send_signal(signal.SIGINT)
                    break
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 130, stdout + stderr)
            self.assertTrue(self.report()["interrupted"])
            self.run_copy()
            self.assertEqual((self.destination / "large").stat().st_size, source_file.stat().st_size)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    unittest.main()
