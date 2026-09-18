import asyncio
import os
from pathlib import Path
import stat
import tempfile
import textwrap
import unittest

from db_backup_stream import (
    BackupProcessError,
    create_streaming_encrypted_backup,
    redact_backup_process_error,
    verify_pg_dump_without_file,
)


PG_DUMP_SCRIPT = """\
#!/usr/bin/env python3
import os
import sys
import time

mode = os.environ.get("FAKE_PG_DUMP_MODE", "ok")
if mode == "sleep":
    time.sleep(30)
sys.stdout.buffer.write(b"PLAINTEXT DATABASE CONTENT")
sys.stdout.buffer.flush()
if mode == "fail":
    sys.stderr.write("pg dump failed safely")
    sys.exit(7)
"""


OPENSSL_SCRIPT = """\
#!/usr/bin/env python3
import os
from pathlib import Path
import signal
import sys
import time

mode = "fail" if "fail" in Path(sys.argv[0]).name else "ok"
output_path = Path(sys.argv[sys.argv.index("-out") + 1])
if "late-writer" in Path(sys.argv[0]).name:
    def write_during_shutdown(_signum, _frame):
        output_path.write_bytes(b"PARTIAL DURING TERMINATION")
        sys.exit(0)
    signal.signal(signal.SIGTERM, write_during_shutdown)
if mode == "sleep":
    time.sleep(30)
payload = sys.stdin.buffer.read()
output_path.write_bytes(b"ENCRYPTED:" + payload)
if mode == "fail":
    sys.stderr.write("encryption failed safely")
    sys.exit(9)
"""


class BackupStreamTests(unittest.IsolatedAsyncioTestCase):
    def make_executable(self, directory, name, source):
        path = Path(directory) / name
        path.write_text(textwrap.dedent(source), encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return str(path)

    async def test_happy_path_streams_directly_to_restrictive_encrypted_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            pg_dump = self.make_executable(directory, "fake-pg-dump", PG_DUMP_SCRIPT)
            openssl = self.make_executable(directory, "fake-openssl", OPENSSL_SCRIPT)
            output_path = await create_streaming_encrypted_backup(
                [pg_dump], os.environ.copy(), "test-encryption-key",
                temp_dir=directory, openssl_executable=openssl,
            )
            try:
                self.assertEqual(
                    Path(output_path).read_bytes(),
                    b"ENCRYPTED:PLAINTEXT DATABASE CONTENT",
                )
                self.assertEqual(stat.S_IMODE(Path(output_path).stat().st_mode), 0o600)
                self.assertFalse(list(Path(directory).glob("*.sql")))
                self.assertEqual(list(Path(directory).glob("*.enc")), [Path(output_path)])
            finally:
                Path(output_path).unlink(missing_ok=True)

    async def test_pg_dump_failure_removes_partial_encrypted_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            pg_dump = self.make_executable(directory, "fake-pg-dump", PG_DUMP_SCRIPT)
            openssl = self.make_executable(directory, "fake-openssl", OPENSSL_SCRIPT)
            env = {**os.environ, "FAKE_PG_DUMP_MODE": "fail"}
            with self.assertRaisesRegex(BackupProcessError, "pg_dump_failed"):
                await create_streaming_encrypted_backup(
                    [pg_dump], env, "test-encryption-key",
                    temp_dir=directory, openssl_executable=openssl,
                )
            self.assertFalse(list(Path(directory).glob("*.sql")))
            self.assertFalse(list(Path(directory).glob("*.enc")))

    async def test_encryption_failure_removes_partial_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            pg_dump = self.make_executable(directory, "fake-pg-dump", PG_DUMP_SCRIPT)
            openssl = self.make_executable(directory, "fake-openssl-fail", OPENSSL_SCRIPT)
            with self.assertRaisesRegex(BackupProcessError, "encryption_failed"):
                await create_streaming_encrypted_backup(
                    [pg_dump], os.environ.copy(), "test-encryption-key",
                    temp_dir=directory, openssl_executable=openssl,
                )
            self.assertFalse(list(Path(directory).glob("*.sql")))
            self.assertFalse(list(Path(directory).glob("*.enc")))

    def test_process_error_redaction_removes_database_url_and_password(self):
        database_url = "postgresql://backup:db-password@db.example/club"
        stderr = f"connection failed for {database_url}; password=db-password".encode()
        redacted = redact_backup_process_error(stderr, (database_url, "db-password"))
        self.assertNotIn(database_url, redacted)
        self.assertNotIn("db-password", redacted)
        self.assertIn("***", redacted)

    async def test_invalid_key_fails_before_subprocess_or_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "invalid_backup_encryption_key"):
                await create_streaming_encrypted_backup(
                    ["must-not-run"], os.environ.copy(), "   ", temp_dir=directory,
                )
            self.assertEqual(list(Path(directory).iterdir()), [])

    async def test_disabled_delivery_verifies_dump_without_creating_any_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            pg_dump = self.make_executable(directory, "fake-pg-dump", PG_DUMP_SCRIPT)
            await verify_pg_dump_without_file([pg_dump], os.environ.copy())
            self.assertEqual(
                sorted(path.name for path in Path(directory).iterdir()),
                ["fake-pg-dump"],
            )

    async def test_cancellation_terminates_pipeline_and_removes_partial_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            pg_dump = self.make_executable(directory, "fake-pg-dump", PG_DUMP_SCRIPT)
            openssl = self.make_executable(directory, "fake-openssl-late-writer", OPENSSL_SCRIPT)
            env = {**os.environ, "FAKE_PG_DUMP_MODE": "sleep"}
            task = asyncio.create_task(create_streaming_encrypted_backup(
                [pg_dump], env, "test-encryption-key",
                temp_dir=directory, openssl_executable=openssl,
            ))
            await asyncio.sleep(0.1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(list(Path(directory).glob("*.sql")))
            self.assertFalse(list(Path(directory).glob("*.enc")))

    async def test_timeout_terminates_pipeline_and_removes_partial_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            pg_dump = self.make_executable(directory, "fake-pg-dump", PG_DUMP_SCRIPT)
            openssl = self.make_executable(directory, "fake-openssl-late-writer", OPENSSL_SCRIPT)
            env = {**os.environ, "FAKE_PG_DUMP_MODE": "sleep"}
            with self.assertRaisesRegex(BackupProcessError, "timeout_failed"):
                await create_streaming_encrypted_backup(
                    [pg_dump], env, "test-encryption-key",
                    temp_dir=directory, openssl_executable=openssl, timeout=0.05,
                )
            self.assertFalse(list(Path(directory).glob("*.sql")))
            self.assertFalse(list(Path(directory).glob("*.enc")))


if __name__ == "__main__":
    unittest.main()
