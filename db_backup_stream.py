import asyncio
import os
import tempfile


BACKUP_PROCESS_TIMEOUT_SECONDS = 15 * 60


class BackupProcessError(RuntimeError):
    def __init__(self, stage, returncode=None, stderr=b""):
        super().__init__(f"{stage}_failed")
        self.stage = stage
        self.returncode = returncode
        self.stderr = stderr or b""


def redact_backup_process_error(stderr, secret_values=()):
    value = (stderr or b"").decode("utf-8", errors="replace")
    for secret in secret_values:
        if secret:
            value = value.replace(str(secret), "***")
    return value


async def _terminate_process(process):
    if process is None or process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        await process.wait()
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except (asyncio.TimeoutError, ProcessLookupError):
        if process.returncode is None:
            process.kill()
            await process.wait()


async def create_streaming_encrypted_backup(
    pg_dump_argv,
    pg_dump_env,
    encryption_key,
    *,
    temp_dir=None,
    timeout=BACKUP_PROCESS_TIMEOUT_SECONDS,
    openssl_executable="openssl",
):
    if not isinstance(encryption_key, str) or not encryption_key.strip() or "\x00" in encryption_key:
        raise ValueError("invalid_backup_encryption_key")

    output_fd, output_path = tempfile.mkstemp(
        prefix="club-db-backup-",
        suffix=".sql.enc",
        dir=temp_dir,
    )
    os.close(output_fd)
    os.chmod(output_path, 0o600)

    read_fd = None
    write_fd = None
    pg_dump_process = None
    encryption_process = None
    pipeline_task = None
    completed = False
    try:
        read_fd, write_fd = os.pipe()
        encryption_process = await asyncio.create_subprocess_exec(
            openssl_executable,
            "enc", "-aes-256-cbc", "-salt", "-pbkdf2",
            "-pass", "env:BACKUP_ENCRYPTION_KEY",
            "-out", output_path,
            stdin=read_fd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            env={
                key: value
                for key, value in os.environ.items()
                if key in {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL"}
            } | {"BACKUP_ENCRYPTION_KEY": encryption_key},
        )
        os.close(read_fd)
        read_fd = None

        pg_dump_process = await asyncio.create_subprocess_exec(
            *pg_dump_argv,
            stdout=write_fd,
            stderr=asyncio.subprocess.PIPE,
            env=pg_dump_env,
        )
        os.close(write_fd)
        write_fd = None

        pipeline_task = asyncio.gather(
            pg_dump_process.communicate(),
            encryption_process.communicate(),
        )
        (pg_result, encryption_result) = await asyncio.wait_for(
            pipeline_task, timeout=timeout,
        )
        _pg_stdout, pg_stderr = pg_result
        _encryption_stdout, encryption_stderr = encryption_result
        if pg_dump_process.returncode != 0:
            raise BackupProcessError("pg_dump", pg_dump_process.returncode, pg_stderr)
        if encryption_process.returncode != 0:
            raise BackupProcessError(
                "encryption", encryption_process.returncode, encryption_stderr,
            )
        completed = True
        return output_path
    except asyncio.TimeoutError as error:
        raise BackupProcessError("timeout") from error
    finally:
        if pipeline_task is not None and not pipeline_task.done():
            pipeline_task.cancel()
        if pipeline_task is not None:
            try:
                await pipeline_task
            except BaseException:
                pass
        await asyncio.gather(
            _terminate_process(pg_dump_process),
            _terminate_process(encryption_process),
        )
        if read_fd is not None:
            os.close(read_fd)
        if write_fd is not None:
            os.close(write_fd)
        if not completed and os.path.exists(output_path):
            os.remove(output_path)


async def verify_pg_dump_without_file(
    pg_dump_argv,
    pg_dump_env,
    *,
    timeout=BACKUP_PROCESS_TIMEOUT_SECONDS,
):
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *pg_dump_argv,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            env=pg_dump_env,
        )
        _stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        if process.returncode != 0:
            raise BackupProcessError("pg_dump", process.returncode, stderr)
    except asyncio.TimeoutError as error:
        raise BackupProcessError("timeout") from error
    finally:
        await _terminate_process(process)
