"""Immutable v1 filesystem journal writer for explicit schema adoption.

A database and filesystem cannot share a commit. Callers must distinguish
prepared, unconfirmed and commit_unknown outcomes and independently verify
history after interruptions. Files are fsynced and atomically replaced on the
same filesystem. sync_directory_v1 returns False where directory fsync is not
supported (including Windows); this is not a cross-platform durability promise.
"""

import errno
import json
import os
import tempfile


def sync_directory_v1(path):
    if os.name == "nt":
        return False
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if exc.errno in {errno.EINVAL, errno.ENOTSUP}:
                return False
            raise
    finally:
        os.close(descriptor)
    return True


def write_receipt_v1(path, payload, *, exclusive=False):
    if exclusive:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, sort_keys=True, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        sync_directory_v1(path.parent)
        return
    descriptor, temporary = tempfile.mkstemp(prefix=".reproq-adoption-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, sort_keys=True, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory_v1(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
