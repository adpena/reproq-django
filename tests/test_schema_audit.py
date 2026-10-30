"""Actual filesystem failure probes for the shared schema journal."""

import errno
import json
import os

import pytest

from reproq_django import schema_audit


def test_exclusive_receipt_cannot_replace_an_existing_operator_file(tmp_path):
    path = tmp_path / "receipt.json"
    path.write_text("operator-owned\n")
    with pytest.raises(FileExistsError):
        schema_audit.write_receipt_v1(path, {"state": "prepared"}, exclusive=True)
    assert path.read_text() == "operator-owned\n"


def test_prepared_then_committed_receipt_is_complete_and_private(tmp_path):
    path = tmp_path / "receipt.json"
    schema_audit.write_receipt_v1(path, {"state": "prepared", "value": "résumé"}, exclusive=True)
    assert json.loads(path.read_text())["state"] == "prepared"
    schema_audit.write_receipt_v1(path, {"state": "committed", "value": "résumé"})
    assert json.loads(path.read_text()) == {"state": "committed", "value": "résumé"}
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [path]


def test_interrupted_atomic_replace_keeps_prepared_receipt(tmp_path, monkeypatch):
    path = tmp_path / "receipt.json"
    schema_audit.write_receipt_v1(path, {"state": "prepared"}, exclusive=True)
    prior = path.read_bytes()

    def interrupted(*args):
        raise OSError("injected interruption before replace")

    monkeypatch.setattr(schema_audit.os, "replace", interrupted)
    with pytest.raises(OSError, match="injected interruption"):
        schema_audit.write_receipt_v1(path, {"state": "committed"})
    assert path.read_bytes() == prior
    assert list(tmp_path.iterdir()) == [path]


def test_lost_replace_ack_retains_complete_new_state(tmp_path, monkeypatch):
    path = tmp_path / "receipt.json"
    schema_audit.write_receipt_v1(path, {"state": "prepared"}, exclusive=True)
    replace = schema_audit.os.replace

    def replace_then_lose_ack(*args):
        replace(*args)
        raise OSError("injected lost replace acknowledgement")

    monkeypatch.setattr(schema_audit.os, "replace", replace_then_lose_ack)
    with pytest.raises(OSError, match="lost replace acknowledgement"):
        schema_audit.write_receipt_v1(path, {"state": "committed"})
    assert json.loads(path.read_text())["state"] == "committed"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("error", [errno.EINVAL, errno.ENOTSUP])
def test_unsupported_directory_fsync_is_reported_not_assumed(tmp_path, monkeypatch, error):
    def unsupported(descriptor):
        raise OSError(error, "owned unsupported-directory-fsync probe")

    monkeypatch.setattr(schema_audit.os, "fsync", unsupported)
    assert schema_audit.sync_directory_v1(tmp_path) is False


def test_other_directory_fsync_errors_propagate(tmp_path, monkeypatch):
    if os.name == "nt":
        pytest.skip("Windows does not attempt directory fsync")

    def failed(descriptor):
        raise OSError(errno.EIO, "owned fsync IO failure")

    monkeypatch.setattr(schema_audit.os, "fsync", failed)
    with pytest.raises(OSError, match="owned fsync IO failure"):
        schema_audit.sync_directory_v1(tmp_path)
