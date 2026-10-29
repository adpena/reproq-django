import hashlib
import io
import json
from pathlib import Path
from urllib.error import HTTPError

import pytest
from django.core.management.base import CommandError

from reproq_django.management.commands import reproq


BINARY = b"verified worker fixture"
DIGEST = hashlib.sha256(BINARY).hexdigest()


@pytest.fixture
def installation(tmp_path, settings, monkeypatch):
    target = tmp_path / "bin" / "reproq"
    target.parent.mkdir()
    target.write_bytes(b"previous worker")
    settings.REPROQ_WORKER_BIN = str(target)
    monkeypatch.setattr(reproq.platform, "system", lambda: "Linux")
    monkeypatch.setattr(reproq.platform, "machine", lambda: "x86_64")
    executions, builds = [], []

    def execute(binary):
        assert hashlib.sha256(Path(binary).read_bytes()).hexdigest() == DIGEST
        assert target.read_bytes() == b"previous worker"
        executions.append([binary, 'capabilities'])
        return json.dumps({"worker_version": "owned-unreleased", "schema_id": "reproq-schema-v2",
                           "schema_version": 2, "executor_protocol": 2}).encode()

    def build(argv, **kwargs):
        builds.append((argv, kwargs))
        Path(argv[argv.index("-o") + 1]).write_bytes(BINARY)

    def canonical_build(source, destination, **kwargs):
        builds.append((source, kwargs))
        Path(destination).write_bytes(BINARY)
        return DIGEST

    monkeypatch.setattr('reproq_django.capabilities._capture_worker_capabilities', execute)
    monkeypatch.setattr(reproq.subprocess, "run", build)
    monkeypatch.setattr("reproq_django.worker_install.build_verified_source", canonical_build)
    return target, executions, builds


def serve(monkeypatch, *, binary=BINARY, sidecar=None, document=None, download_error=None):
    requests = []

    def open_url(request, **kwargs):
        url = request.full_url if isinstance(request, reproq.urllib.request.Request) else request
        requests.append(url)
        assert kwargs == {"timeout": 30}
        if url.startswith("https://api.github.com/"):
            if isinstance(document, Exception):
                raise document
            return io.BytesIO(json.dumps(document).encode())
        if url.endswith(".sha256"):
            if sidecar is None:
                raise HTTPError(url, 404, "Not found", {}, None)
            return io.BytesIO(sidecar)
        if download_error:
            raise download_error
        return io.BytesIO(binary)

    monkeypatch.setattr(reproq.urllib.request, "urlopen", open_url)
    return requests


def assert_preserved(installation):
    target, executions, builds = installation
    assert target.read_bytes() == b"previous worker"
    assert not executions
    assert not builds
    assert list(target.parent.glob(".reproq-install-*")) == []


def test_explicit_pin_is_checked_before_execution_and_atomic_publication(installation, monkeypatch):
    requests = serve(monkeypatch)
    reproq.Command().run_install({"tag": "v0.0.144", "sha256": DIGEST.upper()})
    target, executions, builds = installation
    assert target.read_bytes() == BINARY
    assert len(executions) == 1
    assert Path(executions[0][0]).parent == target.parent
    assert not builds
    assert len(requests) == 1  # Explicit pins do not depend on mutable checksum metadata.
    assert "/download/v0.0.144/reproq-linux-amd64" in requests[0]
    assert list(target.parent.glob(".reproq-install-*")) == []


def test_management_install_does_not_open_a_database(installation, monkeypatch):
    from django.core.management import call_command
    from django.db.backends.base.base import BaseDatabaseWrapper

    def forbidden_connection(*args, **kwargs):
        raise AssertionError("Worker installation must not open a database")

    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbidden_connection)
    serve(monkeypatch)
    call_command("reproq", "install", sha256=DIGEST, skip_checks=False, stdout=io.StringIO())
    assert installation[0].read_bytes() == BINARY
    assert len(installation[1]) == 1
    assert not installation[2]


def test_install_check_selection_preserves_native_operational_checks():
    from django.core.management.base import BaseCommand

    command = reproq.Command()
    for subcommand in ("worker", "beat", "schedule", "doctor", "pg-cron", "migrate-worker", "init"):
        options = {"subcommand": subcommand}
        assert command.get_check_kwargs(options) == BaseCommand.get_check_kwargs(command, options)


@pytest.mark.parametrize("digest", ["", "0" * 63, "g" * 64, " " + DIGEST, None])
def test_invalid_explicit_pin_stops_before_network(installation, monkeypatch, digest):
    if digest is None:
        digest = 123
    requests = serve(monkeypatch)
    with pytest.raises(CommandError, match="64-character"):
        reproq.Command().run_install({"sha256": digest})
    assert not requests
    assert_preserved(installation)


def test_digest_mismatch_cannot_execute_or_fall_back(installation, monkeypatch, tmp_path):
    serve(monkeypatch, binary=b"wrong bytes")
    with pytest.raises(CommandError, match="SHA-256 mismatch"):
        reproq.Command().run_install({"sha256": DIGEST, "source": str(tmp_path)})
    assert_preserved(installation)


def test_pinned_download_error_cannot_fall_back_to_existing_source(installation, monkeypatch, tmp_path):
    serve(monkeypatch, download_error=OSError("offline"))
    with pytest.raises(CommandError, match="fallback is disabled"):
        reproq.Command().run_install({"sha256": DIGEST, "source": str(tmp_path)})
    assert_preserved(installation)


def test_default_install_uses_official_release_digest_without_sidecar(installation, monkeypatch):
    requests = serve(monkeypatch, document={"assets": [
        {"name": "reproq-linux-amd64", "digest": "sha256:" + DIGEST},
        {"name": "reproq-darwin-arm64", "digest": "sha256:" + "0" * 64},
    ]})
    reproq.Command().run_install({"tag": "v0.0.144"})
    assert installation[0].read_bytes() == BINARY
    assert len(installation[1]) == 1
    assert requests[-1] == "https://api.github.com/repos/adpena/reproq-worker/releases/tags/v0.0.144"


@pytest.mark.parametrize("document", [
    {"assets": []},
    {"assets": [{"name": "reproq-linux-amd64", "digest": None}]},
    {"assets": [{"name": "reproq-linux-amd64", "digest": "md5:" + DIGEST}]},
    {"assets": [{"name": "reproq-linux-amd64", "digest": "sha256:invalid"}]},
    {"assets": [{"name": "reproq-linux-amd64", "digest": "sha256:" + DIGEST}] * 2},
    OSError("metadata unavailable"),
])
def test_missing_or_invalid_release_integrity_fails_closed(installation, monkeypatch, document, tmp_path):
    serve(monkeypatch, document=document)
    with pytest.raises(CommandError, match="Cannot verify"):
        reproq.Command().run_install({"source": str(tmp_path)})
    assert_preserved(installation)


@pytest.mark.parametrize("sidecar", [b"not-a-sha256", ("0" * 64).encode(), b"x" * 4097, b"\xff"])
def test_sidecar_integrity_failure_cannot_fall_back(installation, monkeypatch, sidecar, tmp_path):
    serve(monkeypatch, sidecar=sidecar)
    with pytest.raises(CommandError):
        reproq.Command().run_install({"source": str(tmp_path)})
    assert_preserved(installation)


def test_explicit_source_build_obeys_the_same_digest_before_execution(installation, tmp_path):
    reproq.Command().run_install({"build": True, "source": str(tmp_path), "sha256": DIGEST})
    assert installation[0].read_bytes() == BINARY
    assert len(installation[1]) == len(installation[2]) == 1


def test_failed_version_probe_preserves_target_and_cleans_staging(installation, monkeypatch):
    serve(monkeypatch)
    monkeypatch.setattr('reproq_django.capabilities._capture_worker_capabilities', lambda binary: (_ for _ in ()).throw(OSError("broken")))
    with pytest.raises(CommandError, match="verification or publication"):
        reproq.Command().run_install({"sha256": DIGEST})
    assert_preserved(installation)


@pytest.mark.parametrize("subcommand", ["install", "init", "upgrade"])
def test_every_install_entry_point_accepts_the_same_integrity_arguments(subcommand):
    parser = reproq.Command().create_parser("manage.py", "reproq")
    options = parser.parse_args([subcommand, "--tag", "v0.0.144", "--sha256", DIGEST])
    assert options.sha256 == DIGEST
    assert options.tag == "v0.0.144"


@pytest.mark.parametrize("subcommand", ["install", "init", "upgrade"])
def test_every_install_entry_point_defaults_to_the_sdk_tested_worker(subcommand):
    parser = reproq.Command().create_parser("manage.py", "reproq")
    options = parser.parse_args([subcommand])
    assert options.tag == reproq.DEFAULT_WORKER_TAG == "v0.0.145"


def test_default_download_uses_an_immutable_compatible_release(installation, monkeypatch):
    requests = serve(monkeypatch)
    reproq.Command().run_install({"sha256": DIGEST})
    assert requests == [
        "https://github.com/adpena/reproq-worker/releases/download/v0.0.145/reproq-linux-amd64"
    ]
    assert len(installation[1]) == 1


def test_mutable_latest_is_an_explicit_override(installation, monkeypatch):
    requests = serve(monkeypatch)
    parser = reproq.Command().create_parser("manage.py", "reproq")
    options = vars(parser.parse_args(["install", "--tag", "latest", "--sha256", DIGEST]))
    reproq.Command().run_install(options)
    assert requests == [
        "https://github.com/adpena/reproq-worker/releases/latest/download/reproq-linux-amd64"
    ]
    assert len(installation[1]) == 1


@pytest.mark.parametrize("tag", [None, ""])
def test_invalid_release_tag_preserves_installation_without_network(installation, monkeypatch, tag):
    requests = serve(monkeypatch)
    with pytest.raises(CommandError, match="non-empty string"):
        reproq.Command().run_install({"tag": tag})
    assert requests == []
    assert_preserved(installation)
