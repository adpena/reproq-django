"""Native Django command precedence without starting processes or opening a DB."""
from io import StringIO
import builtins
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.test import override_settings
import pytest

from reproq_django.management.commands import reproq


@pytest.fixture
def foreground(tmp_path, monkeypatch):
    config = tmp_path / "reproq.toml"
    config.write_text('[worker]\nconcurrency = 1\nqueues = ["default"]\n[beat]\ninterval = "30s"\n')
    dsn = "postgresql://owned:owned@127.0.0.1:1/never_connect"
    execute = Mock()
    discovery = Mock(return_value="")

    def refuse_database(*args, **kwargs):
        pytest.fail("Option resolution must not access a database")

    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", refuse_database)
    monkeypatch.setattr(reproq.Command, "get_dsn", lambda self, alias=None: dsn)
    monkeypatch.setattr(reproq.Command, "_resolve_worker_bin", lambda self: ("owned-reproq", "/owned/reproq", True))
    monkeypatch.setattr(reproq.Command, "_find_default_config", discovery)
    monkeypatch.setattr(reproq.Command, "_compute_allowed_task_modules", lambda self: (["owned_tasks."], [], []))
    monkeypatch.setattr(reproq, "require_worker_capabilities", Mock())
    monkeypatch.setattr(reproq.os, "execvpe", execute)
    monkeypatch.setattr(reproq.connections, "close_all", Mock())
    monkeypatch.setattr(sys, "argv", ["embedded-controller"])

    def invoke(subcommand="worker", *args, **options):
        execute.reset_mock()
        output = StringIO()
        call_command("reproq", subcommand, *args, stdout=output, stderr=StringIO(), **options)
        execute.assert_called_once()
        return SimpleNamespace(argv=execute.call_args.args[1], environment=execute.call_args.args[2],
                               output=output.getvalue())

    environment = {"REPROQ_CONFIG": str(config), "REPROQ_DATABASE_URL": dsn,
                   "ALLOWED_TASK_MODULES": "owned_tasks"}
    with patch.dict(os.environ, environment, clear=True):
        yield SimpleNamespace(invoke=invoke, config=config, execute=execute, discovery=discovery, dsn=dsn)


@pytest.mark.parametrize("form", ["kwargs", "cli"])
@pytest.mark.parametrize("global_args", [[], ["--concurrency=99", "--interval=99s"]])
@pytest.mark.parametrize(("subcommand", "option", "value"), [
    ("worker", "concurrency", 5),
    ("worker", "queues", "special"),
    ("worker", "metrics_auth_token", "owned-synthetic-token"),
    ("worker", "metrics_auth_token", ""),
    ("worker", "logs_dir", ""),
    ("worker", "metrics_port", 0),
    ("beat", "interval", "5s"),
])
def test_actual_call_options_override_config_independently_of_process_argv(
    foreground, monkeypatch, form, global_args, subcommand, option, value,
):
    monkeypatch.setattr(sys, "argv", ["host-process", *global_args])
    flag = "--" + option.replace("_", "-")
    with override_settings(REPROQ_QUEUE_DATABASES={"default": "default", "special": "default"}):
        result = (foreground.invoke(subcommand, **{option: value}) if form == "kwargs"
                  else foreground.invoke(subcommand, flag, str(value)))
    assert result.argv[:4] == ["owned-reproq", subcommand, "--config", str(foreground.config)]
    assert result.argv[result.argv.index(flag) + 1] == str(value)
    assert result.environment["REPROQ_DATABASE_URL"] == foreground.dsn


@pytest.mark.parametrize("subcommand", ["worker", "beat"])
def test_foreign_host_flags_do_not_invent_command_overrides(foreground, monkeypatch, subcommand):
    monkeypatch.setattr(sys, "argv", ["host-process", "--concurrency=99", "--interval=99s"])
    result = foreground.invoke(subcommand)
    assert result.argv == ["owned-reproq", subcommand, "--config", str(foreground.config)]


@pytest.mark.parametrize("form", ["kwargs", "cli"])
def test_default_config_is_resolved_before_explicit_overrides(foreground, monkeypatch, form):
    monkeypatch.delenv("REPROQ_CONFIG")
    foreground.discovery.return_value = str(foreground.config)
    result = foreground.invoke(concurrency=5) if form == "kwargs" else foreground.invoke("worker", "--concurrency", "5")
    assert result.argv == ["owned-reproq", "worker", "--config", str(foreground.config), "--concurrency", "5"]
    assert f"Config: {foreground.config}" in result.output


def test_schedule_implied_once_preserves_default_config_discovery(foreground, monkeypatch):
    monkeypatch.delenv("REPROQ_CONFIG")
    foreground.discovery.return_value = str(foreground.config)
    result = foreground.invoke("schedule")
    assert result.argv == ["owned-reproq", "beat", "--config", str(foreground.config), "--once"]


@pytest.mark.parametrize("once", [True, False])
def test_explicit_beat_once_is_not_host_argv_dependent(foreground, once):
    result = foreground.invoke("beat", once=once)
    assert ("--once" in result.argv) is once


def test_no_file_keeps_go_environment_and_defaults_without_shadow_flags(foreground, monkeypatch):
    monkeypatch.delenv("REPROQ_CONFIG")
    monkeypatch.setenv("QUEUE_NAMES", "special")
    with override_settings(REPROQ_QUEUE_DATABASES={"special": "default", "default": "other"}):
        result = foreground.invoke()
    assert result.argv == ["owned-reproq", "worker"]
    assert result.environment["QUEUE_NAMES"] == "special"


@pytest.mark.parametrize("queue_value", ["", " , "])
def test_explicit_empty_queue_list_uses_go_default_in_route_preflight(foreground, queue_value):
    with override_settings(REPROQ_QUEUE_DATABASES={"default": "other"}):
        with pytest.raises(CommandError, match="queues do not route"):
            foreground.invoke(queues=queue_value)
    foreground.execute.assert_not_called()
    with override_settings(REPROQ_QUEUE_DATABASES={"default": "default"}):
        result = foreground.invoke(queues=queue_value)
    assert result.argv[-2:] == ["--queues", queue_value]


def test_environment_queue_override_matches_route_preflight_and_child(foreground, monkeypatch):
    monkeypatch.setenv("QUEUE_NAMES", "special")
    with override_settings(REPROQ_QUEUE_DATABASES={"special": "default", "default": "other"}):
        result = foreground.invoke()
    assert "--queues" not in result.argv
    assert result.environment["QUEUE_NAMES"] == "special"


def test_whitespace_environment_queue_list_matches_go_default(foreground, monkeypatch):
    foreground.config.write_text('[worker]\nqueues = ["special"]\n')
    monkeypatch.setenv("QUEUE_NAMES", " , ")
    with override_settings(REPROQ_QUEUE_DATABASES={"special": "other", "default": "default"}):
        result = foreground.invoke()
    assert result.environment["QUEUE_NAMES"] == " , "
    with override_settings(REPROQ_QUEUE_DATABASES={"special": "default", "default": "other"}):
        with pytest.raises(CommandError, match="queues do not route"):
            foreground.invoke()
    foreground.execute.assert_not_called()


@pytest.mark.parametrize("form", ["kwargs", "cli"])
def test_discovered_config_queues_are_validated_before_foreground_exec(foreground, monkeypatch, form):
    monkeypatch.delenv("REPROQ_CONFIG")
    foreground.discovery.return_value = str(foreground.config)
    with override_settings(REPROQ_QUEUE_DATABASES={"default": "other"}):
        with pytest.raises(CommandError, match="queues do not route"):
            if form == "kwargs":
                foreground.invoke(concurrency=5)
            else:
                foreground.invoke("worker", "--concurrency", "5")
    foreground.execute.assert_not_called()


def test_discovered_config_dsn_is_validated_before_foreground_exec(foreground, monkeypatch):
    monkeypatch.delenv("REPROQ_CONFIG")
    monkeypatch.delenv("REPROQ_DATABASE_URL")
    foreground.discovery.return_value = str(foreground.config)
    foreground.config.write_text('dsn = "postgresql://owned:owned@127.0.0.1:1/wrong_target"\n')
    with pytest.raises(CommandError, match="DSN does not match"):
        foreground.invoke(concurrency=5)
    foreground.execute.assert_not_called()


def test_explicit_queue_and_database_override_config_routing(foreground):
    with override_settings(REPROQ_QUEUE_DATABASES={"default": "default", "special": "other"}):
        result = foreground.invoke(queues="special", database="other")
    assert result.argv[-2:] == ["--queues", "special"]


@pytest.mark.parametrize("form", ["kwargs", "cli"])
def test_retired_worker_queue_alias_is_rejected(foreground, form):
    if form == "kwargs":
        # Django also sees queue as another subcommand's required positional
        # destination. Depending on its version, native option rejection occurs
        # before this command's handler. Never add an alias to work around it.
        with pytest.raises((CommandError, TypeError, ValueError)):
            foreground.invoke(queue="special")
    else:
        with pytest.raises(CommandError, match="unrecognized arguments"):
            foreground.invoke("worker", "--queue", "special")
    foreground.execute.assert_not_called()


def test_queue_control_positional_queue_is_preserved(foreground, monkeypatch):
    paused = Mock()
    monkeypatch.setattr(reproq.Command, "run_pause_queue", paused)
    call_command("reproq", "pause-queue", "special", stdout=StringIO())
    paused.assert_called_once()
    assert paused.call_args.args[0]["queue"] == "special"


@pytest.mark.parametrize("config_format", [None, "yaml"])
def test_init_default_config_can_be_read_without_optional_yaml(
    foreground, tmp_path, monkeypatch, config_format,
):
    project = tmp_path / "owned-project"
    project.mkdir()
    monkeypatch.chdir(project)
    original_import = builtins.__import__

    def without_yaml(name, *args, **kwargs):
        if name == "yaml":
            raise ImportError("Optional YAML package deliberately unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_yaml)
    monkeypatch.setattr(reproq.Command, "run_install", lambda *args: pytest.fail("Init installed a worker"))
    monkeypatch.setattr(reproq.Command, "run_migrate", lambda *args: pytest.fail("Init ran migrations"))
    options = {} if config_format is None else {"format": config_format}
    call_command("reproq", "init", skip_install=True, skip_worker_migrate=True,
                 skip_migrate=True, stdout=StringIO(), stderr=StringIO(), **options)
    if config_format is None:
        assert sorted(path.name for path in project.iterdir()) == ["reproq.toml"]
        data, actual_format = reproq.Command()._load_config_file(str(project / "reproq.toml"))
        assert actual_format == "toml"
        assert "default" in data["worker"]["queues"]
    else:
        assert sorted(path.name for path in project.iterdir()) == ["reproq.yaml"]
        with pytest.raises(CommandError, match="PyYAML is required"):
            reproq.Command()._load_config_file(str(project / "reproq.yaml"))


def test_init_ignores_shadow_template_and_uses_current_bundled_contract(foreground, tmp_path, monkeypatch):
    project = tmp_path / "shadow-project"
    project.mkdir()
    monkeypatch.chdir(project)
    (project / "reproq.example.toml").write_text('[worker]\nmax_attempts_default = 999\n')
    call_command("reproq", "init", skip_install=True, skip_worker_migrate=True,
                 skip_migrate=True, stdout=StringIO(), stderr=StringIO())
    data, actual_format = reproq.Command()._load_config_file(str(project / "reproq.toml"))
    assert actual_format == "toml"
    assert "max_attempts_default" not in data["worker"]
    assert "default" in data["worker"]["queues"]


def test_missing_bundled_template_fails_before_writing_config(foreground, tmp_path, monkeypatch):
    project = tmp_path / "missing-resource-project"
    project.mkdir()
    monkeypatch.chdir(project)

    def missing_resource(*args, **kwargs):
        raise FileNotFoundError("Owned missing package resource")

    monkeypatch.setattr(reproq.importlib.resources, "files", missing_resource)
    with pytest.raises(CommandError, match="Bundled ReproQ toml configuration template is unavailable"):
        call_command("reproq", "init", skip_install=True, skip_worker_migrate=True,
                     skip_migrate=True, stdout=StringIO(), stderr=StringIO())
    assert not (project / "reproq.toml").exists()


def test_default_init_worker_derives_selected_django_dsn(foreground, tmp_path, monkeypatch):
    project = tmp_path / "init-worker-project"
    project.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.delenv("REPROQ_CONFIG")
    monkeypatch.delenv("REPROQ_DATABASE_URL")
    call_command("reproq", "init", skip_install=True, skip_worker_migrate=True,
                 skip_migrate=True, stdout=StringIO(), stderr=StringIO())
    generated = project / "reproq.toml"
    data, _ = reproq.Command()._load_config_file(str(generated))
    assert data["dsn"] == ""
    foreground.discovery.return_value = str(generated)
    with override_settings(REPROQ_QUEUE_DATABASES={"default": "default", "high": "default"}):
        result = foreground.invoke()
    assert result.argv == ["owned-reproq", "worker", "--config", str(generated)]
    assert result.environment["REPROQ_DATABASE_URL"] == foreground.dsn
    output = StringIO()
    call_command("reproq", "config", mode="all", explain=True, stdout=output)
    for name in ["dsn", "worker.dsn", "beat.dsn"]:
        assert f"{name}: " in output.getvalue()
        assert next(line for line in output.getvalue().splitlines() if line.startswith(name + ": ")).endswith("(settings)")


@pytest.mark.parametrize("scope", ["global", "worker", "beat"])
def test_genuine_wrong_operator_dsn_is_not_replaced(foreground, monkeypatch, scope):
    monkeypatch.delenv("REPROQ_DATABASE_URL")
    wrong = "postgresql://owned:owned@127.0.0.1:1/wrong_target"
    foreground.config.write_text(('dsn = "' + wrong + '"\n') if scope == "global"
                                 else '[' + scope + ']\ndsn = "' + wrong + '"\n')
    with pytest.raises(CommandError, match="DSN does not match"):
        foreground.invoke("beat" if scope == "beat" else "worker")
    foreground.execute.assert_not_called()


def test_no_file_genuine_queue_environment_dsn_is_not_replaced(foreground, monkeypatch):
    monkeypatch.delenv("REPROQ_CONFIG")
    monkeypatch.setenv("REPROQ_DATABASE_URL", "postgresql://owned:owned@127.0.0.1:1/wrong_target")
    with pytest.raises(CommandError, match="DSN does not match"):
        foreground.invoke()
    foreground.execute.assert_not_called()


def test_config_explain_preserves_genuine_global_scoped_and_inherited_provenance(foreground, monkeypatch):
    monkeypatch.delenv("REPROQ_DATABASE_URL")
    foreground.config.write_text('dsn = "postgresql://owned:owned@127.0.0.1:1/global_db"\n'
                                 '[worker]\ndsn = "postgresql://owned:owned@127.0.0.1:1/queue_db"\n')
    output = StringIO()
    call_command("reproq", "config", mode="all", explain=True, stdout=output)
    text = output.getvalue()
    data, _ = json.JSONDecoder().raw_decode(text[text.index("{"):])
    assert "global_db" in data["dsn"] and "global_db" in data["beat"]["dsn"]
    assert "queue_db" in data["worker"]["dsn"]
    for name in ["dsn", "worker.dsn", "beat.dsn"]:
        assert next(line for line in text.splitlines() if line.startswith(name + ": ")).endswith("(config)")


@pytest.mark.parametrize(("filename", "config_format"), [
    ("custom.yaml", None), ("custom.yml", None), ("custom.toml", "yaml"),
    ("custom.cfg", None), ("custom.cfg", "yaml"),
])
def test_init_rejects_format_suffix_mismatch_before_any_bootstrap_side_effect(
    foreground, tmp_path, monkeypatch, filename, config_format,
):
    parent = tmp_path / "must-not-be-created"
    target = parent / filename
    install, migrate = Mock(), Mock()
    monkeypatch.setattr(reproq.Command, "run_install", install)
    monkeypatch.setattr(reproq.Command, "run_migrate", migrate)
    options = {} if config_format is None else {"format": config_format}
    with pytest.raises(CommandError, match="Config filename must end.*--format"):
        call_command("reproq", "init", config=str(target), stdout=StringIO(), stderr=StringIO(), **options)
    assert not parent.exists()
    install.assert_not_called()
    migrate.assert_not_called()


@pytest.mark.parametrize(("filename", "config_format"), [
    ("custom.toml", None), ("custom.TOML", "toml"),
    ("custom.yaml", "yaml"), ("custom.YML", "yaml"),
])
def test_init_accepts_reader_supported_filename_suffix(foreground, tmp_path, filename, config_format):
    target = tmp_path / filename
    options = {} if config_format is None else {"format": config_format}
    call_command("reproq", "init", config=str(target), skip_install=True,
                 skip_worker_migrate=True, skip_migrate=True,
                 stdout=StringIO(), stderr=StringIO(), **options)
    assert target.is_file()
    if config_format != "yaml":
        data, actual_format = reproq.Command()._load_config_file(str(target))
        assert actual_format == "toml" and "worker" in data


def test_init_rejects_existing_directory_before_bootstrap(foreground, tmp_path, monkeypatch):
    target = tmp_path / "directory.toml"
    target.mkdir()
    install, migrate = Mock(), Mock()
    monkeypatch.setattr(reproq.Command, "run_install", install)
    monkeypatch.setattr(reproq.Command, "run_migrate", migrate)
    with pytest.raises(CommandError, match="Config path must name a file"):
        call_command("reproq", "init", config=str(target), stdout=StringIO(), stderr=StringIO())
    assert not list(target.iterdir())
    install.assert_not_called()
    migrate.assert_not_called()
