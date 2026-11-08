import json
import os
from pathlib import Path
import subprocess
import sys

from django.core.management.base import CommandError
import pytest

from reproq_django.management.commands.reproq import Command


def generate_units(**options):
    Command().run_systemd({"user": "test", "group": "test", "concurrency": 1, "schedule": True, **options})


def directive(path, name):
    return next(line.removeprefix(f"{name}=") for line in path.read_text().splitlines()
                if line.startswith(f"{name}="))


def quoted_words(line):
    """Read the quoted tokens emitted by the generator, without shell parsing."""
    decoder = json.JSONDecoder()
    words = []
    while line:
        item, end = decoder.raw_decode(line)
        words.append(item)
        line = line[end:].lstrip()
    return words


@pytest.mark.skipif(os.name != "posix", reason="Linux unit paths use POSIX syntax")
@pytest.mark.parametrize("relative_env_file", [True, False])
def test_systemd_execstart_keeps_project_and_environment_values_literal(tmp_path, monkeypatch, relative_env_file):
    project = tmp_path / "parent ' %l $HOME=literal" / "research-app"
    project.mkdir(parents=True)
    monkeypatch.chdir(project)
    project = Path.cwd()
    interpreter = str(project / "Python runner")
    Path(interpreter).symlink_to(sys.executable)
    (project / "manage.py").write_text("import json, sys; print(json.dumps(sys.argv[1:]))\n")
    monkeypatch.setattr(sys, "executable", interpreter)
    env_file = project / 'metrics "prod".env'
    logs_dir = str(project / "logs")
    token = r'example\token"%l$HOME'
    generate_units(env_file=env_file.name if relative_env_file else str(env_file),
                   logs_dir=logs_dir, metrics_auth_token=token)

    for role in ("worker", "beat", "schedule"):
        unit = project / f"reproq-{role}-research-app.service"
        expected = [":/bin/sh", "-c", 'exec "$@"', "reproq", interpreter, str(project / "manage.py"), "reproq", role]
        if role == "worker":
            expected.extend(["--concurrency", "1", "--logs-dir", logs_dir])
        argv = quoted_words(directive(unit, "ExecStart"))
        assert argv == [value.replace("%", "%%") for value in expected]
        parsed = [argv[0].removeprefix(":"), *(value.replace("%%", "%") for value in argv[1:])]
        completed = subprocess.run(parsed, capture_output=True, text=True, timeout=5, check=True)
        assert json.loads(completed.stdout) == expected[6:]
        assert directive(unit, "WorkingDirectory") == str(project).replace("%", "%%")
        assert directive(unit, "EnvironmentFile") == str(env_file).replace("%", "%%")
        assert json.loads(directive(unit, "Environment")) == "METRICS_AUTH_TOKEN=" + token.replace("%", "%%")
        assert ("Restart=always" in unit.read_text()) == (role != "schedule")


@pytest.mark.skipif(os.name != "posix", reason="The generated condition uses a POSIX shell")
@pytest.mark.parametrize(("mode", "expected"), [(None, "beat"), ("", "beat"), ("beat", "beat"),
                                                ("cron", "schedule"), ("off", None), ("pg_cron", None),
                                                ("private-scheduler-canary", None)])
def test_systemd_conditions_select_one_scheduler_and_keep_mode_private(tmp_path, monkeypatch, mode, expected):
    monkeypatch.chdir(tmp_path)
    generate_units()
    env = dict(os.environ)
    if mode is None:
        env.pop("REPROQ_SCHEDULER_MODE", None)
    else:
        env["REPROQ_SCHEDULER_MODE"] = mode
    for role, label in (("beat", "beat"), ("schedule", "cron")):
        unit, = tmp_path.glob(f"reproq-{role}-*.service")
        executable, *arguments = quoted_words(directive(unit, "ExecCondition"))
        assert executable == ":/bin/sh"
        completed = subprocess.run([executable[1:], *arguments], env=env, capture_output=True,
                                   text=True, timeout=5, check=False)
        assert completed.returncode == (0 if role == expected else 1)
        assert not completed.stderr
        assert completed.stdout == ("" if role == expected else f"Scheduler mode does not select reproq {label}.\n")


@pytest.mark.parametrize(("option", "value"), [("user", "test\nExecStart=/bin/false"), ("group", "test\r"),
                                               ("schedule_on_calendar", "daily\nPersistent=false"),
                                               ("env_file", "/srv/unsafe\\"), ("env_file", "/srv/unsafe\nType=simple"),
                                               ("metrics_auth_token", "private\x00value"),
                                               ("metrics_allow_cidrs", "example\rvalue"),
                                               ("logs_dir", "/srv/log\n[Service]")])
def test_systemd_rejects_input_that_cannot_be_written_safely(tmp_path, monkeypatch, option, value):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(CommandError, match="cannot") as caught:
        generate_units(**{option: value})
    assert value not in str(caught.value)
    assert not list(tmp_path.glob("reproq-*"))


def test_systemd_rejects_invalid_unit_basename(tmp_path, monkeypatch):
    project = tmp_path / "research app"
    project.mkdir()
    monkeypatch.chdir(project)
    with pytest.raises(CommandError, match="systemd unit filename"):
        generate_units()
    assert not list(project.glob("reproq-*"))


@pytest.mark.parametrize("path", ["conf/*.env", "conf/?.env", "conf/[a].env", r"conf/back\slash.env", "-/srv/app.env"])
def test_systemd_env_file_must_name_one_required_file(tmp_path, monkeypatch, path):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(CommandError, match="exact path"):
        generate_units(env_file=path)
    assert not list(tmp_path.glob("reproq-*"))
