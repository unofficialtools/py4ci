"""Unit tests for py4ci. Run with::

    python -m pytest apps/py4ci/tests/test_unit.py

These tests stay away from the real workers — they only exercise the
pure-Python logic: duration parsing, queue matching, callback token
verification, config validation, state-machine transitions, and the tag
attachment helper.
"""

import datetime
import os
import tempfile
import types

import pytest
from pydal import DAL

from ..ci import (
    ALLOWED_TRANSITIONS,
    CI,
    delta,
)


# ---------------------------------------------------------------------------
# delta()
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("0s", 0),
    ("30s", 30),
    ("1m", 60),
    ("2h", 7200),
    ("1d", 86400),
    ("1w", 7 * 86400),
    ("180", 180),                # plain int = seconds
    ("1.5h", 5400),              # decimal
    ("1h30m", 5400),             # compound
    ("1h 30m", 5400),            # compound with whitespace
    (" 5m ", 300),               # surrounding whitespace
])
def test_delta_valid(text, expected):
    assert delta(text) == datetime.timedelta(seconds=expected)


def test_delta_accepts_ints():
    assert delta(45) == datetime.timedelta(seconds=45)
    assert delta(0) == datetime.timedelta(seconds=0)


@pytest.mark.parametrize("text", [
    "",
    "garbage",
    "abc",
    "h",
    "1q",
    "1h foo",
    None,
])
def test_delta_rejects_garbage(text):
    with pytest.raises((ValueError, TypeError)):
        delta(text)


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

def test_state_machine_terminal_only_goes_to_queued():
    for state in ("success", "failure", "broken", "skipped", "stopped", "jammed", "timeout"):
        assert ALLOWED_TRANSITIONS[state] == {"queued"}


def test_state_machine_queued_only_goes_to_skipped_or_starting():
    assert ALLOWED_TRANSITIONS["queued"] == {"queued", "skipped", "starting"}


# ---------------------------------------------------------------------------
# CI helpers — full instance against an in-memory sqlite
# ---------------------------------------------------------------------------

@pytest.fixture
def make_ci(tmp_path):
    """Factory that builds a CI instance pointed at a fresh sqlite db and an
    empty config directory the caller can populate before instantiation."""
    def _factory(config_files=None):
        cfg_dir = tmp_path / "ci_config"
        cfg_dir.mkdir(exist_ok=True)
        for name, body in (config_files or {}).items():
            (cfg_dir / name).write_text(body)
        db = DAL("sqlite:memory", folder=str(tmp_path))
        ci_instance = CI(
            db,
            app_base_url="http://localhost",
            config_path=str(cfg_dir),
        )
        return ci_instance
    return _factory


def test_available_worker_returns_idle_match(make_ci):
    ci_ = make_ci({"workers.toml": """
[workers.w1]
host = "u@a"
queues = ["default"]
[workers.w2]
host = "u@b"
queues = ["fast"]
"""})
    assert ci_.available_worker(["default"]) == "w1"
    assert ci_.available_worker(["fast"]) == "w2"
    assert ci_.available_worker(["nope"]) is None


def test_available_worker_handles_bare_string(make_ci):
    """Regression: when called with a plain string, must not be split into chars."""
    ci_ = make_ci({"workers.toml": """
[workers.w1]
host = "u@a"
queues = ["default"]
"""})
    assert ci_.available_worker("default") == "w1"
    # "default" used to set([d,e,f,a,u,l,t]); ensure the actual queue name still wins
    assert ci_.available_worker("d") is None


def test_post_run_done_requires_token(make_ci):
    ci_ = make_ci()
    db = ci_.db
    run_id = db.task_run.insert(
        name="x", status="started", callback_token="GOOD", start_timestamp=datetime.datetime.utcnow()
    )
    db.commit()
    assert ci_.post_run_done(run_id, "BAD") is False
    assert ci_.post_run_done(run_id, "") is False
    assert ci_.post_run_done(run_id, "GOOD") is True
    # token should be invalidated after a successful callback
    db.commit()
    fresh = db.task_run(run_id)
    assert fresh.callback_token in (None, "")
    assert ci_.post_run_done(run_id, "GOOD") is False


def test_verify_github_signature(make_ci):
    import hashlib, hmac
    ci_ = make_ci()
    ci_.github_webhook_secret = "topsecret"
    body = b'{"hello":"world"}'
    good = "sha256=" + hmac.new(b"topsecret", body, hashlib.sha256).hexdigest()
    assert ci_.verify_github_signature(body, good)
    assert not ci_.verify_github_signature(body, "sha256=" + "0" * 64)
    assert not ci_.verify_github_signature(body, "")


def test_verify_github_signature_no_secret(make_ci):
    ci_ = make_ci()
    ci_.github_webhook_secret = ""
    assert not ci_.verify_github_signature(b"x", "sha256=abc")


def test_config_validation_rejects_unknown_top_keys(make_ci):
    ci_ = make_ci({"bad.toml": "[noplace]\nfoo='bar'\n"})
    assert any("unknown top-level keys" in e for e in ci_.config_errors)


def test_config_validation_rejects_unknown_task_fields(make_ci):
    ci_ = make_ci({"t.toml": """
[tasks.x]
command = "echo"
tiemout = "10s"
"""})
    assert "x" not in ci_.config["tasks"]
    assert any("unknown fields" in e for e in ci_.config_errors)


def test_config_validation_rejects_bad_duration(make_ci):
    ci_ = make_ci({"t.toml": """
[tasks.x]
command = "echo"
timeout = "garbage"
"""})
    assert "x" not in ci_.config["tasks"]
    assert any("invalid timeout" in e for e in ci_.config_errors)


def test_config_validation_rejects_unresolved_variable(make_ci):
    ci_ = make_ci({"t.toml": """
[tasks.x]
command = "echo ${:nope}"
"""})
    assert "x" not in ci_.config["tasks"]
    assert any("unresolved variables" in e for e in ci_.config_errors)


def test_config_variable_substitution(make_ci):
    ci_ = make_ci({"t.toml": """
[variables]
greeting = "hi"
[tasks.x]
command = "echo ${:greeting}"
"""})
    assert ci_.config["tasks"]["x"]["command"].strip() == "echo hi"
    assert ci_.config_errors == []


def test_attach_tags_filters_uuids(make_ci):
    ci_ = make_ci()
    db = ci_.db
    run_id = db.task_run.insert(name="x", status="queued")
    db.commit()
    ci_.run_tags.add(run_id, "real-tag")
    ci_.run_tags.add(run_id, "550e8400-e29b-41d4-a716-446655440000")
    db.commit()
    rows = [{"id": run_id}]
    ci_._attach_tags(rows)
    assert rows[0]["tags"] == ["real-tag"]


def test_loop_lock_acquire_release(make_ci):
    ci_ = make_ci()
    assert ci_.acquire_loop_lock("a")
    # b can't steal while a's lock is alive
    assert not ci_.acquire_loop_lock("b")
    assert ci_.heartbeat_loop_lock("a")
    assert not ci_.heartbeat_loop_lock("b")
    ci_.release_loop_lock("a")
    # b can now acquire
    assert ci_.acquire_loop_lock("b")


def test_log_write_and_read(make_ci, monkeypatch, tmp_path):
    ci_ = make_ci()
    # redirect log dir into the tmp dir so we don't pollute the app folder
    monkeypatch.setattr(ci_, "_log_dir", lambda: str(tmp_path))
    db = ci_.db
    run_id = db.task_run.insert(name="x", status="success")
    db.commit()
    run = db.task_run(run_id)
    ci_.write_log(run, "hello world")
    fresh = db.task_run(run_id)
    assert fresh.log_size == len("hello world")
    assert ci_.read_log(fresh) == "hello world"


def test_post_git_no_branches_field_doesnt_crash(make_ci):
    ci_ = make_ci({"t.toml": """
[tasks.x]
enabled = true
command = "echo"
[[tasks.x.triggered_by]]
ssh_url = "git@example.com:o/r.git"
"""})
    # 'branches' is missing entirely — must not raise TypeError
    payload = {
        "repository": {"ssh_url": "git@example.com:o/r.git"},
        "ref": "refs/heads/main",
        "head_commit": {"id": "abc"},
    }
    out = ci_.post_git(payload)
    assert "No matching task" in out  # nothing matched, but didn't crash
