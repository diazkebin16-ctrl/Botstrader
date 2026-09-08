import json
import subprocess

import pytest

import automation_v3_railway_adapter as adapter


def test_railway_json_retries_bounded_transient_failure(monkeypatch):
    calls = []
    sleeps = []

    def fake_run(command, *, cwd=None, timeout=None):
        calls.append((command, cwd, timeout))
        if len(calls) == 1:
            raise subprocess.TimeoutExpired(command, timeout)
        if len(calls) == 2:
            return subprocess.CompletedProcess(command, 1, "", "temporary failure")
        return subprocess.CompletedProcess(command, 0, json.dumps({"variables": {"A": "B"}}), "")

    monkeypatch.setattr(adapter, "_run", fake_run)
    monkeypatch.setattr(adapter.time, "sleep", sleeps.append)
    monkeypatch.setenv("BOTS_V3_RAILWAY_READ_ATTEMPTS", "3")
    monkeypatch.setenv("BOTS_V3_RAILWAY_READ_TIMEOUT_SECONDS", "17")
    assert adapter._railway_json(["variable", "list", "--json"]) == {"variables": {"A": "B"}}
    assert len(calls) == 3
    assert all(call[2] == 17 for call in calls)
    assert sleeps == [2, 4]


def test_railway_json_stops_after_configured_attempts(monkeypatch):
    calls = []

    def always_timeout(command, *, cwd=None, timeout=None):
        calls.append(command)
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(adapter, "_run", always_timeout)
    monkeypatch.setattr(adapter.time, "sleep", lambda _seconds: None)
    monkeypatch.setenv("BOTS_V3_RAILWAY_READ_ATTEMPTS", "2")
    with pytest.raises(RuntimeError, match="failed after 2 attempts: variable list"):
        adapter._railway_json(["variable", "list", "--json"])
    assert len(calls) == 2
