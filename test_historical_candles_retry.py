import asyncio

import httpx
import pytest

import historical_candles as candles


class Response:
    def __init__(self, status, *, headers=None, text=""):
        self.status_code = status
        self.headers = headers or {}
        self.text = text


class Client:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    async def get(self, *_args, **_kwargs):
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_oanda_historical_read_retries_504_and_timeout(monkeypatch):
    client = Client([
        Response(504),
        httpx.ReadTimeout("temporary"),
        Response(200),
    ])
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(candles.asyncio, "sleep", fake_sleep)
    monkeypatch.setenv("BOTS_V3_OANDA_READ_ATTEMPTS", "4")
    result = asyncio.run(candles._get_page(client, "https://example.invalid", params={}, headers={}))
    assert result.status_code == 200
    assert client.calls == 3
    assert sleeps == [1.0, 2.0]


def test_oanda_historical_read_honors_capped_retry_after(monkeypatch):
    client = Client([Response(429, headers={"Retry-After": "60"}), Response(200)])
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(candles.asyncio, "sleep", fake_sleep)
    result = asyncio.run(candles._get_page(client, "https://example.invalid", params={}, headers={}))
    assert result.status_code == 200
    assert sleeps == [10.0]


def test_oanda_historical_read_does_not_retry_nontransient_400(monkeypatch):
    client = Client([Response(400, text="bad request")])
    result = asyncio.run(candles._get_page(client, "https://example.invalid", params={}, headers={}))
    assert result.status_code == 400
    assert client.calls == 1


def test_oanda_historical_read_fails_after_bounded_attempts(monkeypatch):
    client = Client([Response(504), Response(504)])

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(candles.asyncio, "sleep", fake_sleep)
    monkeypatch.setenv("BOTS_V3_OANDA_READ_ATTEMPTS", "2")
    with pytest.raises(RuntimeError, match="failed after 2 attempts: HTTP 504"):
        asyncio.run(candles._get_page(client, "https://example.invalid", params={}, headers={}))
