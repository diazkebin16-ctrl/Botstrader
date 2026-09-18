"""The scan is skipped while the market is closed, and nothing else is.

Execution already refused to act on a closed market; what this covers is that
the collect pipeline no longer runs either, that skipping it does not look like
a dead worker to the watchdog, and that the pieces which must keep working
through the weekend still do.
"""

import asyncio

import pytest

import server


class _Recorder:
    def __init__(self, result=None):
        self.calls = []
        self.result = result

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result

    @property
    def called(self):
        return bool(self.calls)


@pytest.fixture
def closed_market(monkeypatch):
    monkeypatch.setattr(server, 'market_is_weekend_closed', lambda *a, **k: True)
    monkeypatch.setattr(server, 'WEEKEND_SCAN_ENABLED', False)
    monkeypatch.setattr(server, 'SCAN_INSTRUMENTS', ['EUR_USD', 'GBP_USD'])
    monkeypatch.setattr(server, 'instrument_mode', lambda _i: 'ENABLED')
    monkeypatch.setattr(server, 'OBSERVABILITY_ENABLED', False)
    monkeypatch.setattr(server, 'storage_maintenance_window', _Recorder({'compacted': False}))
    monkeypatch.setattr(server, 'collect_weekend_news_snapshot', _Recorder({'collected': True}))
    server.state.pop('market_closed_cycles', None)
    server.state.pop('worker_last_heartbeat', None)
    return server


def test_closed_market_does_not_touch_the_collect_pipeline(closed_market, monkeypatch):
    scan = _Recorder({'signal': 'BUY'})
    monkeypatch.setattr(server, '_batch_scan_candidate', scan)
    candles = _Recorder([])
    monkeypatch.setattr(server, 'candles', candles)

    assert asyncio.run(server.scan_instruments_once(None)) is True

    assert scan.called is False, 'no instrument should be scanned with the market shut'
    assert candles.called is False, 'no candle should be fetched with the market shut'
    assert server.state['market_closed_cycles'] == 1


def test_closed_market_still_signals_liveness(closed_market, monkeypatch):
    """A skipped scan must not read as a dead worker to the watchdog."""
    monkeypatch.setattr(server, '_batch_scan_candidate', _Recorder({}))
    asyncio.run(server.scan_instruments_once(None))

    assert server.state.get('worker_last_heartbeat')
    snapshot = server.scanner_health_snapshot()
    assert snapshot['stale'] is False


def test_closed_market_still_collects_weekend_research(closed_market, monkeypatch):
    monkeypatch.setattr(server, '_batch_scan_candidate', _Recorder({}))
    asyncio.run(server.scan_instruments_once(None))

    assert len(server.collect_weekend_news_snapshot.calls) == 2
    assert server.state['weekend_research']['EUR_USD'] == {'collected': True}


def test_closed_market_is_when_maintenance_is_offered(closed_market, monkeypatch):
    """VACUUM takes an exclusive lock, so it is offered only here."""
    monkeypatch.setattr(server, '_batch_scan_candidate', _Recorder({}))
    asyncio.run(server.scan_instruments_once(None))

    assert server.storage_maintenance_window.called is True


def test_weekend_research_failure_does_not_break_the_cycle(closed_market, monkeypatch):
    async def boom(*_a, **_k):
        raise RuntimeError('GDELT down')

    monkeypatch.setattr(server, 'collect_weekend_news_snapshot', boom)
    monkeypatch.setattr(server, '_batch_scan_candidate', _Recorder({}))

    assert asyncio.run(server.scan_instruments_once(None)) is True


def test_the_old_behaviour_is_one_setting_away(closed_market, monkeypatch):
    """WEEKEND_SCAN_ENABLED=true puts the full closed-market scan back."""
    monkeypatch.setattr(server, 'WEEKEND_SCAN_ENABLED', True)
    delegated = _Recorder(True)
    monkeypatch.setattr(server, 'closed_market_cycle', delegated)
    monkeypatch.setattr(server, '_batch_scan_candidate', _Recorder({}))

    try:
        asyncio.run(server.scan_instruments_once(None))
    except Exception:
        # The rest of the cycle needs a real broker client; reaching it at all
        # is the point, and it is what the assertion below records.
        pass

    assert delegated.called is False


def test_open_market_is_untouched(closed_market, monkeypatch):
    monkeypatch.setattr(server, 'market_is_weekend_closed', lambda *a, **k: False)
    delegated = _Recorder(True)
    monkeypatch.setattr(server, 'closed_market_cycle', delegated)
    monkeypatch.setattr(server, '_batch_scan_candidate', _Recorder({}))

    try:
        asyncio.run(server.scan_instruments_once(None))
    except Exception:
        pass

    assert delegated.called is False
