from datetime import datetime, timezone

import pytest

from broker_weekly_reporter import classify_fill_reason, summarize_transactions, week_window_utc


def test_classify_take_profit_and_stop_loss():
    assert classify_fill_reason("TAKE_PROFIT_ORDER") == "take_profit"
    assert classify_fill_reason("STOP_LOSS_ORDER") == "stop_loss"
    assert classify_fill_reason("TRAILING_STOP_LOSS_ORDER") == "trailing_stop"
    assert classify_fill_reason("MARKET_ORDER_TRADE_CLOSE") == "other"


def test_week_window_starts_monday_eastern():
    now = datetime(2026, 9, 4, 18, 0, tzinfo=timezone.utc)
    start, end = week_window_utc(now)
    assert start == datetime(2026, 8, 31, 4, 0, tzinfo=timezone.utc)
    assert end == now


def test_summarize_transactions_by_instrument_and_close_reason():
    txs = [
        {"type": "ORDER_FILL", "instrument": "EUR_USD", "reason": "TAKE_PROFIT_ORDER",
         "tradesClosed": [{"tradeID": "10", "realizedPL": "1.25"}]},
        {"type": "ORDER_FILL", "instrument": "EUR_USD", "reason": "STOP_LOSS_ORDER",
         "tradesClosed": [{"tradeID": "11", "realizedPL": "-0.75"}]},
        {"type": "ORDER_FILL", "instrument": "USD_JPY", "reason": "TAKE_PROFIT_ORDER",
         "tradesClosed": [{"tradeID": "12", "realizedPL": "2.0"}]},
        {"type": "ORDER_FILL", "instrument": "USD_JPY", "reason": "MARKET_ORDER_TRADE_CLOSE",
         "tradesClosed": [{"tradeID": "13", "realizedPL": "0.1"}]},
    ]
    out = summarize_transactions(txs)
    assert out["EUR_USD"]["closed_trades"] == 2
    assert out["EUR_USD"]["take_profit"] == 1
    assert out["EUR_USD"]["stop_loss"] == 1
    assert out["EUR_USD"]["tp_sl_win_rate"] == pytest.approx(0.5)
    assert out["USD_JPY"]["closed_trades"] == 2
    assert out["USD_JPY"]["take_profit"] == 1
    assert out["USD_JPY"]["other_closes"] == 1


def test_duplicate_trade_is_counted_once():
    tx = {"type": "ORDER_FILL", "instrument": "EUR_USD", "reason": "TAKE_PROFIT_ORDER",
          "tradesClosed": [{"tradeID": "10", "realizedPL": "1.25"}]}
    out = summarize_transactions([tx, tx])
    assert out["EUR_USD"]["closed_trades"] == 1


def test_unrelated_instrument_is_ignored():
    tx = {"type": "ORDER_FILL", "instrument": "GBP_USD", "reason": "TAKE_PROFIT_ORDER",
          "tradesClosed": [{"tradeID": "20", "realizedPL": "1.0"}]}
    out = summarize_transactions([tx])
    assert out["EUR_USD"]["closed_trades"] == 0
    assert out["USD_JPY"]["closed_trades"] == 0
