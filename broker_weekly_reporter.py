import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

PRACTICE_OANDA = "https://api-fxpractice.oanda.com"
ET = ZoneInfo("America/New_York")
LOG = logging.getLogger("broker_weekly_reporter")


def week_window_utc(now_utc=None):
    now = now_utc or datetime.now(timezone.utc)
    local = now.astimezone(ET)
    monday = (local - timedelta(days=local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return monday.astimezone(timezone.utc), now.astimezone(timezone.utc)


def classify_fill_reason(reason):
    reason = str(reason or "").upper()
    if reason == "TAKE_PROFIT_ORDER":
        return "take_profit"
    if reason == "STOP_LOSS_ORDER":
        return "stop_loss"
    if reason == "TRAILING_STOP_LOSS_ORDER":
        return "trailing_stop"
    return "other"


def summarize_transactions(transactions, instruments=("EUR_USD", "USD_JPY")):
    wanted = set(instruments)
    out = {
        inst: {
            "closed_trades": 0,
            "take_profit": 0,
            "stop_loss": 0,
            "trailing_stop": 0,
            "other_closes": 0,
            "realized_pl": 0.0,
            "trade_ids": [],
        }
        for inst in instruments
    }
    seen = set()
    for tx in transactions:
        if str(tx.get("type") or "").upper() != "ORDER_FILL":
            continue
        instrument = str(tx.get("instrument") or "").upper()
        if instrument not in wanted:
            continue
        bucket = classify_fill_reason(tx.get("reason"))
        for closed in tx.get("tradesClosed") or []:
            trade_id = str(closed.get("tradeID") or "")
            if not trade_id or trade_id in seen:
                continue
            seen.add(trade_id)
            item = out[instrument]
            item["closed_trades"] += 1
            item["trade_ids"].append(trade_id)
            try:
                item["realized_pl"] += float(closed.get("realizedPL") or 0.0)
            except (TypeError, ValueError):
                pass
            if bucket == "take_profit":
                item["take_profit"] += 1
            elif bucket == "stop_loss":
                item["stop_loss"] += 1
            elif bucket == "trailing_stop":
                item["trailing_stop"] += 1
            else:
                item["other_closes"] += 1
    for item in out.values():
        denom = item["take_profit"] + item["stop_loss"]
        item["tp_sl_win_rate"] = (item["take_profit"] / denom) if denom else None
        item["realized_pl"] = round(item["realized_pl"], 8)
    return out


async def fetch_transactions(client, account, token, start_utc, end_utc):
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": start_utc.isoformat().replace("+00:00", "Z"),
        "to": end_utc.isoformat().replace("+00:00", "Z"),
        "pageSize": "100",
    }
    url = f"{PRACTICE_OANDA}/v3/accounts/{account}/transactions"
    r = await client.get(url, headers=headers, params=params, timeout=30.0)
    r.raise_for_status()
    payload = r.json()
    transactions = list(payload.get("transactions") or [])
    for page in payload.get("pages") or []:
        if not str(page).startswith(PRACTICE_OANDA):
            raise RuntimeError("unexpected OANDA transaction page host")
        pr = await client.get(page, headers=headers, timeout=30.0)
        pr.raise_for_status()
        transactions.extend(pr.json().get("transactions") or [])
    return transactions


async def build_weekly_report(now_utc=None):
    trading_env = os.getenv("TRADING_ENVIRONMENT", "PAPER").strip().upper()
    oanda_env = os.getenv("PRIMARY_OANDA_ENV", "practice").strip().lower()
    if trading_env == "PRODUCTION" or oanda_env != "practice":
        raise RuntimeError("weekly report is restricted to PAPER/OANDA Practice")
    account = os.getenv("OANDA_ACCOUNT_ID", "").strip()
    token = os.getenv("OANDA_TOKEN", "").strip()
    if not account or not token:
        raise RuntimeError("missing OANDA Practice credentials")
    start_utc, end_utc = week_window_utc(now_utc)
    async with httpx.AsyncClient() as client:
        txs = await fetch_transactions(client, account, token, start_utc, end_utc)
    stats = summarize_transactions(txs)
    return {
        "report_type": "BROKER_WEEKLY_REPORT",
        "source": "OANDA_PRACTICE_READ_ONLY",
        "period_start_utc": start_utc.isoformat(),
        "period_end_utc": end_utc.isoformat(),
        "instruments": stats,
        "production_authority": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def run_forever():
    interval = max(900, int(os.getenv("BROKER_WEEKLY_REPORT_INTERVAL_SECONDS", "3600")))
    while True:
        try:
            report = await build_weekly_report()
            LOG.info("BROKER_WEEKLY_REPORT %s", json.dumps(report, separators=(",", ":"), sort_keys=True))
        except Exception as exc:
            LOG.error("BROKER_WEEKLY_REPORT_ERROR %s", str(exc)[:300])
        await asyncio.sleep(interval)


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
