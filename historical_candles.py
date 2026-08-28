"""Read-only OANDA historical candle downloader for research replay."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
import asyncio, json, os
import httpx

DUR={"M1":60,"M5":300,"M15":900,"H1":3600}

def _dt(v):
    d=v if isinstance(v,datetime) else datetime.fromisoformat(str(v).replace("Z","+00:00"))
    if d.tzinfo is None:d=d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)

def _iso(d):return _dt(d).isoformat().replace("+00:00","Z")

async def fetch_oanda_candles(instrument:str,granularity:str,start:datetime,end:datetime,*,token:str=None,base_url:str=None)->List[Dict[str,Any]]:
    token=(token or os.getenv("OANDA_TOKEN","")).strip()
    if not token:raise RuntimeError("OANDA_TOKEN is required for historical download")
    env=os.getenv("PRIMARY_OANDA_ENV","practice").strip().lower()
    base_url=base_url or ("https://api-fxtrade.oanda.com" if env=="live" else "https://api-fxpractice.oanda.com")
    start,end=_dt(start),_dt(end);cursor=start;out=[];seen=set()
    headers={"Authorization":f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=30) as client:
        while cursor<=end:
            params={"price":"MBA","granularity":granularity,"from":_iso(cursor),"count":5000}
            r=await client.get(f"{base_url}/v3/instruments/{instrument}/candles",params=params,headers=headers)
            if r.status_code>=400:raise RuntimeError(f"OANDA {r.status_code}: {r.text[:300]}")
            candles=r.json().get("candles",[])
            if not candles:break
            last=None
            for x in candles:
                if not x.get("complete"):continue
                t=_dt(x["time"]);last=t
                if t>end:continue
                if t in seen:continue
                seen.add(t)
                m=x.get("mid") or {}; b=x.get("bid") or {}; a=x.get("ask") or {}
                if not (m and b and a):
                    raise RuntimeError(f"OANDA candle missing requested midpoint/bid/ask components at {t.isoformat()}")
                out.append({
                    "t":t.isoformat(),"o":float(m["o"]),"h":float(m["h"]),"l":float(m["l"]),"c":float(m["c"]),
                    "bid_o":float(b["o"]),"bid_h":float(b["h"]),"bid_l":float(b["l"]),"bid_c":float(b["c"]),
                    "ask_o":float(a["o"]),"ask_h":float(a["h"]),"ask_l":float(a["l"]),"ask_c":float(a["c"]),
                    "v":int(x.get("volume",0))})
            if last is None:break
            nxt=last+timedelta(seconds=DUR[granularity])
            if nxt<=cursor:break
            cursor=nxt
            if last>=end:break
    out.sort(key=lambda x:x["t"])
    return out

async def fetch_bundle(instrument,start,end,warmup_days:int=10,horizon_minutes:int=240):
    fs=_dt(start)-timedelta(days=warmup_days);fe=_dt(end)+timedelta(minutes=horizon_minutes)
    vals=await asyncio.gather(*(fetch_oanda_candles(instrument,tf,fs,fe) for tf in ("H1","M15","M5","M1")))
    return dict(zip(("H1","M15","M5","M1"),vals))

def save_bundle(path:str,bundle):
    with open(path,"w",encoding="utf-8") as f:json.dump(bundle,f,separators=(",",":"))

def load_bundle(path:str):
    with open(path,encoding="utf-8") as f:return json.load(f)
