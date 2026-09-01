import json
from datetime import datetime, timedelta, timezone

from research_integrity import compare_determinism, validate_dataset


def _bundle(midpoint_only=False):
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    bundle={}
    for tf,seconds in (("H1",3600),("M15",900),("M5",300),("M1",60)):
        rows=[]
        for i in range(86400//seconds+4):
            row={"t":(start+timedelta(seconds=i*seconds)).isoformat(),"o":1.0,"h":1.2,"l":.9,"c":1.1,"v":10}
            if tf=="M1" and not midpoint_only:
                row.update({"bid_o":1.0,"bid_h":1.2,"bid_l":.9,"bid_c":1.1,
                            "ask_o":1.01,"ask_h":1.21,"ask_l":.91,"ask_c":1.11})
            rows.append(row)
        bundle[tf]=rows
    return bundle


def test_data_integrity_passes_real_bid_ask_and_blocks_midpoint(tmp_path):
    good=tmp_path/"good.json";good.write_text(json.dumps(_bundle()),encoding="utf-8")
    out=validate_dataset(str(good),instrument="AUD_USD",start="2026-01-02T00:00:00Z",end="2026-01-02T00:01:00Z",warmup_days=1,horizon_minutes=1,repo=".")
    assert out["status"]=="PASS" and out["bid_ask_real"] is True
    bad=tmp_path/"bad.json";bad.write_text(json.dumps(_bundle(True)),encoding="utf-8")
    blocked=validate_dataset(str(bad),instrument="AUD_USD",start="2026-01-02T00:00:00Z",end="2026-01-02T00:01:00Z",warmup_days=1,horizon_minutes=1,repo=".")
    assert blocked["status"]=="FAIL"
    assert "M1_MIDPOINT_ONLY_OR_INCOMPLETE_BID_ASK" in blocked["failures"]


def test_determinism_ignores_only_explicit_timestamp_fields(tmp_path):
    a=tmp_path/"a.json";b=tmp_path/"b.json"
    a.write_text('{"created_at":"one","metric":3}',encoding="utf-8")
    b.write_text('{"created_at":"two","metric":3}',encoding="utf-8")
    assert compare_determinism(str(a),str(b))["status"]=="PASS"
    b.write_text('{"created_at":"two","metric":4}',encoding="utf-8")
    assert compare_determinism(str(a),str(b))["status"]=="FAIL"
