import forward_audit
import server


def _r(room=0.8, rr=1.5, ext=0.5):
    return {
        "instrument":"EUR_USD","signal":"BUY","score":45,"rr":1.5,"rr_raw":rr,
        "buy_score":45,"sell_score":30,"direction_edge":15,"barrier_class":"WEAK",
        "blocked":False,
        "filters":{"barrier_room_ok":True,"m1_confirmation":True},
        "safety_checks":{"minimum_rr":True,"barrier_room_ok":True},
        "features":{"room_to_barrier_r":room,"rr_raw":rr,"extension_atr":ext,"buy_score":45,"sell_score":30,"direction_edge":15},
    }


def test_snapshot_records_independent_vetoes_without_authority():
    x=server.forward_observation_snapshot(_r(room=.3,rr=.9,ext=.9),{"probability":.5,"samples":0})
    assert x["observational_only"] is True
    assert x["vetoes"]["low_room_low_rr"] is True
    assert x["vetoes"]["low_room_extended"] is True
    assert x["admitted_only_by_min_entry_rr_relaxation"] is True


def test_evidence_classes_are_preregistered():
    assert forward_audit.evidence_class(14)=="INCONCLUSIVE_UNDERPOWERED"
    assert forward_audit.evidence_class(15)=="WEAK_EVIDENCE"
    assert forward_audit.evidence_class(30)=="USABLE_EVIDENCE"


def test_overlap_is_order_independent():
    rows=[
        {"vetoes":{"a":True,"b":False}},
        {"vetoes":{"a":True,"b":True}},
        {"vetoes":{"a":False,"b":True}},
    ]
    out=forward_audit.filter_overlap_audit(rows,["a","b"])
    assert out["filters"]["a"]["true_unique_veto"]==1
    assert out["filters"]["b"]["true_unique_veto"]==1
    assert out["filters"]["a"]["remove_one_delta"]==1
    assert out["pairs"][0]["intersection"]==1
    assert abs(out["pairs"][0]["jaccard"]-1/3)<1e-12
