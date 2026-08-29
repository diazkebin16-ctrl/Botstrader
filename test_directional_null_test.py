from directional_null_test import DirectionalNullConfig,evaluate_pairs,evaluate_shadow_test_b,_side_geometry_valid,_side_shadow_geometry_valid

def side(r,status="WIN"):
    return {"status":status,"realized_r":r if status in ("WIN","LOSS") else None}

def test_geometry_requires_frozen_safety_fields():
    hyp={"safety_checks":{k:True for k in ("finite_prices","positive_risk","minimum_rr","minimum_tp_pips","minimum_stop_pips","barrier_room_ok","volatility_sane")}}
    assert _side_geometry_valid(hyp)[0]
    hyp["safety_checks"]["minimum_rr"]=False
    assert not _side_geometry_valid(hyp)[0]

def test_pair_accounting_and_one_sided_diagnostic():
    rows=[
      {"bot_direction":"BUY","buy":side(1.0),"sell":side(-1.0,"LOSS")},
      {"bot_direction":"BUY","buy":side(1.0),"sell":side(None,"TIMEOUT")},
      {"bot_direction":"SELL","buy":side(None,"AMBIGUOUS"),"sell":side(-1.0,"LOSS")},
      {"bot_direction":"BUY","buy":side(None,"ENTRY_INVALIDATED"),"sell":side(None,"TIMEOUT")},
    ]
    r=evaluate_pairs(rows,DirectionalNullConfig(simulations=100,bootstrap_samples=100,rng_seed=1,bootstrap_seed=2))
    assert r["counts"]["both_sides_comparable"]==1
    assert r["counts"]["buy_only_valid"]==1
    assert r["counts"]["sell_only_valid"]==1
    assert r["counts"]["neither_comparable"]==1
    assert r["one_sided_robustness"]["n"]==2
    assert r["one_sided_robustness"]["bot_selected_unique_valid_side"]==2

def test_null_is_reproducible():
    rows=[{"bot_direction":"BUY","buy":side(1.2),"sell":side(-1.1,"LOSS")} for _ in range(8)]
    cfg=DirectionalNullConfig(simulations=500,bootstrap_samples=500,rng_seed=7,bootstrap_seed=8)
    a=evaluate_pairs(rows,cfg);b=evaluate_pairs(rows,cfg)
    assert a["paired_test"]==b["paired_test"]
    assert a["economic_edge"]==b["economic_edge"]


def test_shadow_geometry_ignores_only_rr_and_barrier():
    keys=("finite_prices","positive_risk","minimum_rr","minimum_tp_pips",
          "minimum_stop_pips","barrier_room_ok","volatility_sane")
    hyp={"safety_checks":{k:True for k in keys}}
    hyp["safety_checks"]["minimum_rr"]=False
    hyp["safety_checks"]["barrier_room_ok"]=False
    assert _side_shadow_geometry_valid(hyp)[0]
    hyp["safety_checks"]["minimum_stop_pips"]=False
    assert not _side_shadow_geometry_valid(hyp)[0]

def test_shadow_test_b_uses_extended_outcomes_separately():
    rows=[
      {"bot_direction":"BUY",
       "buy":side(1.0),"sell":side(None,"GEOMETRY_INVALID"),
       "buy_shadow_b":side(1.0),"sell_shadow_b":side(-1.0,"LOSS")},
      {"bot_direction":"SELL",
       "buy":side(None,"GEOMETRY_INVALID"),"sell":side(1.0),
       "buy_shadow_b":side(-1.0,"LOSS"),"sell_shadow_b":side(1.0)},
    ]
    # Supply the exact allowed invalid reasons used for eligibility accounting.
    rows[0]["sell"]["note"]="SAFETY:minimum_rr"
    rows[1]["buy"]["note"]="SAFETY:minimum_rr,barrier_room_ok"
    cfg=DirectionalNullConfig(simulations=100,bootstrap_samples=100,rng_seed=3,bootstrap_seed=4)
    r=evaluate_shadow_test_b(rows,cfg)
    assert r["n"]==2
    assert r["eligible_invalid_sides"]==2
    assert r["eligible_shadow_sides_resolved_comparable"]==2
    assert r["bot_expectancy_r"]==1.0
    assert r["opposite_shadow_expectancy_r"]==-1.0

from directional_null_test import (population_selection_audit,
    differential_evidence_association_audit,score_margin_calibration_audit,
    session_regime_shadow_audit,E1_PRIMARY_COMPONENTS,E1_SECONDARY_COMPONENTS,E1_EXCLUDED_COMPONENTS,_e1_evidence_sample_class)

def _audit_row(br,sr,bd=70,sd=40,session="BUY"):
    comps=lambda score,h1,m15,m5,m1,rr: {
      "direction_score":score,"h1_score_contribution":h1,"m15_score_contribution":m15,
      "m5_score_contribution":m5,"m1_score_contribution":m1,"pullback_score_contribution":8,
      "rr_score_contribution":rr,"broken_barrier_score_contribution":2,
      "session_direction":session,"session_strength":.7,
    }
    return {"bot_direction":"BUY","buy_shadow_b":side(br,"WIN" if br>0 else "LOSS"),
            "sell_shadow_b":side(sr,"WIN" if sr>0 else "LOSS"),
            "buy_components":comps(bd,16,20,18,16,8),
            "sell_components":comps(sd,-10,-12,7,6,0)}

def test_population_selection_audit_counts_status_pairs():
    rows=[_audit_row(1,-1),
          {**_audit_row(1,-1),"sell_shadow_b":side(None,"TIMEOUT") }]
    r=population_selection_audit(rows)
    assert r["total_opportunities"]==2 and r["both_sides_comparable"]==1
    assert r["status_pair_counts"]["WIN/TIMEOUT"]==1

def test_e1_uses_buy_minus_sell_contributions_not_raw_market_duplicates():
    rows=[_audit_row(1,-1) for _ in range(24)]
    r=differential_evidence_association_audit(rows,bootstrap_samples=100,seed=1)
    assert r["components"]["h1_score_contribution"]["concordance"]==1.0
    assert r["components"]["direction_score"]["status"]=="WEAK_LIMITED_EVIDENCE"
    assert r["components"]["direction_score"]["concordance_fdr_q"] < .10

def test_score_margin_and_session_shadow_are_research_only():
    rows=[_audit_row(1,-1,70+i,40,"BUY") for i in range(25)]
    cal=score_margin_calibration_audit(rows); sess=session_regime_shadow_audit(rows)
    assert cal["research_only"] and sess["research_only"]
    assert sess["accuracy"]==1.0 and sess["n_non_neutral"]==25


def test_e1_excludes_m1_gate_from_primary_and_marks_rr_secondary():
    assert "m1_score_contribution" not in E1_PRIMARY_COMPONENTS
    assert "m1_score_contribution" not in E1_SECONDARY_COMPONENTS
    assert "m1_score_contribution" in E1_EXCLUDED_COMPONENTS
    assert "NON_IDENTIFIABLE_SELECTION_CONDITIONED" in E1_EXCLUDED_COMPONENTS["m1_score_contribution"]
    assert "extension_score_contribution" in E1_EXCLUDED_COMPONENTS
    assert E1_SECONDARY_COMPONENTS == ("rr_score_contribution",)
    r=differential_evidence_association_audit([_audit_row(1,-1) for _ in range(24)],bootstrap_samples=50,seed=9)
    assert "m1_score_contribution" not in r["components"]
    assert r["secondary_components"] == ["rr_score_contribution"]
    assert "m1_score_contribution" in r["excluded_components"]


def test_e1_pre_registered_evidence_size_boundaries_are_fixed():
    assert _e1_evidence_sample_class(14) == "UNDERPOWERED"
    assert _e1_evidence_sample_class(15) == "WEAK_LIMITED_EVIDENCE"
    assert _e1_evidence_sample_class(29) == "WEAK_LIMITED_EVIDENCE"
    assert _e1_evidence_sample_class(30) == "USABLE"

    for n, expected in ((14,"UNDERPOWERED"),(15,"WEAK_LIMITED_EVIDENCE"),
                        (29,"WEAK_LIMITED_EVIDENCE"),(30,"USABLE")):
        result=differential_evidence_association_audit(
            [_audit_row(1,-1) for _ in range(n)],bootstrap_samples=20,seed=77)
        comp=result["components"]["h1_score_contribution"]
        assert comp["n_effective"] == n
        assert comp["evidence_sample_class"] == expected

def test_e1_bh_family_regression_excludes_mechanically_conditioned_m1():
    result=differential_evidence_association_audit(
        [_audit_row(1,-1) for _ in range(30)],bootstrap_samples=20,seed=78)
    assert "m1_score_contribution" not in result["components"]
    assert "m1_score_contribution" in result["excluded_components"]
    assert "NON_IDENTIFIABLE_SELECTION_CONDITIONED" in result["excluded_components"]["m1_score_contribution"]
    assert "m1_score_contribution" not in result["primary_components"]
    assert "m1_score_contribution" not in result["secondary_components"]
    assert "m1_score_contribution:concordance" not in {
        f"{name}:concordance" for name in result["components"]
    }
