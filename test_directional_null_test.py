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
