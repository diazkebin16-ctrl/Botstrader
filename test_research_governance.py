from research_governance import DecisionGateEngine


def test_phase2_blocks_unrecovered_target_wins():
    result=DecisionGateEngine.evaluate(
        "PHASE_2",integrity={"status":"PASS"},replay={"methodology":{"no_lookahead_decision":True}},
        target_population={"lookahead_protection":True},
        phase1={"all_target_wins_recovered":False,"unrecovered_target_wins":[{}, {}, {}],"selection_scope":"DISCOVERY_ONLY"},
    )
    assert result["status"]=="BLOCKED"
    assert "3 target WINs unrecovered" in result["reasons"][0]


def test_holdout_requires_freeze_and_forward_requires_ia1():
    assert DecisionGateEngine.evaluate("HOLDOUT",discovery={"status":"OK"},frozen=None)["status"]=="BLOCKED"
    forward=DecisionGateEngine.evaluate(
        "FORWARD",holdout={"status":"PASS","overfitting_risk":{"severity":"LOW"}},
        pre_audit={"verdict":"ACCEPT"},ia1_approved=False,
    )
    assert forward["status"]=="BLOCKED"
    assert "IA #1 APPROVAL REQUIRED" in forward["reasons"]
