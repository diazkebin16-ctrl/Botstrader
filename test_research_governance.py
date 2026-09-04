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


def _valid_best_viable_approval():
    import hashlib, json
    best = {"opened_gates": [], "wins_recovered": 2, "losses_released": 4, "eligible_episodes": 9}
    best_sha = hashlib.sha256(
        json.dumps(best, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    return {
        "approval_type": "BEST_VIABLE_POLICY",
        "active": True,
        "instrument": "AUD_USD",
        "dataset_identity": "dataset-a",
        "code_sha": "code-a",
        "phase1_artifact_sha256": "phase1-sha",
        "best_policy_sha256": best_sha,
        "ia1_approved": True,
        "production_authority": False,
    }, best


def test_phase2_allows_valid_best_viable_approval():
    approval, best = _valid_best_viable_approval()
    result = DecisionGateEngine.evaluate(
        "PHASE_2",
        integrity={"status":"PASS"},
        replay={"methodology":{"no_lookahead_decision":True}},
        target_population={"lookahead_protection":True},
        phase1={
            "all_target_wins_recovered":False,
            "unrecovered_target_wins":[{"immutable_blocks":["WAIT_DIRECTION"]}],
            "selection_scope":"DISCOVERY_ONLY",
            "best_policy":best,
        },
        phase1_approval=approval,
        instrument="AUD_USD",
        dataset_identity="dataset-a",
        code_sha="code-a",
        phase1_artifact_sha256="phase1-sha",
    )
    assert result["status"] == "ALLOWED"
    assert result["production_authority"] is False


def test_phase2_rejects_boolean_best_viable_approval():
    result = DecisionGateEngine.evaluate(
        "PHASE_2",
        integrity={"status":"PASS"},
        replay={"methodology":{"no_lookahead_decision":True}},
        target_population={"lookahead_protection":True},
        phase1={
            "all_target_wins_recovered":False,
            "unrecovered_target_wins":[{"immutable_blocks":["WAIT_DIRECTION"]}],
            "selection_scope":"DISCOVERY_ONLY",
            "best_policy":{"opened_gates":[],"wins_recovered":1,"losses_released":0,"eligible_episodes":1},
        },
        phase1_approval=True,
        instrument="AUD_USD",
        dataset_identity="dataset-a",
        code_sha="code-a",
        phase1_artifact_sha256="phase1-sha",
    )
    assert result["status"] == "BLOCKED"


def test_phase2_rejects_stale_or_unsafe_best_viable_approval():
    approval, best = _valid_best_viable_approval()
    mutations = [
        ("approval_type", "OTHER"),
        ("active", False),
        ("ia1_approved", False),
        ("production_authority", True),
        ("instrument", "EUR_USD"),
        ("dataset_identity", "dataset-b"),
        ("code_sha", "code-b"),
        ("phase1_artifact_sha256", "other-sha"),
        ("best_policy_sha256", "0" * 64),
    ]
    for field, value in mutations:
        bad = dict(approval)
        bad[field] = value
        result = DecisionGateEngine.evaluate(
            "PHASE_2",
            integrity={"status":"PASS"},
            replay={"methodology":{"no_lookahead_decision":True}},
            target_population={"lookahead_protection":True},
            phase1={
                "all_target_wins_recovered":False,
                "unrecovered_target_wins":[{"immutable_blocks":["WAIT_DIRECTION"]}],
                "selection_scope":"DISCOVERY_ONLY",
                "best_policy":best,
            },
            phase1_approval=bad,
            instrument="AUD_USD",
            dataset_identity="dataset-a",
            code_sha="code-a",
            phase1_artifact_sha256="phase1-sha",
        )
        assert result["status"] == "BLOCKED"
