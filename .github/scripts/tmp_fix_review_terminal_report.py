from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'anchor not found in {path}')
    text = text.replace(old, new, 1)
    p.write_text(text, encoding='utf-8')


# 1) REVIEW orchestration: persist diagnostics even when the scientific pipeline
# terminates NO_VALID_CANDIDATE before the pre-holdout sentinel is reached.
path = 'automation_v3_modes.py'
old = '''    def _terminal(self, ledger: V3Ledger, instrument: str, status: str, reason: str, **extra: Any) -> dict[str, Any]:
        if status == "METHODOLOGY_BLOCKED" and reason == REVIEW_PAUSE_SENTINEL:
            run = ledger.run(instrument)
            attempts = run.get("lookback_attempts") or []
            if not attempts:
                return super()._terminal(ledger, instrument, "NO_VALID_CANDIDATE", "review workspace unavailable", **extra)
            months = int(attempts[-1]["months"])
            code_sha = str(run.get("code_sha") or "")
            workspace = Path(run.get("workspace") or "") / f"lookback_{months:02d}m_{code_sha[:12]}"
            try:
                path, shortlist = build_review_shortlist(workspace, run_id=os.getenv("GITHUB_RUN_ID", "local-review-run"))
            except ValueError as exc:
                return super()._terminal(ledger, instrument, "NO_VALID_CANDIDATE", str(exc), lookback_months=months, **extra)
            return ledger.mutate(
                instrument, status=REVIEW_READY, final_outcome=REVIEW_READY,
                stop_reason="awaiting exact pre-holdout candidate selection",
                lookback_months=months, review_shortlist=str(path),
                review_shortlist_sha256=shortlist["shortlist_sha256"],
                review_candidates=shortlist["diagnostic_top_candidates"],
                diagnostic_top_candidates=shortlist["diagnostic_top_candidates"],
                deployable_candidates=shortlist["deployable_candidates"],
                incumbent_metrics=shortlist["incumbent_metrics"], mode=REVIEW_BEFORE_HOLDOUT_DEPLOY,
            )
        return super()._terminal(ledger, instrument, status, reason, **extra)
'''
new = '''    def _review_workspace(self, ledger: V3Ledger, instrument: str) -> tuple[Path, int] | None:
        run = ledger.run(instrument)
        attempts = run.get("lookback_attempts") or []
        if not attempts or not isinstance(attempts[-1], Mapping):
            return None
        months = int(attempts[-1].get("months") or 0)
        code_sha = str(run.get("code_sha") or "")
        workspace_root = str(run.get("workspace") or "")
        if not months or len(code_sha) != 40 or not workspace_root:
            return None
        return Path(workspace_root) / f"lookback_{months:02d}m_{code_sha[:12]}", months

    def _persist_review_result(
        self,
        ledger: V3Ledger,
        instrument: str,
        *,
        status: str,
        reason: str,
        final_outcome: str,
        **extra: Any,
    ) -> dict[str, Any] | None:
        resolved = self._review_workspace(ledger, instrument)
        if resolved is None:
            return None
        workspace, months = resolved
        discovery_path = workspace / "06_discovery.json"
        if not discovery_path.is_file():
            return None
        try:
            path, shortlist = build_review_shortlist(
                workspace, run_id=os.getenv("GITHUB_RUN_ID", "local-review-run")
            )
        except ValueError:
            return None
        return ledger.mutate(
            instrument,
            status=status,
            final_outcome=final_outcome,
            stop_reason=reason,
            lookback_months=months,
            review_shortlist=str(path),
            review_shortlist_sha256=shortlist["shortlist_sha256"],
            review_candidates=shortlist["diagnostic_top_candidates"],
            diagnostic_top_candidates=shortlist["diagnostic_top_candidates"],
            deployable_candidates=shortlist["deployable_candidates"],
            incumbent_metrics=shortlist["incumbent_metrics"],
            mode=REVIEW_BEFORE_HOLDOUT_DEPLOY,
            **extra,
        )

    def _terminal(self, ledger: V3Ledger, instrument: str, status: str, reason: str, **extra: Any) -> dict[str, Any]:
        if status == "METHODOLOGY_BLOCKED" and reason == REVIEW_PAUSE_SENTINEL:
            persisted = self._persist_review_result(
                ledger,
                instrument,
                status=REVIEW_READY,
                reason="awaiting exact pre-holdout candidate selection",
                final_outcome=REVIEW_READY,
                **extra,
            )
            if persisted is not None:
                return persisted
            return super()._terminal(
                ledger, instrument, "NO_VALID_CANDIDATE", "review workspace unavailable", **extra
            )

        # Scientific terminal and review reporting are separate concerns. The
        # base optimizer can classify discovery as NO_VALID_CANDIDATE before it
        # reaches the review sentinel. In REVIEW mode we preserve that terminal
        # decision while still persisting the immutable diagnostic evidence.
        if status == "NO_VALID_CANDIDATE":
            persisted = self._persist_review_result(
                ledger,
                instrument,
                status=status,
                reason=reason,
                final_outcome=status,
                **extra,
            )
            if persisted is not None:
                return persisted
        return super()._terminal(ledger, instrument, status, reason, **extra)
'''
replace_once(path, old, new)

# 2) Make incumbent metrics extraction explicit and resilient.
old = '''        "incumbent_metrics": ((incumbent.get("validation") or {}).get("metrics") or (diagnostic_top_candidates[0].get("incumbent_metrics") if diagnostic_top_candidates else {})),
'''
new = '''        "incumbent_metrics": (
            (diagnostic_top_candidates[0].get("incumbent_metrics") if diagnostic_top_candidates else None)
            or ((incumbent.get("validation") or {}).get("metrics") if isinstance(incumbent.get("validation"), Mapping) else None)
            or (incumbent.get("validation") if isinstance(incumbent.get("validation"), Mapping) else None)
            or {}
        ),
'''
replace_once(path, old, new)

# 3) Remote status must surface persisted REVIEW diagnostics even when terminal
# state remains NO_VALID_CANDIDATE.
path = 'automation_v3_remote_worker.py'
old = '''        "autonomous_approval": run.get("autonomous_approval") if authoritative else None,
        "production_authority": False,
'''
new = '''        "autonomous_approval": run.get("autonomous_approval") if authoritative else None,
        "mode": run.get("mode") if authoritative else None,
        "incumbent_metrics": run.get("incumbent_metrics") if authoritative else None,
        "diagnostic_top_candidates": run.get("diagnostic_top_candidates") if authoritative else None,
        "deployable_candidates": run.get("deployable_candidates") if authoritative else None,
        "shortlist_sha256": run.get("review_shortlist_sha256") if authoritative else None,
        "production_authority": False,
'''
replace_once(path, old, new)

# 4) Add real terminal-path regression tests, not just shortlist-builder tests.
test_path = Path('test_automation_v3_modes.py')
text = test_path.read_text(encoding='utf-8')
if 'test_review_no_valid_candidate_terminal_persists_real_diagnostics' not in text:
    text += r'''


def _terminal_workspace(tmp_path: Path, *, status_kind="HIGH_OVERFITTING_RISK", count=30):
    sha = "b" * 40
    root = tmp_path / "GBP_USD" / "autonomous_v3"
    workspace = root / f"lookback_01m_{sha[:12]}"
    workspace.mkdir(parents=True)
    dataset = {"code_sha": sha, "data_sha256": "d" * 64}
    write_json(workspace / "03_target_population.json", {"instrument": "GBP_USD", "dataset_identity": dataset})
    write_json(workspace / "04_phase_1.json", {"stage": "phase_1"})
    write_json(workspace / "05_phase_2.json", {
        "instrument": "GBP_USD", "dataset_identity": dataset,
        "selection_protocol": "DISCOVERY_DEFINE__VALIDATION_SELECT__FREEZE__HOLDOUT_ONCE",
        "partition_config": {"horizon_minutes": 240, "embargo_minutes": 30},
        "lookahead_protection": True,
    })
    records = []
    for i in range(count):
        rec = _candidate(f"diag{i}", 0.20 - i * 0.001, 0.12 - i * 0.001, eligible=False)
        rec["validation"]["selected"].update({"wins": 40 + i, "losses": 60, "resolved_binary": 100 + i})
        rec["incumbent_comparison"]["validation"]["incumbent"].update({"wins": 16, "losses": 30, "resolved_binary": 46})
        if status_kind == "HIGH_OVERFITTING_RISK":
            rec["overfitting_risk"] = {"severity": "HIGH", "flags": ["HIGH_OVERFITTING_RISK"]}
            rec["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "NO_VALID_CANDIDATE", "failed": ["HIGH_OVERFITTING_RISK"]}
        elif status_kind == "NO_MEANINGFUL_IMPROVEMENT":
            rec["incumbent_comparison"]["validation"].update({"challenger_beats_incumbent": False, "material_improvement": False})
            rec["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "NO_MEANINGFUL_IMPROVEMENT", "failed": ["NO_MEANINGFUL_IMPROVEMENT"]}
        elif status_kind == "CHALLENGER_BETTER_BUT_NOT_ROBUST":
            rec["incumbent_comparison"]["validation"].update({"challenger_beats_incumbent": True, "material_improvement": True})
            rec["directional_stability"] = {"stable": False}
            rec["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "CHALLENGER_BETTER_BUT_NOT_ROBUST", "failed": ["DIRECTIONAL_INSTABILITY"]}
        records.append(rec)
    discovery = {
        "instrument": "GBP_USD", "dataset_identity": dataset, "holdout_opened": False,
        "incumbent": {"definition": {"methodology_identity": "m" * 64}, "incumbent_definition_sha256": "i" * 64},
        "candidate_space": {"generated": 120 if count else 0, "evaluated_after_discovery_gate": count, "freeze_eligible": 0},
        "ranked_candidates": records, "proposed_frozen_candidate": None, "production_authority": False,
    }
    write_json(workspace / "06_discovery.json", discovery)
    write_json(workspace / "08_determinism.json", {"status": "PASS"})
    ledger = V3Ledger(root / "automation_v3_state.json")
    ledger.mutate("GBP_USD", status="RUNNING", code_sha=sha, workspace=str(root), lookback_attempts=[{"months": 1, "code_sha": sha}])
    optimizer = ReviewBeforeHoldoutOptimizer(Path.cwd(), code_sha_provider=lambda: sha)
    return optimizer, ledger, root, workspace, sha


def test_review_no_valid_candidate_terminal_persists_real_diagnostics(tmp_path):
    optimizer, ledger, root, workspace, sha = _terminal_workspace(tmp_path, status_kind="HIGH_OVERFITTING_RISK", count=30)
    result = optimizer._terminal(
        ledger, "GBP_USD", "NO_VALID_CANDIDATE", "HIGH_OVERFITTING_RISK",
        diagnostic={"generated_candidates": 120, "evaluated_after_discovery_gate": 30, "freeze_eligible": 0, "dominant_failure": "HIGH_OVERFITTING_RISK"},
    )
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert result["final_outcome"] == "NO_VALID_CANDIDATE"
    assert result["incumbent_metrics"]
    assert len(result["diagnostic_top_candidates"]) == 3
    assert result["deployable_candidates"] == []
    assert len(result["review_shortlist_sha256"]) == 64
    assert all(c["deployment_eligible"] is False for c in result["diagnostic_top_candidates"])
    assert all(c["status"] == "HIGH_OVERFITTING_RISK" for c in result["diagnostic_top_candidates"])
    shortlist = load_json(Path(result["review_shortlist"]))
    assert shortlist["shortlist_sha256"] == result["review_shortlist_sha256"]
    assert shortlist["holdout_opened"] is False
    assert result.get("paper_deployment") is None
    assert result["production_authority"] is False


@pytest.mark.parametrize("kind", ["NO_MEANINGFUL_IMPROVEMENT", "CHALLENGER_BETTER_BUT_NOT_ROBUST"])
def test_review_scientific_rejection_still_reports_top3(tmp_path, kind):
    optimizer, ledger, *_ = _terminal_workspace(tmp_path, status_kind=kind, count=6)
    result = optimizer._terminal(ledger, "GBP_USD", "NO_VALID_CANDIDATE", kind)
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert len(result["diagnostic_top_candidates"]) == 3
    assert result["deployable_candidates"] == []
    assert result["review_shortlist_sha256"]
    assert result["production_authority"] is False


def test_review_zero_evaluated_candidates_may_return_empty_diagnostic(tmp_path):
    optimizer, ledger, *_ = _terminal_workspace(tmp_path, count=0)
    result = optimizer._terminal(ledger, "GBP_USD", "NO_VALID_CANDIDATE", "NO_EVALUATED_CANDIDATES")
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert result["diagnostic_top_candidates"] == []
    assert result["deployable_candidates"] == []
    assert result["review_shortlist_sha256"]
    assert result["production_authority"] is False
'''
    test_path.write_text(text, encoding='utf-8')

# Ensure imports required by the appended integration tests exist.
text = test_path.read_text(encoding='utf-8')
text = text.replace(
    '    KEEP_INCUMBENT,\n',
    '    KEEP_INCUMBENT,\n    ReviewBeforeHoldoutOptimizer,\n    V3Ledger,\n',
    1,
)
test_path.write_text(text, encoding='utf-8')

# 5) Remote snapshot regression coverage.
remote_test = Path('test_automation_v3_remote_runner.py')
rt = remote_test.read_text(encoding='utf-8')
if 'test_snapshot_surfaces_review_diagnostics_on_scientific_terminal' not in rt:
    rt += r'''


def test_snapshot_surfaces_review_diagnostics_on_scientific_terminal(tmp_path, monkeypatch):
    import automation_v3_remote_worker as worker
    root = tmp_path
    base = root / "GBP_USD" / "autonomous_v3"
    base.mkdir(parents=True)
    sha = "c" * 40
    payload = {
        "runs": {"GBP_USD": {
            "instrument": "GBP_USD", "status": "NO_VALID_CANDIDATE", "final_outcome": "NO_VALID_CANDIDATE",
            "code_sha": sha, "lookback_attempts": [{"months": 1, "code_sha": sha}],
            "mode": "REVIEW_BEFORE_HOLDOUT_DEPLOY",
            "incumbent_metrics": {"resolved_binary": 46, "win_rate": 0.3478, "expectancy_r": -0.255, "profit_factor": 0.651},
            "diagnostic_top_candidates": [{"rank": 1, "candidate_id": "x", "deployment_eligible": False}],
            "deployable_candidates": [], "review_shortlist_sha256": "a" * 64,
            "production_authority": False,
        }}
    }
    worker._write_json(base / "automation_v3_state.json", payload)
    monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)
    snap = worker._snapshot(root, "GBP_USD", "33938856618")
    assert snap["terminal_state"] == "NO_VALID_CANDIDATE"
    assert snap["incumbent_metrics"]["resolved_binary"] == 46
    assert len(snap["diagnostic_top_candidates"]) == 1
    assert snap["deployable_candidates"] == []
    assert snap["shortlist_sha256"] == "a" * 64
    assert snap["paper_deployment_status"] is None
    assert snap["production_authority"] is False
'''
    remote_test.write_text(rt, encoding='utf-8')
