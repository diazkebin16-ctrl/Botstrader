from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"{path}: expected one match for followup, found {text.count(old)}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")

# Preserve established negative-expectancy semantic labels while keeping the
# orthogonal STANDARD/EXPERIMENTAL confidence class.
replace_once(
    "research_phase2.py",
    '        "paper_candidate_classification":paper_class,\n'
    '        "relative_improvement_classification":relative_label,\n',
    '        "paper_candidate_classification":relative_label,\n'
    '        "confidence_candidate_classification":paper_class,\n'
    '        "relative_improvement_classification":relative_label,\n',
)

# Static cleanup after the policy transformation. The decision-gate robust
# value remains because it drives diagnostic_state. Only the post-holdout copy
# becomes redundant after candidate_record no longer re-vetoes MEDIUM warnings.
unused_candidate_record_robust = (
    '    robust = (\n'
    '        (holdout.get("directional_stability") or {}).get("stable") is True\n'
    '        and (holdout.get("temporal_stability") or {}).get("stable") is True\n'
    '        and (holdout.get("sensitivity") or {}).get("classification") != "FRAGILE"\n'
    '        and (holdout.get("walk_forward_stability") or {}).get("status") == "PASS"\n'
    '    )\n'
)
replace_once("research_phase2.py", unused_candidate_record_robust, "")
replace_once("test_automation_v3_adaptive_paper_confidence.py", "import copy\n", "")

# Historical tests that encoded all instability as fatal are policy tests, not
# immutable data/safety tests. Update them to the governed confidence contract.
replace_once(
    "test_automation_v3_incumbent_challenger.py",
    "def test_stability_fail_blocks(): assert gate(-.1,-.1,directional={\"stable\":False})['decision']=='REJECT'\n",
    "def test_stability_fail_becomes_experimental_not_standard():\n"
    "    result=gate(-.1,-.1,directional={\"stable\":False})\n"
    "    assert result['decision']=='FREEZE_ELIGIBLE'\n"
    "    assert result['confidence_class']=='EXPERIMENTAL'\n"
    "    assert result['experimental'] is True\n",
)
replace_once(
    "test_automation_v3_incumbent_challenger.py",
    "    assert gate_result['decision']=='REJECT'\n"
    "    assert gate_result['diagnostic_state']=='CHALLENGER_BETTER_BUT_NOT_ROBUST'\n",
    "    assert gate_result['decision']=='FREEZE_ELIGIBLE'\n"
    "    assert gate_result['confidence_class']=='EXPERIMENTAL'\n"
    "    assert gate_result['experimental'] is True\n",
)
