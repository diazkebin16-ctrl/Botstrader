from pathlib import Path


def replace_once(path, old, new):
    p=Path(path); text=p.read_text(encoding='utf-8')
    if text.count(old)!=1:
        raise SystemExit(f'{path}: expected one match for followup, found {text.count(old)}')
    p.write_text(text.replace(old,new,1),encoding='utf-8')

# Preserve established negative-expectancy semantic labels while keeping the
# orthogonal STANDARD/EXPERIMENTAL confidence class.
replace_once('research_phase2.py',
'''        "paper_candidate_classification":paper_class,
        "relative_improvement_classification":relative_label,
''',
'''        "paper_candidate_classification":relative_label,
        "confidence_candidate_classification":paper_class,
        "relative_improvement_classification":relative_label,
''')

# Historical tests that encoded "all instability is fatal" are policy tests,
# not immutable safety tests. Update them to the new governed confidence contract.
replace_once('test_automation_v3_incumbent_challenger.py',
'''def test_stability_fail_blocks(): assert gate(-.1,-.1,directional={"stable":False})['decision']=='REJECT'
''',
'''def test_stability_fail_becomes_experimental_not_standard():
    result=gate(-.1,-.1,directional={"stable":False})
    assert result['decision']=='FREEZE_ELIGIBLE'
    assert result['confidence_class']=='EXPERIMENTAL'
    assert result['experimental'] is True
''')
replace_once('test_automation_v3_incumbent_challenger.py',
'''    assert gate_result['decision']=='REJECT'
    assert gate_result['diagnostic_state']=='CHALLENGER_BETTER_BUT_NOT_ROBUST'
''',
'''    assert gate_result['decision']=='FREEZE_ELIGIBLE'
    assert gate_result['confidence_class']=='EXPERIMENTAL'
    assert gate_result['experimental'] is True
''')
