from server import _shadow_model_acceptance

def test_ml_acceptance_rejects_worse_than_random_or_baseline():
    r=_shadow_model_acceptance({'roc_auc':0.469,'accuracy':0.576,'baseline_accuracy':0.676})
    assert r['accepted'] is False
    assert 'roc_auc_not_above_random' in r['reason']
    assert 'accuracy_below_majority_baseline' in r['reason']

def test_ml_acceptance_requires_both_conditions():
    assert _shadow_model_acceptance({'roc_auc':0.61,'accuracy':0.70,'baseline_accuracy':0.68})['accepted'] is True
    assert _shadow_model_acceptance({'roc_auc':0.61,'accuracy':0.60,'baseline_accuracy':0.68})['accepted'] is False
