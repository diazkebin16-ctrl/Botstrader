import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

import validation_pipeline as vp


def make_row(i, net, conf=0.8, regime='BULL_TREND', vol='NORMAL'):
    entry=datetime(2099,1,1,tzinfo=timezone.utc)+timedelta(hours=i*2)
    return {
        'trade_id':f'T{i}', 'entry_ts':entry.isoformat(),
        'exit_ts':(entry+timedelta(minutes=30)).isoformat(),
        'net_result':float(net),'realized_r':float(net)/10,
        'fees_total':0.1,'entry_slippage_pips':0.2,
        'strategy_confidence_entry':conf,'director_confidence_entry':0.9,
        'market_regime_entry':regime,'volatility_state_entry':vol,
    }


CAND={'change_type':'MIN_CONFIDENCE','parameter_name':'execution_min_confidence',
      'current_value':0.65,'proposed_value':0.75}


class ValidationPipelineTests(unittest.TestCase):
    def test_temporal_split_is_strict(self):
        rows=[make_row(i,1 if i%3 else -1) for i in range(120)]
        sp=vp.strict_temporal_split(rows,0.6,0.2,30)
        self.assertEqual(sp['status'],'OK')
        self.assertLess(max(datetime.fromisoformat(x['exit_ts']) for x in sp['train']),
                        min(datetime.fromisoformat(x['entry_ts']) for x in sp['validation']))
        self.assertLess(max(datetime.fromisoformat(x['exit_ts']) for x in sp['validation']),
                        min(datetime.fromisoformat(x['entry_ts']) for x in sp['test']))

    def test_candidate_predicate_has_no_lookahead(self):
        row=make_row(1,10,0.82)
        before=vp.candidate_passes(CAND,row)
        row.update(net_result=-999,realized_r=-99,exit_ts='2999-01-01T00:00:00+00:00',mfe_r=999,mae_r=999)
        self.assertEqual(before,vp.candidate_passes(CAND,row))

    def test_robust_candidate_requires_paper(self):
        rows=[]
        for i in range(240):
            high=i%3!=0
            net=(2 if i%5<4 else -1) if high else (1 if i%5<2 else -1)
            rows.append(make_row(i,net,0.82 if high else 0.65,'BULL_TREND' if i%4 else 'RANGE'))
        r=vp.run_historical_validation(rows,CAND,60,20,20,3,30,20,200,9)
        self.assertEqual(r['status'],'PAPER_TRADING_REQUIRED')
        self.assertNotEqual(r['status'],'READY_FOR_REVIEW')

    def test_overfit_candidate_is_rejected(self):
        rows=[]
        for i in range(240):
            high=i%3!=0
            net=(3 if high else -1) if i<130 else (-3 if high else 1)
            rows.append(make_row(i,net,0.82 if high else 0.65))
        r=vp.run_historical_validation(rows,CAND,60,20,20,3,30,20,200,10)
        self.assertNotEqual(r['status'],'PAPER_TRADING_REQUIRED')

    def test_high_profit_extreme_drawdown_can_fail(self):
        rows=[]
        for i in range(180):
            high=i%3==0
            net=(18 if i%6==0 else -14) if high else (0.8 if i%5<3 else -0.6)
            rows.append(make_row(i,net,0.9 if high else 0.6))
        r=vp.run_historical_validation(rows,CAND,60,20,20,3,0,10,200,44)
        self.assertEqual(r['status'],'FAILED')
        self.assertTrue(any('drawdown extreme' in x for x in r['reasons']))

    def test_monte_carlo_reproducible(self):
        rows=[make_row(i,2 if i%4 else -1,0.82) for i in range(100)]
        self.assertEqual(vp.monte_carlo(rows,CAND,150,123),vp.monte_carlo(rows,CAND,150,123))

    def test_no_auto_deploy_constants(self):
        server=Path(__file__).with_name('server.py').read_text(encoding='utf-8')
        self.assertIn('VALIDATION_AUTO_DEPLOY = False',server)
        self.assertIn('VALIDATION_MAX_STATE = "READY_FOR_REVIEW"',server)
        start=server.index('def execution_decision')
        end=server.find('\ndef ',start+5)
        body=server[start:end].lower()
        self.assertNotIn('candidate_registry',body)
        self.assertNotIn('candidate_validation',body)


if __name__=='__main__':
    unittest.main(verbosity=2)
