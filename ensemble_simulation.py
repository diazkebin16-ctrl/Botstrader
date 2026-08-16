from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone, timedelta
import json, random, statistics, tempfile, math
from ensemble_engine import EnsembleEngine


def signal(mid,direction,conf,family,deps,edge,ts,regime='HIGH_VOLATILITY_TREND',strength=.85,status='ONLINE'):
    return {
        'strategy_id':mid,'strategy_version':'sim_v1','symbol':'EUR_USD','timestamp':ts,
        'direction':direction,'confidence':conf,'expected_edge':edge,'market_regime':regime,
        'time_horizon':'INTRADAY','signal_strength':strength,'risk_characteristics':{},'data_quality':1.0,
        'family':family,'input_dependencies':deps,'role':'DIRECTIONAL' if direction!='ABSTAIN' else 'CONTEXT',
        'ttl_seconds':300,'status':status,
    }


def critical_scenario()->dict:
    db=tempfile.mktemp(suffix='.db')
    e=EnsembleEngine(db,'3.25',mode='SHADOW',max_model_weight=.40,max_family_weight=.50,
                     min_sample_size=20,correlation_threshold=.70,weight_cooldown_hours=24,min_active_directional=2)
    e.ensure_schema();ts=datetime.now(timezone.utc).isoformat()
    signals=[
        signal('TREND_A','LONG',.90,'TREND_FAMILY',['M15_EMA','M5_MOMENTUM','M1_CONFIRM'],5,ts),
        signal('TREND_B','LONG',.88,'TREND_FAMILY',['M15_EMA','M5_MOMENTUM','M1_CONFIRM'],5,ts),
        signal('TREND_C','LONG',.86,'TREND_FAMILY',['M15_EMA','M5_MOMENTUM','M1_CONFIRM'],5,ts),
        signal('BREAKOUT','LONG',.72,'BREAKOUT_FAMILY',['RANGE_BREAK','VOLUME_PROXY'],4,ts),
        signal('MEAN_REVERSION','SHORT',.80,'MEAN_REVERSION_FAMILY',['Z_SCORE','DISTANCE_FROM_MEAN'],4.5,ts),
        signal('VOL_CONTEXT','ABSTAIN',.90,'VOLATILITY_FAMILY',['ATR','REALIZED_VOL'],None,ts),
    ]
    for s in signals:
        e.register_model(s['strategy_id'],s['strategy_version'],s['family'],s['role'],s['input_dependencies'],s['time_horizon'])
    out=e.evaluate(signals,method='REGIME_WEIGHTED',regime='HIGH_VOLATILITY_TREND',execution_cost=1.2,
                   current_system_direction='LONG',current_system_confidence=.75,current_executed=False)
    naive_long=4;naive_short=1
    trend_family=out['family_weight_info']['family_totals'].get('TREND_FAMILY',0)
    meanrev=next(x for x in out['model_contributions'] if x['strategy_id']=='MEAN_REVERSION')
    return {
        'scenario':'5? requested example uses 3 correlated trend LONG + breakout LONG + mean-reversion SHORT + abstain',
        'naive_directional_vote':{'LONG':naive_long,'SHORT':naive_short,'ABSTAIN':1,'naive_long_share':naive_long/(naive_long+naive_short)},
        'correlation_aware_output':out,
        'proof':{
            'trend_family_total_weight':trend_family,
            'max_family_weight':e.max_family_weight,
            'trend_family_is_capped':trend_family<=e.max_family_weight+1e-12,
            'independent_short_weight':meanrev['weight'],
            'independent_short_not_erased':meanrev['weight']>0,
            'ensemble_confidence':out['ensemble_confidence'],
            'not_naive_80_percent_confidence':out['ensemble_confidence']<.8,
            'risk_increase_authority':False,
            'direct_execution_authority':False,
        }
    }


def run_simulation(seed=170025,cycles=400)->dict:
    rng=random.Random(seed);db=tempfile.mktemp(suffix='.db')
    e=EnsembleEngine(db,'3.25',mode='SHADOW',max_model_weight=.40,max_family_weight=.50,
                     min_sample_size=30,correlation_threshold=.70,weight_change_limit=.10,
                     weight_cooldown_hours=24,min_active_directional=2)
    e.ensure_schema()
    naive=[];ensemble=[];abstains=0;diversities=[];agreements=[];confidences=[]
    for i in range(cycles):
        ts=datetime.now(timezone.utc).isoformat()
        regime=rng.choice(['BULLISH_TREND','BEARISH_TREND','RANGE','HIGH_VOLATILITY'])
        latent=1 if regime=='BULLISH_TREND' else -1 if regime=='BEARISH_TREND' else rng.choice([-1,1])
        # Trend trio shares one noisy latent observation -> deliberately correlated.
        trend_view=latent if rng.random()<.68 else -latent
        sigs=[]
        for n in range(3):
            sigs.append(signal(f'TREND_{n}', 'LONG' if trend_view>0 else 'SHORT', rng.uniform(.65,.9),
                               'TREND_FAMILY',['M15_EMA','M5_MOMENTUM','M1_CONFIRM'],rng.uniform(2,6),ts,regime))
        breakout=latent if rng.random()<.62 else -latent
        meanrev=(-latent if regime=='RANGE' else latent) if rng.random()<.58 else -(-latent if regime=='RANGE' else latent)
        sigs.append(signal('BREAKOUT','LONG' if breakout>0 else 'SHORT',rng.uniform(.55,.82),'BREAKOUT_FAMILY',['RANGE_BREAK','VOLUME_PROXY'],rng.uniform(1.5,5),ts,regime))
        sigs.append(signal('MEANREV','LONG' if meanrev>0 else 'SHORT',rng.uniform(.55,.82),'MEAN_REVERSION_FAMILY',['Z_SCORE','DISTANCE_FROM_MEAN'],rng.uniform(1.5,5),ts,regime))
        sigs.append(signal('VOL_CONTEXT','ABSTAIN',.8,'VOLATILITY_FAMILY',['ATR','REALIZED_VOL'],None,ts,regime))
        for s in sigs:
            e.register_model(s['strategy_id'],s['strategy_version'],s['family'],s['role'],s['input_dependencies'],s['time_horizon'])
        # Synthetic resolved direction generated independently enough to test weighting mechanics, not market alpha.
        actual=latent if rng.random()<.64 else -latent
        dirs=[1 if x['direction']=='LONG' else -1 for x in sigs if x['direction'] in ('LONG','SHORT')]
        naive_dir=1 if sum(dirs)>0 else -1 if sum(dirs)<0 else 0
        naive.append(1 if naive_dir==actual else -1 if naive_dir else 0)
        out=e.evaluate(sigs,method='REGIME_WEIGHTED',regime=regime,execution_cost=.8,
                       current_system_direction='LONG' if naive_dir>0 else 'SHORT',current_system_confidence=.7,current_executed=False)
        ed=1 if out['ensemble_direction']=='LONG' else -1 if out['ensemble_direction']=='SHORT' else 0
        if ed==0:abstains+=1;ensemble.append(0)
        else:ensemble.append(1 if ed==actual else -1)
        diversities.append(out['diversity_score']);agreements.append(out['agreement_score']);confidences.append(out['ensemble_confidence'])
        # Resolve only signals whose own directional prediction has an observed synthetic outcome.
        c=e.conn();rows=c.execute('SELECT signal_id,strategy_id,direction FROM ensemble_signals WHERE ensemble_cycle_id=?',(out['ensemble_cycle_id'],)).fetchall();c.close()
        for row in rows:
            if row['direction'] not in ('LONG','SHORT'):continue
            label=1 if (row['direction']=='LONG' and actual>0) or (row['direction']=='SHORT' and actual<0) else 0
            e.resolve_signal(row['signal_id'],label,1.0 if label else -1.0)
    def stats(xs):
        active=[x for x in xs if x!=0]
        wins=sum(x>0 for x in active);loss=sum(x<0 for x in active)
        return {'cycles':len(xs),'active_decisions':len(active),'abstentions':len(xs)-len(active),
                'accuracy_active':wins/max(1,len(active)),'expectancy_proxy':statistics.mean(active) if active else None,
                'total_proxy':sum(xs),'profit_factor_proxy':wins/max(1,loss)}
    return {
        'version':'3.25','seed':seed,'cycles':cycles,'evidence_type':'SEEDED_SYNTHETIC_ENSEMBLE_MECHANICS_SIMULATION',
        'critical_scenario':critical_scenario(),'naive_majority':stats(naive),'correlation_aware_ensemble':stats(ensemble),
        'ensemble_diagnostics':{'abstain_rate':abstains/cycles,'average_diversity':statistics.mean(diversities),
                                'average_agreement':statistics.mean(agreements),'average_confidence':statistics.mean(confidences)},
        'interpretation':'Synthetic simulation validates correlation-aware weighting, abstention and calibration mechanics. It is not evidence of live ensemble alpha.',
        'ensemble_value_added_live_status':'NO_ENSEMBLE_ADVANTAGE_DETECTED_UNTIL_SHADOW_OUTCOMES_ACCUMULATE',
        'production_activation':False,
    }

if __name__=='__main__':
    import sys
    result=run_simulation()
    if len(sys.argv)>1:Path(sys.argv[1]).write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))
