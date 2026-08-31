# Known Limitations V3.38.1

1. This is not profitability certification. EUR/GBP remain forward-evidence experiments.
2. EUR learned research veto historical equivalence remains NOT_HISTORICALLY_RECONSTRUCTABLE.
3. Full regression was run during the 15:00-19:00 ET entry blackout, producing the same three time-dependent gate failures on both V3.38 base and merged candidate. All other tests passed, and a controlled off-blackout recheck of those three passed. No production/test change was made to suppress the real time gate.
4. Broker verification can observe uncertain state after an ambiguous write, but ambiguity is never promoted to confirmed success and the write is not retried.
