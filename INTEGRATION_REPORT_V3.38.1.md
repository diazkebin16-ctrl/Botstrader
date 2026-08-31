# BotsTrader V3.38.1 EUR+GBP Forward + Post-Fill Integration Report

## Identity
- Base V3.38 SHA-256: `9903e4226bef9390de16cb6528a780460c7abb42a55a0991809e1efbe65defba`
- Post-fill source SHA-256: `9b54c4991dd3fd297a0f45f648d1a334ab23d3007b6b569cf388864ac58c62a5`
- Merge method: differential/semantic transfer; V3.38.0 remained the base. No whole-file overwrite from the V3.37 source was used.

## Post-fill source audit
Comparison of the hardened source against canonical V3.37.0 isolated production changes to `server.py` plus the added `test_post_fill_protection_reanchor_v3370.py`. Cache/bytecode/runtime DB artifacts in the source package were not merged.

Functions added/modified by the post-fill hardening:
- `_protection_price_tolerance`
- `post_fill_protection_geometry`
- `replace_trade_protection`
- `verify_trade_protection`
- `_post_fill_protection_observability`
- `reanchor_post_fill_protection`
- `record_trade_memory_entry`
- `register_trade_management`
- `scan`
- `execute_ranked_candidate`

## Reanchor invariant
The execution path remains protected at submission with `stopLossOnFill` and `takeProfitOnFill`. After broker-confirmed MARKET fill, planned risk/reward distances are re-anchored around the actual fill, normalized to instrument precision, both protective orders are replaced in one PUT, and the broker state is read back and compared to the expected prices. Trade management and trade memory use broker-observed effective geometry only when that state is available/confirmed.

## Fail-closed behavior
- Invalid/non-finite/zero-distance or rounding-inverted geometry: `INVALID_GEOMETRY`, CRITICAL safe mode, no invented geometry.
- PUT timeout/transport ambiguity: `UNKNOWN`, no retry of the protection write, CRITICAL safe mode, broker GET observation only.
- Known write rejection: `FAILED`, no retry, CRITICAL safe mode; original on-fill protections remain the safety baseline.
- Verification mismatch/missing leg: `VERIFY_MISMATCH`/`PROTECTION_ERROR`, no false confirmation, CRITICAL safe mode.
- No trade ID: not treated as confirmed protection.
- No duplicate reanchor submission: replacement is one atomic PUT containing both stop and target.

## V3.38 strategy preservation
`EUR_PHASE2_FORWARD_V1` and `GBP_PHASE2_FORWARD_V1` strategy functions/gates are unchanged from the V3.38 base. Historical equivalence was rerun after the merge and remained exact.

EUR Discovery: 27 WIN kept / 69 LOSS kept / 0 WIN blocked / 10 LOSS blocked.
EUR Holdout: 14 WIN kept / 30 LOSS kept / 2 WIN blocked / 7 LOSS blocked.
GBP Discovery: 20 WIN kept / 43 LOSS kept / 5 WIN blocked / 50 LOSS blocked.
GBP Holdout: 22 WIN kept / 38 LOSS kept / 4 WIN blocked / 22 LOSS blocked.

## Validation
- Original post-fill tests on source: 24 passed / 0 failed.
- Original post-fill tests after merge: 24 passed / 0 failed.
- Post-fill + V3.38.1 integration tests: 32 passed / 0 failed.
- Cross-regression selected safety/security/recovery/trade-management/time/forward suites: 116 passed / 0 failed.
- Full real-clock regression: 437 passed / 3 failed. The three failures are the same time-dependent `GLOBAL_ENTRY_TIME_GATE` failures reproduced byte-for-byte on the unmodified V3.38 base while the clock was in the 15:00-19:00 ET blackout.
- Controlled off-blackout recheck of those same three tests: 3 passed / 0 failed, using an external temporary validation shim that is NOT included in the candidate.
- compileall: PASS.

No regression-specific production or test workaround was added. The time-gate implementation itself remains unchanged.

## Authority
No Railway, OANDA, GitHub remote, deployment, leverage, hard-risk, position-sizing policy, global time gate, portfolio/correlation control, recovery semantics, duplicate protection, worker model, broker metadata authority, IBKR authority, or unrelated instrument strategy was changed.
