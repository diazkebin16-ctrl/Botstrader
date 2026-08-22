# Botstrader v3.33 — Research Validation

## Purpose

This release adds a broker-free, read-only research validation layer. It does not relax execution gates or change live order authority.

## Added

- `research_validation.py`
  - normalizes CLOSED `trade_memory` rows using only frozen pre-trade context for policy decisions;
  - collapses correlated fills into independent directional episodes;
  - computes expectancy in R, win rate, profit factor, max drawdown, net R, normal-approximation and bootstrap expectancy intervals;
  - performs chronological walk-forward optimization where parameters are selected on train only and frozen for the next test window;
  - exposes explicit scope/causal limitations so executed-trade analysis is not mislabeled as a counterfactual strategy backtest;
  - includes research-only session opposition/alignment policies.
- `run_research_validation.py`
  - read-only CLI against a Railway SQLite database;
  - writes a JSON validation report without mutating strategy or production state.
- `test_research_validation.py`
  - verifies pre-trade/outcome separation;
  - verifies consecutive-gap episode formation;
  - verifies no episode leakage across train/test;
  - verifies train-only parameter selection;
  - verifies core R metrics.

## Important limitation

`trade_memory` contains executed trades. It can validate whether a pre-trade policy is associated with better realized outcomes among those trades, but it cannot reconstruct missed entries, rejected signals, or the opposite direction. Therefore it cannot by itself prove v3.31-vs-v3.32 directional edge. A historical candle replay is still required for that stronger comparison.

## Production behavior

No change to order placement, Recovery, Security, Governance, Production Readiness, `minimum_rr`, or session-regime live scoring in this release.
