# Step 18 — Dynamic Capital & Risk Allocation (V3.26, SHADOW)

## Pre-implementation audit
The existing system already has conservative risk sizing, not a portfolio allocator. `adaptive_risk_recommendation()` starts from `risk.base_fraction`, applies only <=1 multipliers, and caps each recommendation by max trade, max strategy and max portfolio fractions. Broker context tracks NAV, margin, drawdown, open risk and a conservative shared-currency correlation proxy. Deployment/production stages also cap exposure. Smart Execution may reduce authorized quantity but cannot increase it. Ensemble V3.25 discounts correlated evidence by family/model.

Current hard ceilings remain unchanged: max trade 1%, max strategy 3%, max portfolio 6%, max margin usage 50%, drawdown warning 5%, drawdown stop 10%, and max correlated positions 2 (environment defaults; managed configuration may only be equally or more restrictive). Step 18 does not alter these ceilings.

The main concentration already visible is conceptual: technical signals share the TREND_STRUCTURE family and many price/ATR inputs; FX positions sharing a currency are conservatively treated as correlated. There is not yet enough production history to claim a stable empirical cross-strategy covariance matrix, so the allocator accepts explicit/observed correlations and otherwise uses conservative family/cluster proxies rather than inventing independence.

## Architecture
`ENSEMBLE -> AI DIRECTOR -> CAPITAL ALLOCATION [SHADOW] -> RISK ENGINE (VETO) -> SMART EXECUTION -> BROKER`.

Capital Allocation has no signal authority, order authority or risk-limit authority. It distributes only an externally authorized risk budget and may leave any amount unused.

## Baselines
EQUAL_RISK, FIXED_RISK, VOLATILITY_WEIGHTED and PERFORMANCE_WEIGHTED are implemented before DYNAMIC. DYNAMIC uses expected net edge after Step-16 costs, reliability/calibration/sample size/stability, regime compatibility, drawdown, volatility/tail risk, execution quality, ensemble confidence, data quality and correlation.

## Core protections
- `ALLOCATED_RISK <= AUTHORIZED_TOTAL_RISK`.
- No martingale and no loss-recovery sizing.
- Maximum strategy/family/symbol/asset/directional/correlated-cluster caps.
- Correlated strategies are discounted and correlated clusters are capped.
- Portfolio Heat includes open risk, correlation, volatility, tail risk and drawdown; heat at/above the limit reduces allocation.
- Correlation stress forces pair correlations toward >=0.85 and applies a volatility stress multiplier.
- Low opportunity and Risk-Off can leave 100% of budget unused.
- Winners cannot take the portfolio: evidence, concentration and per-cycle change caps remain binding.
- Allocation changes have cooldown, max-change and turnover/churn measurement.
- Candidate policies are `SHADOW -> VALIDATION -> PAPER -> CANARY -> LIMITED_LIVE`; `auto_deploy=false`.
- Failure policy is LAST_KNOWN_SAFE_ALLOCATION or reduced activity; never maximum equal sizing.

## Decision trace
Every allocation version persists strategy risk, percentage, confidence, reliability-adjusted edge, marginal risk contribution, family, symbol, direction, portfolio heat, diversification, efficiency, correlations, stress result and applied limits.

## Alerts
PORTFOLIO_CONCENTRATION_HIGH / HIDDEN_CONCENTRATION_DETECTED, PORTFOLIO_HEAT_HIGH, ALLOCATION_CHURN_DETECTED, CORRELATION_SPIKE, RISK_BUDGET_EXCEEDED, REALLOCATION_COST_TOO_HIGH and LOW_OPPORTUNITY_ENVIRONMENT are supported by the decision result. V3.26 remains shadow-only, so these are evidence/monitoring outputs rather than autonomous production reallocations.

## Promotion rule
`DYNAMIC_ALLOCATION_VALUE_ADDED` is not assumed. It must outperform current allocation and the equal/volatility baselines after costs without materially worsening drawdown, concentration, turnover or tail risk. Until sufficient shadow evidence exists, status is `NO_DYNAMIC_ALLOCATION_ADVANTAGE / INSUFFICIENT_DATA`.
