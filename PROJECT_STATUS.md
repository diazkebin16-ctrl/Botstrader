# BotsTrader Project Status

## Authoritative local baseline
- Version: V3.35.2 LOCAL HARDENED
- Environment target: PAPER / OANDA Practice
- Primary instrument currently under observation: EUR/USD
- Production/Railway code must not be patched ad hoc. Changes are made to the local master, tested, packaged, then deployed.

## Locally applied hardening
- OANDA dependent-stop replacement uses an explicit JSON `body=body` request.
- Trade-management R thresholds use broker-confirmed fill price when available.
- Protective-order failures are persisted/observable and retried on later scans.
- New-entry blackouts in `America/New_York`: 07:00-10:00 ET and 15:00-19:00 ET.
- Managed positions continue normal management during entry-blackout windows.
- Weekday rollover flattening window is 16:50-19:00 ET. This closes pre-rollover positions without killing legitimate trades opened once the 19:00 ET evening window reopens.
- Synchronous outcome resolvers, research refreshes, model refresh work, system evaluation, governance, smart-execution observability and ensemble observability are offloaded from the asyncio event-loop thread.
- Regression guard prevents direct POST/PUT/PATCH `req()` calls from passing request bodies positionally.

## Strategy-change boundary
Do not create new strategy exceptions or filters from a single WIN/LOSS. Indicator research remains OFFLINE/SHADOW until repeated evidence and proper validation support a change.

## Historical indicator audit
The currently available local databases contain only two broker-evidenced executed trades. Do not substitute thousands of shadow/signals for the missing executed population. Recover older broker/DB evidence before certifying an executed-trade Indicator Discrimination Audit.

## External/runtime validation still required after deployment
- Confirm Railway starts with Security Manager READY.
- Measure real event-loop lag after deployment.
- Confirm OANDA Practice accepts protective-stop replacement during a real PAPER trade.
- Confirm 07:00-10:00 and 15:00-19:00 ET entry gates using runtime timestamps.
- Confirm 16:50 ET flattening closes open managed positions and that post-19:00 ET trades remain open normally.

## Public GitHub safety
Do not commit `.env`, credentials, runtime DBs, virtual environments, caches, logs, generated archives or large audit artifacts. Review the repository for secrets before every first public synchronization.
