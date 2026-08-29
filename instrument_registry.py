from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Iterable, Optional


@dataclass(frozen=True)
class InstrumentMetadata:
    symbol: str
    display_precision: int
    pip_location: int
    trade_units_precision: int = 0
    minimum_trade_size: float = 1.0
    margin_rate: Optional[float] = None
    instrument_type: str = "CURRENCY"
    source: str = "FALLBACK"

    @property
    def pip_size(self) -> float:
        return float(Decimal(10) ** int(self.pip_location))

    @property
    def price_quantum(self) -> Decimal:
        return Decimal(1).scaleb(-int(self.display_precision))

    def format_price(self, value: float) -> str:
        q = Decimal(str(value)).quantize(self.price_quantum, rounding=ROUND_HALF_UP)
        return f"{q:.{int(self.display_precision)}f}"

    def normalize_units(self, units: float, *, allow_zero: bool = False) -> float:
        sign = -1.0 if float(units) < 0 else 1.0
        raw = abs(Decimal(str(units)))
        quantum = Decimal(1).scaleb(-int(self.trade_units_precision))
        normalized = raw.quantize(quantum, rounding=ROUND_DOWN)
        minimum = Decimal(str(self.minimum_trade_size or 0))
        if normalized < minimum:
            if allow_zero:
                normalized = Decimal(0)
            else:
                normalized = minimum
        return float(normalized) * sign

    def format_units(self, units: float, *, allow_zero: bool=False) -> str:
        normalized=Decimal(str(self.normalize_units(units,allow_zero=allow_zero)))
        precision=int(self.trade_units_precision)
        if precision<=0:
            return str(int(normalized))
        return f"{normalized:.{precision}f}"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "display_precision": self.display_precision,
            "pip_location": self.pip_location,
            "pip_size": self.pip_size,
            "trade_units_precision": self.trade_units_precision,
            "minimum_trade_size": self.minimum_trade_size,
            "margin_rate": self.margin_rate,
            "instrument_type": self.instrument_type,
            "source": self.source,
        }


# Safe offline defaults mirror OANDA's conventional FX metadata for these pairs.
# Runtime broker metadata replaces these values when available.
_DEFAULTS: Dict[str, InstrumentMetadata] = {
    "EUR_USD": InstrumentMetadata("EUR_USD", display_precision=5, pip_location=-4),
    "GBP_USD": InstrumentMetadata("GBP_USD", display_precision=5, pip_location=-4),
    # Future-proofing example: not enabled by default and not part of this release.
    "USD_JPY": InstrumentMetadata("USD_JPY", display_precision=3, pip_location=-2),
}


class InstrumentRegistry:
    def __init__(self, defaults: Optional[Iterable[InstrumentMetadata]] = None):
        self._items: Dict[str, InstrumentMetadata] = dict(_DEFAULTS)
        if defaults:
            for meta in defaults:
                self._items[self.normalize_symbol(meta.symbol)] = meta

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        return str(symbol or "").strip().upper().replace("/", "_")

    def get(self, symbol: str) -> InstrumentMetadata:
        key = self.normalize_symbol(symbol)
        if key in self._items:
            return self._items[key]
        # Unknown FX instruments get a conservative convention fallback only for
        # offline/shadow calculations. Broker metadata should replace this before
        # any new instrument is PAPER-enabled.
        if "_" in key:
            quote = key.split("_")[-1]
            if quote == "JPY":
                meta = InstrumentMetadata(key, display_precision=3, pip_location=-2)
            else:
                meta = InstrumentMetadata(key, display_precision=5, pip_location=-4)
            self._items[key] = meta
            return meta
        raise KeyError(f"Unknown instrument: {symbol}")

    def update_from_oanda(self, payload: Dict[str, Any]) -> Dict[str, InstrumentMetadata]:
        updated: Dict[str, InstrumentMetadata] = {}
        for item in (payload or {}).get("instruments") or []:
            symbol = self.normalize_symbol(item.get("name"))
            if not symbol:
                continue
            current = self.get(symbol)
            meta = replace(
                current,
                display_precision=int(item.get("displayPrecision", current.display_precision)),
                pip_location=int(item.get("pipLocation", current.pip_location)),
                trade_units_precision=int(item.get("tradeUnitsPrecision", current.trade_units_precision)),
                minimum_trade_size=float(item.get("minimumTradeSize", current.minimum_trade_size)),
                margin_rate=float(item["marginRate"]) if item.get("marginRate") is not None else current.margin_rate,
                instrument_type=str(item.get("type") or current.instrument_type),
                source="OANDA",
            )
            self._items[symbol] = meta
            updated[symbol] = meta
        return updated

    def snapshot(self, symbols: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
        keys = [self.normalize_symbol(x) for x in symbols] if symbols is not None else sorted(self._items)
        return {key: self.get(key).as_dict() for key in keys}
