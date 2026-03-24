"""Interactive Brokers connector powered by ib_async."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.core.models import MarketQuote
from global_arbitrage.execution.broker import (
    BrokerAccountSnapshot,
    BrokerPosition,
    OrderReceipt,
    OrderSide,
)

_DURATION_PATTERN = re.compile(r"^(?P<count>\d+)(?P<unit>m|h|d|w|mo|y)$", re.IGNORECASE)
_BAR_SIZE_PATTERN = re.compile(r"^(?P<count>\d+)(?P<unit>m|h|d|w)$", re.IGNORECASE)


def _ib_duration(period: str) -> str:
    match = _DURATION_PATTERN.fullmatch(period.strip())
    if match is None:
        raise ValueError(f"Unsupported IB history period '{period}'.")
    count = int(match.group("count"))
    unit = match.group("unit").lower()
    if unit == "m":
        return f"{count * 60} S"
    if unit == "h":
        return f"{count * 3600} S"
    if unit == "d":
        return f"{count} D"
    if unit == "w":
        return f"{count} W"
    if unit == "mo":
        return f"{count} M"
    if unit == "y":
        return f"{count} Y"
    raise ValueError(f"Unsupported IB history period unit '{unit}'.")


def _ib_bar_size(interval: str) -> str:
    match = _BAR_SIZE_PATTERN.fullmatch(interval.strip())
    if match is None:
        raise ValueError(f"Unsupported IB history interval '{interval}'.")
    count = int(match.group("count"))
    unit = match.group("unit").lower()
    if unit == "m":
        suffix = "min" if count == 1 else "mins"
        return f"{count} {suffix}"
    if unit == "h":
        suffix = "hour" if count == 1 else "hours"
        return f"{count} {suffix}"
    if unit == "d":
        suffix = "day" if count == 1 else "days"
        return f"{count} {suffix}"
    if unit == "w":
        suffix = "week" if count == 1 else "weeks"
        return f"{count} {suffix}"
    raise ValueError(f"Unsupported IB history interval unit '{unit}'.")


@dataclass(frozen=True, slots=True)
class IBContractSpec:
    """Small serializable contract specification for IB instruments."""

    symbol: str
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    primary_exchange: str | None = None
    last_trade_date_or_contract_month: str | None = None
    local_symbol: str | None = None
    multiplier: str | None = None
    trading_class: str | None = None
    include_expired: bool = False
    con_id: int | None = None
    what_to_show: str | None = None
    use_rth: bool | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> IBContractSpec:
        return cls(
            symbol=str(payload["symbol"]),
            sec_type=str(payload.get("sec_type", "STK")),
            exchange=str(payload.get("exchange", "SMART")),
            currency=str(payload.get("currency", "USD")),
            primary_exchange=None
            if payload.get("primary_exchange") is None
            else str(payload["primary_exchange"]),
            last_trade_date_or_contract_month=None
            if payload.get("last_trade_date_or_contract_month") is None
            else str(payload["last_trade_date_or_contract_month"]),
            local_symbol=None if payload.get("local_symbol") is None else str(payload["local_symbol"]),
            multiplier=None if payload.get("multiplier") is None else str(payload["multiplier"]),
            trading_class=None
            if payload.get("trading_class") is None
            else str(payload["trading_class"]),
            include_expired=bool(payload.get("include_expired", False)),
            con_id=None if payload.get("con_id") is None else int(payload["con_id"]),
            what_to_show=None if payload.get("what_to_show") is None else str(payload["what_to_show"]),
            use_rth=None if payload.get("use_rth") is None else bool(payload["use_rth"]),
        )


class InteractiveBrokersConnector(MarketDataConnector):
    """Unified IB market data, history, execution, and account connector."""

    venue = "ib"

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 17,
        timeout: float = 4.0,
        readonly: bool = False,
        account: str | None = None,
        base_currency: str = "USD",
        market_data_type: int = 1,
        quote_wait_seconds: float = 1.0,
        order_wait_seconds: float = 5.0,
        contract_overrides: dict[str, IBContractSpec] | None = None,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout
        self.readonly = readonly
        self.account = account
        self.base_currency = base_currency
        self.market_data_type = market_data_type
        self.quote_wait_seconds = quote_wait_seconds
        self.order_wait_seconds = order_wait_seconds
        self.contract_overrides = dict(contract_overrides or {})
        self._module: Any | None = None
        self._ib: Any | None = None
        self._contracts: dict[str, Any] = {}
        self._tickers: dict[str, Any] = {}

    def connect(self) -> None:
        module = self._require_module()
        ib = module.IB()
        ib.connect(
            self.host,
            self.port,
            clientId=self.client_id,
            timeout=self.timeout,
            readonly=self.readonly,
            account=self.account or "",
        )
        if self.market_data_type:
            ib.reqMarketDataType(self.market_data_type)
        self._ib = ib

    def disconnect(self) -> None:
        if self._ib is not None:
            self._ib.disconnect()
        self._ib = None
        self._contracts.clear()
        self._tickers.clear()

    def register_contract(self, alias: str, spec: IBContractSpec) -> None:
        self.contract_overrides[alias] = spec
        self._contracts.pop(alias, None)
        self._tickers.pop(alias, None)

    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        ib = self._require_ib()
        contract = self._qualify_contract(symbol)
        ticker = self._tickers.get(symbol)
        if ticker is None:
            ticker = ib.reqMktData(contract, "", False, False)
            self._tickers[symbol] = ticker
            if self.quote_wait_seconds > 0.0:
                ib.sleep(self.quote_wait_seconds)
        if not self._ticker_has_price(ticker):
            snapshots = ib.reqTickers(contract)
            if snapshots:
                ticker = snapshots[0]
                self._tickers[symbol] = ticker
        bid = self._safe_float(getattr(ticker, "bid", None))
        ask = self._safe_float(getattr(ticker, "ask", None))
        last = self._resolve_market_price(ticker)
        if last is None:
            raise RuntimeError(f"IB quote for '{symbol}' has no usable market price.")
        quote_currency = currency or getattr(contract, "currency", None) or "USD"
        timestamp = self._resolve_ticker_timestamp(ticker)
        return MarketQuote(
            venue=self.venue,
            symbol=symbol,
            last=last,
            bid=bid,
            ask=ask,
            currency=str(quote_currency),
            timestamp=timestamp,
            source="IB Gateway",
            metadata={
                "contract_symbol": str(getattr(contract, "symbol", symbol)),
                "contract_exchange": str(getattr(contract, "exchange", "")),
                "market_data_type": self.market_data_type,
            },
        )

    def history(
        self,
        symbol: str,
        *,
        period: str = "2y",
        interval: str = "1d",
        currency: str | None = None,
    ) -> pd.DataFrame:
        ib = self._require_ib()
        module = self._require_module()
        contract = self._qualify_contract(symbol)
        spec = self._resolve_spec(symbol, currency)
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=_ib_duration(period),
            barSizeSetting=_ib_bar_size(interval),
            whatToShow=spec.what_to_show or self._default_what_to_show(spec),
            useRTH=bool(spec.use_rth) if spec.use_rth is not None else False,
        )
        frame = module.util.df(bars)
        if frame.empty:
            raise ValueError(f"IB returned no history for '{symbol}'.")
        frame.columns = [str(column).strip().lower() for column in frame.columns]
        if "date" not in frame.columns:
            raise ValueError(f"IB historical frame for '{symbol}' is missing a date column.")
        timestamps = pd.to_datetime(frame["date"], utc=True)
        frame["timestamp"] = timestamps.dt.tz_localize(None)
        frame = frame.set_index("timestamp")
        if "volume" not in frame.columns:
            frame["volume"] = 0.0
        normalized = frame[["open", "high", "low", "close", "volume"]].astype(float)
        normalized["currency"] = currency or spec.currency
        return normalized

    def submit_market_order(self, *, symbol: str, side: OrderSide, quantity: float) -> OrderReceipt:
        ib = self._require_ib()
        module = self._require_module()
        contract = self._qualify_contract(symbol)
        order = module.MarketOrder(side.value, float(quantity))
        trade = ib.placeOrder(contract, order)
        if self.order_wait_seconds > 0.0:
            for _ in ib.loopUntil(lambda: trade.isDone(), timeout=self.order_wait_seconds):
                pass
        order_status = getattr(trade, "orderStatus", None)
        status = str(getattr(order_status, "status", "")) or "Submitted"
        filled_price = self._safe_float(getattr(order_status, "avgFillPrice", None))
        order_id = getattr(getattr(trade, "order", None), "orderId", None)
        return OrderReceipt(
            symbol=symbol,
            side=side.value,
            quantity=float(quantity),
            status=status.upper(),
            venue=self.venue,
            order_id=None if order_id is None else str(order_id),
            filled_price=filled_price,
            message=str(getattr(order_status, "status", "")),
            metadata={"contract_exchange": str(getattr(contract, "exchange", ""))},
        )

    def positions(self) -> list[BrokerPosition]:
        ib = self._require_ib()
        account = self._resolve_account()
        portfolio_items = ib.portfolio(account or "")
        positions: list[BrokerPosition] = []
        for item in portfolio_items:
            positions.append(
                BrokerPosition(
                    venue=self.venue,
                    symbol=str(item.contract.symbol),
                    quantity=float(item.position),
                    currency=str(getattr(item.contract, "currency", "USD")),
                    average_price=self._safe_float(getattr(item, "averageCost", None)),
                    market_price=self._safe_float(getattr(item, "marketPrice", None)),
                    market_value=self._safe_float(getattr(item, "marketValue", None)),
                    unrealized_pnl=self._safe_float(getattr(item, "unrealizedPNL", None)),
                    realized_pnl=self._safe_float(getattr(item, "realizedPNL", None)),
                    metadata={"exchange": str(getattr(item.contract, "exchange", ""))},
                )
            )
        return positions

    def account_snapshot(self) -> BrokerAccountSnapshot:
        ib = self._require_ib()
        account = self._resolve_account()
        summary_items = ib.accountSummary(account or "")
        summary = {
            (str(item.tag), str(item.currency or "")): self._safe_float(item.value)
            for item in summary_items
        }
        return BrokerAccountSnapshot(
            venue=self.venue,
            account_id=account,
            currency=self.base_currency,
            timestamp=pd.Timestamp.utcnow().tz_localize(None),
            balance=self._summary_value(summary, "TotalCashValue", "BASE"),
            equity=self._summary_value(summary, "NetLiquidation", "BASE"),
            available_funds=self._summary_value(summary, "AvailableFunds", "BASE"),
            buying_power=self._summary_value(summary, "BuyingPower", "BASE"),
            unrealized_pnl=self._summary_value(summary, "UnrealizedPnL", "BASE"),
            realized_pnl=self._summary_value(summary, "RealizedPnL", "BASE"),
            metadata={"tags": {f"{tag}:{ccy}": value for (tag, ccy), value in summary.items()}},
        )

    def _resolve_account(self) -> str | None:
        if self.account:
            return self.account
        ib = self._require_ib()
        accounts = ib.managedAccounts()
        return None if not accounts else str(accounts[0])

    def _qualify_contract(self, symbol: str):
        cached = self._contracts.get(symbol)
        if cached is not None:
            return cached
        ib = self._require_ib()
        module = self._require_module()
        spec = self._resolve_spec(symbol)
        contract = module.Contract(
            symbol=spec.symbol,
            secType=spec.sec_type,
            exchange=spec.exchange,
            currency=spec.currency,
            primaryExchange=spec.primary_exchange or "",
            lastTradeDateOrContractMonth=spec.last_trade_date_or_contract_month or "",
            localSymbol=spec.local_symbol or "",
            multiplier=spec.multiplier or "",
            tradingClass=spec.trading_class or "",
            includeExpired=spec.include_expired,
            conId=spec.con_id or 0,
        )
        details = ib.reqContractDetails(contract)
        if not details:
            raise ValueError(f"IB could not resolve contract for '{symbol}'.")
        qualified = details[0].contract
        self._contracts[symbol] = qualified
        return qualified

    def _resolve_spec(self, symbol: str, currency: str | None = None) -> IBContractSpec:
        spec = self.contract_overrides.get(symbol)
        if spec is not None:
            return spec
        return IBContractSpec(symbol=symbol, currency=currency or "USD")

    def _require_ib(self):
        if self._ib is None:
            self.connect()
        if self._ib is None:
            raise RuntimeError("IB connector could not establish a session.")
        return self._ib

    def _require_module(self):
        if self._module is None:
            self._module = self._import_ib_async()
        return self._module

    @staticmethod
    def _default_what_to_show(spec: IBContractSpec) -> str:
        return "MIDPOINT" if spec.sec_type.upper() == "CASH" else "TRADES"

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in {None, ""}:
            return None
        resolved = float(value)
        if math.isnan(resolved):
            return None
        return resolved

    def _resolve_market_price(self, ticker: Any) -> float | None:
        market_price = None
        if hasattr(ticker, "marketPrice"):
            market_price = self._safe_float(ticker.marketPrice())
        bid = self._safe_float(getattr(ticker, "bid", None))
        ask = self._safe_float(getattr(ticker, "ask", None))
        midpoint = None if bid is None or ask is None else (bid + ask) / 2.0
        return self._first_finite(
            market_price,
            self._safe_float(getattr(ticker, "last", None)),
            midpoint,
            self._safe_float(getattr(ticker, "close", None)),
        )

    def _ticker_has_price(self, ticker: Any) -> bool:
        return self._resolve_market_price(ticker) is not None

    @staticmethod
    def _resolve_ticker_timestamp(ticker: Any) -> pd.Timestamp:
        for field in ("time", "timestamp", "rtTime", "lastTimestamp", "delayedLastTimestamp"):
            raw_value = getattr(ticker, field, None)
            if raw_value in {None, 0, ""}:
                continue
            timestamp = pd.Timestamp(raw_value)
            if timestamp.tzinfo is not None:
                return timestamp.tz_convert("UTC").tz_localize(None)
            return timestamp
        return pd.Timestamp.utcnow().tz_localize(None)

    @staticmethod
    def _summary_value(
        summary: dict[tuple[str, str], float | None],
        tag: str,
        currency: str,
    ) -> float | None:
        return summary.get((tag, currency))

    @staticmethod
    def _first_finite(*values: float | None) -> float | None:
        for value in values:
            if value is None:
                continue
            if math.isnan(value):
                continue
            return float(value)
        return None

    @staticmethod
    def _import_ib_async():
        try:
            import ib_async

            return ib_async
        except ImportError as exc:
            raise ImportError(
                "ib_async is required for Interactive Brokers support. Install with: pip install ib_async"
            ) from exc
