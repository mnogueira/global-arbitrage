"""MetaTrader 5 execution adapter with live account and position snapshots."""

from __future__ import annotations

from typing import Any

import pandas as pd

from global_arbitrage.execution.broker import (
    BrokerAccountSnapshot,
    BrokerPosition,
    OrderReceipt,
    OrderSide,
)


class MT5ExecutionBroker:
    """MetaTrader 5 market-order adapter."""

    venue = "mt5"

    def __init__(
        self,
        *,
        login: int | None = None,
        password: str | None = None,
        server: str | None = None,
        mt5_path: str | None = None,
        magic_number: int = 431000,
        deviation: int = 20,
    ):
        self.login = login
        self.password = password
        self.server = server
        self.mt5_path = mt5_path
        self.magic_number = magic_number
        self.deviation = deviation
        self._mt5: Any | None = None

    def connect(self) -> None:
        mt5 = self._import_mt5()
        kwargs: dict[str, object] = {}
        if self.mt5_path:
            kwargs["path"] = self.mt5_path
        if self.login is not None:
            kwargs["login"] = self.login
        if self.password:
            kwargs["password"] = self.password
        if self.server:
            kwargs["server"] = self.server
        if not mt5.initialize(**kwargs):
            raise ConnectionError(f"MT5 initialization failed: {mt5.last_error()}")
        self._mt5 = mt5

    def disconnect(self) -> None:
        if self._mt5 is not None:
            self._mt5.shutdown()
        self._mt5 = None

    def submit_market_order(self, *, symbol: str, side: OrderSide, quantity: float) -> OrderReceipt:
        mt5 = self._require_mt5()
        symbol = self.validate_symbol(symbol)
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            raise ValueError(f"MT5 symbol '{symbol}' could not be loaded.")
        if not symbol_info.visible:
            mt5.symbol_select(symbol, True)
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"MT5 tick unavailable for '{symbol}'.")
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(quantity),
            "type": mt5.ORDER_TYPE_BUY if side is OrderSide.BUY else mt5.ORDER_TYPE_SELL,
            "price": tick.ask if side is OrderSide.BUY else tick.bid,
            "deviation": self.deviation,
            "magic": self.magic_number,
            "comment": "global_arbitrage_paper",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_RETURN,
        }
        result = mt5.order_send(request)
        if result is None:
            raise RuntimeError(f"MT5 order_send returned None: {mt5.last_error()}")
        status = "FILLED" if result.retcode == mt5.TRADE_RETCODE_DONE else "REJECTED"
        return OrderReceipt(
            symbol=symbol,
            side=side.value,
            quantity=float(quantity),
            status=status,
            venue=self.venue,
            order_id=None if getattr(result, "order", 0) == 0 else str(result.order),
            filled_price=None if getattr(result, "price", 0.0) in {None, 0.0} else float(result.price),
            message=str(getattr(result, "comment", "")),
            metadata={"retcode": int(getattr(result, "retcode", 0))},
        )

    def positions(self) -> list[BrokerPosition]:
        mt5 = self._require_mt5()
        payload = mt5.positions_get()
        if payload is None:
            return []
        positions: list[BrokerPosition] = []
        for row in payload:
            positions.append(
                BrokerPosition(
                    venue=self.venue,
                    symbol=str(row.symbol),
                    quantity=float(row.volume),
                    currency=str(getattr(row, "currency", "") or getattr(row, "currency_profit", "BRL")),
                    average_price=float(getattr(row, "price_open", 0.0) or 0.0),
                    market_price=float(getattr(row, "price_current", 0.0) or 0.0),
                    market_value=None,
                    unrealized_pnl=float(getattr(row, "profit", 0.0) or 0.0),
                    realized_pnl=None,
                    metadata={
                        "ticket": int(getattr(row, "ticket", 0)),
                        "type": int(getattr(row, "type", 0)),
                    },
                )
            )
        return positions

    def account_snapshot(self) -> BrokerAccountSnapshot:
        mt5 = self._require_mt5()
        info = mt5.account_info()
        if info is None:
            raise RuntimeError("MT5 account_info() returned None.")
        return BrokerAccountSnapshot(
            venue=self.venue,
            account_id=None if getattr(info, "login", None) is None else str(info.login),
            currency=str(getattr(info, "currency", "BRL")),
            timestamp=pd.Timestamp.utcnow().tz_localize(None),
            balance=float(getattr(info, "balance", 0.0) or 0.0),
            equity=float(getattr(info, "equity", 0.0) or 0.0),
            available_funds=float(getattr(info, "margin_free", 0.0) or 0.0),
            buying_power=None,
            unrealized_pnl=float(getattr(info, "profit", 0.0) or 0.0),
            realized_pnl=None,
            metadata={
                "margin": float(getattr(info, "margin", 0.0) or 0.0),
                "margin_level": float(getattr(info, "margin_level", 0.0) or 0.0),
            },
        )

    def validate_symbol(self, symbol: str) -> str:
        """Ensure an MT5 symbol exists exactly, with helpful suffix guidance."""

        mt5 = self._require_mt5()
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is not None:
            return symbol
        candidates = mt5.symbols_get()
        if candidates is None:
            raise ValueError(f"MT5 symbol '{symbol}' not found and symbols_get() returned nothing.")
        prefix_matches = [
            str(candidate.name)
            for candidate in candidates
            if str(candidate.name).upper().startswith(symbol.upper())
        ]
        guidance = "" if not prefix_matches else f" Close matches: {', '.join(prefix_matches[:10])}"
        raise ValueError(
            f"MT5 symbol '{symbol}' not found. Configure the exact broker-specific symbol suffix.{guidance}"
        )

    def _require_mt5(self):
        if self._mt5 is None:
            raise RuntimeError("MT5 broker not connected.")
        return self._mt5

    @staticmethod
    def _import_mt5():
        try:
            import MetaTrader5 as mt5  # noqa: N813

            return mt5
        except ImportError as exc:
            raise ImportError(
                "MetaTrader5 package is required for MT5 integration. Install with: uv sync --extra mt5"
            ) from exc
