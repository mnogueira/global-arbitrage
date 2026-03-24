# Global Arbitrage

`global-arbitrage` is a research and execution lab for cross-market dislocations with a Brazil-first lens.
The project is built around a simple thesis:

- Brazil sits at the overlap of Asia close, Europe midday, and U.S. open.
- Portuguese, English, and Spanish widen the information surface across LatAm, the U.S., and Europe.
- Many apparent "arbitrage" ideas are fake once fees, FX, settlement, and borrow are included.
- A useful system should rank opportunities by realistic net edge, not raw spread.

## Current Focus

The first release concentrates on three strategies that are both actionable and testable with public data plus an MT5 demo account:

1. `ADR parity`
   Compare Brazilian cash equities with their U.S. ADRs after FX and ADR ratios.
   Initial pairs: `PETR4/PBR-A`, `VALE3/VALE`, `ITUB4/ITUB`, `BBDC4/BBD`.

2. `EWZ / BOVA11 / Brazil beta dislocation`
   Translate `EWZ` into BRL, estimate a rolling fair-value anchor for `BOVA11`, and trade large deviations.
   This is not pure mechanical arbitrage, but it is a practical session-overlap relative-value setup.

3. `Crypto implied-FX arbitrage`
   Compare `BTC/BRL` with `BTC/USD * USD/BRL`, then layer optional LatAm venue checks such as `BTC/MXN` and `BTC/ARS`.

## Why These First

- They fit the user's Brazil market knowledge and timezone.
- They can be monitored with public APIs today.
- They can be paper-traded without pretending we already have prime brokerage, stock borrow, or multi-exchange settlement rails.
- They generalize into a reusable scanner framework.

## What This Project Is Not

- It is not a promise of risk-free profit.
- It is not a high-frequency co-location engine.
- It is not a replacement for real borrow, custody, settlement, or tax analysis.

This repo intentionally distinguishes:

- `mechanical parity`: close to textbook arbitrage, but constrained by conversion and settlement.
- `cross-market relative value`: fair-value and session-transfer trades that behave like arbitrage opportunities when dislocations are large.
- `information arbitrage`: multilingual and timezone advantages that create lead-lag edges.

## Repository Layout

```text
configs/                 Example strategy and risk configuration
docs/                    Research notes, market map, and implementation thesis
scripts/                 Thin wrappers around the package CLIs
src/global_arbitrage/    Scanner, connectors, strategies, execution, and backtests
tests/                   Unit tests for spread and signal logic
```

## Quick Start

```bash
uv sync
uv sync --extra dev
uv sync --extra mt5   # optional, only if you want MT5 integration
```

Run one scanner pass:

```bash
python scripts/scan.py --config configs/default.yaml
```

Backtest a strategy:

```bash
python scripts/backtest.py --config configs/default.yaml --strategy adr_petr4_pbra
```

Advance the paper book by one step:

```bash
python scripts/paper.py --config configs/default.yaml --strategy adr_petr4_pbra
```

## Research Backbone

The initial opportunity ranking is documented in [docs/research.md](/C:/Dev/global-arbitrage/docs/research.md).
The short version:

- `ADR parity` is the best mechanical scanner to build first.
- `EWZ/BOVA11` is the best timezone-transfer trade to operationalize quickly.
- `BTC/BRL vs BTC/USD*FX` is the best 24/7 public-api arbitrage monitor.
- `WDO/DOL/DI1/oil crossovers` are important, but more sensitive to contract specifics and broker access.

## MT5 Demo Execution

MT5 in this repo is treated as an execution sandbox, not proof that multi-leg live arbitrage is solved.
The paper engine keeps the synthetic pair P&L locally and can optionally mirror a selected leg to MT5 when:

- the broker exposes the symbol,
- the demo account is connected,
- the user supplies symbol mappings and order sizing.

## Realism Checklist

Every strategy estimates or documents:

- exchange fees
- FX conversion drag
- slippage buffer
- ADR conversion or settlement latency
- borrow or financing assumptions
- capital required per trade

If a spread looks good only before costs, it should be filtered out.

## Roadmap

- Add direct B3 and broker-specific connectors where accessible.
- Add multilingual news feeds for Portuguese and Spanish lead-lag monitoring.
- Add contract-aware futures scanners for `WDO`, `DOL`, `WIN`, and `DI1`.
- Add richer alert routes such as Telegram, e-mail, and Streamlit dashboards.
