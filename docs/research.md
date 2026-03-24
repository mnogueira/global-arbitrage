# Global Arbitrage Research Notes

Research date: `2026-03-24`

This document is the first-pass market map for the project. It separates:

- ideas worth coding now,
- ideas worth monitoring later,
- ideas that sound good but are operationally weak for a retail/demo setup.

## Verified Market Backdrop

### Oil and the Iran war

- The IEA's `Oil Market Report - March 2026`, published on `2026-03-12`, says the war in the Middle East created the largest supply disruption in oil market history and that Brent had traded "within a whisker of $120/bbl" before easing to around `$92/bbl` at the time of writing.
- AP reported on `2026-03-20` that Brent briefly rose above `$119`, later settled at `$108.65`, and U.S. crude settled at `$96.14`.
- AP reported on `2026-03-24` that Brent then fell to `$99.94` after diplomacy headlines.

Practical conclusion:

- The live edge is not "Brent is permanently at a historically wide premium to WTI."
- The live edge is `violent cross-session repricing`, `energy-linked FX shocks`, and `Brazil beta transmission` into Petrobras, EWZ, BOVA11, and USD/BRL.

### Rates and carry

- The Federal Reserve's official `2026-03-18` statement kept the federal funds target range at `3.50% to 3.75%`.
- The latest official Copom decision indexed from Banco Central do Brasil is `2026-01-28`, with the Selic target at `15.00% a.a.` and an asymmetric balance of risks.
- The ECB's official rates page lists the deposit facility at `2.00%` effective `2026-03-11`.
- The Bank of England kept Bank Rate at `3.75%` on `2026-02-05`.

Practical conclusion:

- Brazil still offers a large nominal carry differential versus the U.S. and Europe.
- That matters for `USD/BRL`, `WDO/DOL carry`, and the financing assumptions behind ADR or cross-currency trades.

### B3 contract and schedule facts

- B3's official WDO page states that the `WDO` contract size is `USD 10,000`.
- B3's official derivatives hours page shows `DOL` and `WDO` normal trading from `09:00` to `18:30`.
- B3's DI futures circular states the trading code `DI1` and that each point corresponds to `R$1.00`.

Practical conclusion:

- `WDO` and `DI1` are valid future extensions for the scanner, but the first implementation should not fake maturity-roll and carry mechanics.

### ETF and equity anchors

- BlackRock's `EWZ` page lists `NAV $36.74` and closing price `$36.85` as of `2026-03-23`.
- BlackRock's `BOVA11` page lists `NAV R$172.91` as of `2026-03-20`.
- Petrobras' official Form 20-F states that each Petrobras ADS represents `two` common or `two` preferred shares.
- Itaú IR states `ADR Ratio: 1:1` and that `ITUB` ADRs are backed by `ITUB4`.
- Bradesco IR FAQ states the ADR ratio is `1:1`.
- Vale's official SEC filing/search results state each Vale ADR represents `one` underlying share.

Practical conclusion:

- ADR parity for large Brazilian names is the cleanest mechanical starting point.
- `EWZ` and `BOVA11` are close enough in economic exposure to support a rolling fair-value model, but they are not a perfect creation-redemption arbitrage pair.

## Opportunity Ranking

## 1. ADR Parity

Why it matters:

- It is the closest thing here to classic cross-market arbitrage.
- It fits Brazil/U.S. overlap and uses highly liquid large caps.
- It naturally incorporates FX, settlement friction, and corporate-action reality.

Initial universe:

- `PETR4` vs `PBR-A`, ratio `2`
- `VALE3` vs `VALE`, ratio `1`
- `ITUB4` vs `ITUB`, ratio `1`
- `BBDC4` vs `BBD`, ratio `1`

Edge sources:

- U.S. ETF and macro flows hit ADRs faster.
- Brazil cash equities may lag during early B3 trading or local-event windows.
- FX overshoots can distort parity temporarily.

What makes it real:

- We convert ADR prices into BRL per local share.
- We subtract fees, FX drag, slippage, borrow/settlement buffer, and capital usage.

Main risks:

- Borrow and short availability.
- Settlement and ADR conversion latency.
- Different tax, dividend, and corporate-action timing.

Decision:

- `Implement now`

## 2. EWZ / BOVA11 / Brazil Beta Transfer

Why it matters:

- EWZ trades in the U.S. while Brazilian cash is closed.
- São Paulo traders can watch Asian close, Europe midday, and the U.S. session without waiting for local headlines to be translated.

Edge sources:

- Overnight U.S. repricing of Brazil risk.
- Oil and commodity shocks passing into Brazil beta.
- USD/BRL changes that alter the BRL fair value of EWZ.

What makes it real:

- We do not call it textbook arbitrage.
- We use a rolling fair-value ratio between `BOVA11` and translated `EWZ`.
- We only signal when the deviation clears a conservative cost buffer.

Main risks:

- EWZ tracks the `MSCI Brazil 25/50 Index`, while BOVA11 tracks the `Ibovespa`.
- Fair value can drift during local composition or sector shocks.
- This is relative value, not risk-free convergence.

Decision:

- `Implement now`

## 3. Crypto Implied-FX Arbitrage

Why it matters:

- It runs `24/7`.
- Public APIs are simple.
- Brazil and Spanish-speaking LatAm venues can fragment sharply under stress.

Core expression:

- Compare `BTC/BRL` with `BTC/USD * USD/BRL`.

Optional LatAm overlays:

- `BTC/MXN`
- `BTC/ARS`

Edge sources:

- Fiat ramps and local banking friction.
- Weekend or off-hours FX dislocations.
- Risk-off flows and capital controls.

What makes it real:

- We treat stablecoin or FX basis as a cost, not a free bridge.
- We size conservatively because venue and withdrawal risk dominate.

Main risks:

- Exchange counterparty and transfer risk.
- Stablecoin basis versus true USD.
- Local banking and withdrawal delays.

Decision:

- `Implement now`

## 4. WDO / DOL / LatAm FX Triangles

Interesting because:

- B3 lists futures on `USD`, `EUR`, `GBP`, `MXN`, `CLP`, `ARS`, and others.
- Brazil's carry and local participant mix can create temporary pricing frictions.

Why not first:

- Contract expiry, rolls, and settlement calendars matter a lot.
- Demo-broker symbol coverage is uncertain.

Decision:

- `Monitor next`

## 5. DI1 / Rate Arbitrage

Interesting because:

- Brazil's short-rate complex is still structurally far above developed-market policy rates.
- DI futures encode both macro path and local risk premium.

Why not first:

- This is more curve relative value than simple spot-vs-spot arbitrage.
- It needs contract-aware historical data and better carry accounting.

Decision:

- `Monitor next`

## 6. Oil Cross-Market Dislocations

Interesting because:

- Petrobras, EWZ, USD/BRL, and commodity-sensitive Brazil beta are all reacting to the same shock.
- Current conditions are unusually volatile.

Why not first:

- Brent vs WTI alone is not enough.
- The best version is probably a broader `oil shock transmission` model, not a naked spread trade.

Decision:

- `Use as context and regime filter now`

## Geographic and Language Arbitrage

This is the human edge around the scanner, not just inside it:

- Portuguese sources surface Brazil-specific corporate and macro signals before they are fully absorbed abroad.
- Spanish sources improve coverage of Argentina, Chile, Mexico, and broader LatAm flows.
- The timezone stack lets São Paulo process:
  - Asia close
  - Europe midday
  - U.S. open and close

This repo does not fully automate multilingual news arbitrage yet, but the architecture should make that easy to add.

## What To Ignore For Now

- Pure options "mispricing" without volatility surface data.
- "Risk-free" retail triangular FX arbitrage on OTC quotes.
- Cross-border e-commerce arbitrage as a core repo thesis.
- Any trade that only works before fees or without borrow.

## Sources

- IEA Oil Market Report, published `2026-03-12`:
  `https://www.iea.org/reports/oil-market-report-march-2026`
- Federal Reserve FOMC statement, `2026-03-18`:
  `https://www.federalreserve.gov/newsevents/pressreleases/monetary20260318a.htm`
- Banco Central do Brasil, Copom `276th` decision, `2026-01-28`:
  `https://www.bcb.gov.br/controleinflacao/historicotaxasjuros`
- ECB rates page:
  `https://www.ecb.europa.eu/stats/policy_and_exchange_rates/key_ecb_interest_rates/html/index.en.html`
- Bank of England MPC summary, `2026-02-05`:
  `https://www.bankofengland.co.uk/monetary-policy-summary-and-minutes/2026/february-2026`
- B3 WDO contract page:
  `https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/moedas/futuro-mini-de-taxa-de-cambio-de-reais-por-dolar-comercial.htm`
- B3 derivatives trading hours:
  `https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/horario-de-negociacao/derivativos/cambio-e-dolar-pronto/`
- B3 DI futures circular:
  `https://www.b3.com.br/data/files/BB/B1/68/03/49818910A3717F79AC094EA8/OC%20034-2025-VPC%20ALTERACAO%20DO%20CONTRATO%20FUTURO%20DE%20DI%20E%20REPUBLICACAO%20DOS%20CONTRATOS%20DE%20JUROS_PT.pdf`
- iShares MSCI Brazil ETF (`EWZ`):
  `https://www.ishares.com/us/products/239612/ishares-msci-brazil-capped-etf`
- iShares Ibovespa Fundo de Indice (`BOVA11`):
  `https://www.blackrock.com/br/products/251816/ishares-ibovespa-fundo-de-ndice-fund`
- Petrobras Form 20-F / ADR ratio:
  `https://petrobras.com.br/documents/d/f3a44542-113e-11ee-be56-0242ac120002/form-20f-2023-en`
- Itau ADR ratio:
  `https://www.itau.com.br/relacoes-com-investidores/en/market-information/dividends-and-interest-on-capital/`
- Bradesco ADR ratio:
  `https://www.bradescori.com.br/en/services/faq/`
- Vale ADR profile/contact and filings:
  `https://vale.com/contacts`
  `https://vale.com/documents/44618/434742/Form%2B20-F%2B-%2B2021.pdf/aad5f2c3-ca1e-eb9d-73da-5f4dd2a514c6`
- AP oil market coverage, `2026-03-20`, `2026-03-24`:
  `https://apnews.com/article/stocks-markets-oil-iran-trump-1abeddf7c4bf19d1dc96b3f23c1de402`
  `https://apnews.com/article/stocks-markets-oil-iran-trump-026e3ab83a6256e36001b85058f92b5d`
