# Focus: Win Liquidations

**One system only:** `aave-liquidator-bot`

Everything else was removed from this workspace (`ethereum-bnb-mev-bot`, `buscador_lucro`, empty scaffolding). Do not re-add arb templates, Remix “Launch” bots, tokens, or other products until this bot is winning.

---

## Goal

Make money by capturing Aave liquidation bonuses.

```
detection → simulate → race → withdraw
```

If any step fails, there is no profit. Do not skip ahead to paid infra, new chains, or new strategies until the chain above is proven end-to-end.

---

## What to achieve

### 1. Detection
- See liquidatable borrowers (HF &lt; 1, debt above dust) **before** or **as** they become contested.
- Prove with a live catch: when someone else liquidates a user, check whether that user was in our hot/near tier just before.
- **Done when:** we can show we saw the target (or explain exactly why we missed it).

### 2. Simulate
- `callStatic` / `eth_call` every candidate before sending gas.
- Enforce gas-aware `minProfit` so “wins” that lose money never ship.
- Swap path must work on the live contract (Uniswap V3 / correct router per chain).
- **Done when:** a real candidate passes simulation on a live chain.

### 3. Race
- Land the tx before other searchers.
- Tools: priority fee, fast RPC, pre-resolved path, nonce manager; paid low-latency only **after** detection is proven.
- **Done when:** we have at least one confirmed on-chain liquidation win (`ourTotal` &gt; 0).

### 4. Withdraw
- Profit sits in the liquidator contract after a win.
- Owner withdraws leftover debt-asset tokens to the operator wallet.
- **Done when:** profit is in our wallet and attributed to a known tx.

---

## What to focus on only

| Focus | Why |
|-------|-----|
| Prove detection vs race loss | Stops guessing why win-rate is 0 |
| Fix anything that blocks simulate/swap | Broken route = never win even when first |
| One profitable win on one chain | Proof the system works |
| Then improve speed / infra if we lose races | Don’t buy latency before detection is solid |

---

## Current next action (do this now)

**Run the live-catch detection verdict.**

1. Deploy/restart the bot so it writes tier snapshots (`TIER_SNAPSHOT` defaults on; data in `data/tier-snapshots/<chain>/`).
2. On the droplet (same `data/` volume as the bot):

```bash
# classifier unit check (offline)
npm run test:live-catch

# watch Base liquidations and classify each against pre-event snapshots
npm run live-catch -- base
# or: node scripts/liveCatchDetection.js base
```

3. Read the verdicts:
   - `DETECT_GAP` → fix detection (do not buy RPC)
   - `TIER_GAP` → fix HOT promotion / triggers
   - `RACE_LOSS` → detection OK; only then consider speed/infra
   - `INCONCLUSIVE` → ring not filled yet; keep bot + live-catch running

Until that verdict exists, do not buy paid infra and do not declare coverage “done.”

---

## What not to do

- Do not re-add arb templates or Remix `Launch()` bots
- Do not add Ethereum mainnet, new L2s, or new strategies before one win
- Do not buy paid RPC/colocation until the live-catch test says detection works and we are losing winnable races
- Do not build tokens, dapps, SaaS, or marketing wrappers around this yet

---

## Win pipeline (engineered)

These five levers are wired into the hot path. Tune via `.env` / `.env.example`.

| # | Lever | Default | Impact |
|---|-------|---------|--------|
| 1 | Profit-guided gas tip | `PROFIT_GUIDED_TIP=true` | Bigger opportunities bid more (even on quiet L2 base fee) |
| 2 | Pre-sign HF + dust guard | always on | Zero wasted gas on healed/dust positions |
| 3 | Chainlink price trigger | `PRICE_TRIGGER=true` | Detect drops same update (WS push or HTTP poll) |
| 4 | RPC failover + rank | `RPC_RANK=true`, stall floor 2s | Fastest URL first; no false abort on slow getBlock |
| 5 | Basket / target sort by debt | always on | Always try the most profitable route first |
| 6 | Fee cache + 1-RPC fee read | `FEE_DATA_TTL_MS=1500` | feeData ~400ms→cache 0ms; was ~900ms |

Offline checks:

```bash
npm run test:profit-tip
npm run test:win-sort
npm run test:live-catch
```

---

## Success definition

**Minimum success:** one confirmed liquidation win on a configured chain, profit withdrawn.

**Operating success:** repeated wins where detection → simulate → race → withdraw runs without manual babysitting.

Until minimum success, every task must serve that path. Everything else is noise.
