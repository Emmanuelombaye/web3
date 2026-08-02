# Path to Profit

## Vision

Run one Aave liquidator that **reliably finds unhealthy loans, executes flash-loan liquidations, and keeps the bonus** — without needing large capital up front.

We are not building a token, a dapp, an arb template, or a “press Launch and earn” contract. We are building an **operator system**: a thin on-chain executor plus an off-chain bot that watches, simulates, races, and withdraws.

**Success looks like money in the wallet** from confirmed liquidations, then repeating that loop until it runs without babysitting.

---

## The money model

Aave pays liquidators a **bonus** for cleaning unhealthy debt.

```
flash-borrow debt asset
  → liquidationCall (receive collateral + bonus)
  → swap collateral → debt asset
  → repay flash loan + fee
  → leftover = profit (in the contract)
  → withdraw to operator wallet
```

Capital needed day-to-day is mostly **gas + RPC + server**, not the loan size. The flash loan supplies the size for one transaction.

Profit only exists when:

```
bonus − swap friction − flash fee − gas > 0
```

…and when **our tx lands before competitors**.

---

## The only system

**`aave-liquidator-bot`**

Four steps. Every task must serve this chain:

```
detection → simulate → race → withdraw
```

| Step | Meaning | Done when |
|------|---------|-----------|
| **Detection** | See HF &lt; 1, non-dust debt in time | Live-catch proves we saw the target (or names the miss) |
| **Simulate** | `callStatic` + min-profit floor before spending gas | A real candidate passes simulation |
| **Race** | Land the tx first | At least one on-chain win (`ourTotal` &gt; 0) |
| **Withdraw** | Pull leftover tokens from the contract | Profit attributed to a known tx in our wallet |

If any step fails, there is no profit. Do not skip ahead.

---

## Goal ladder

1. **Prove the pipe** — live-catch verdict: detection gap vs race loss (not a guess).
2. **First dollar** — one confirmed liquidation win + withdraw.
3. **Repeatable** — wins without manual intervention on at least one chain.
4. **Scale carefully** — only then: more speed, better RPC, quieter/better chains — if evidence says we lose *winnable* races.

Until (2), everything else is noise.

---

## What we believe

- Flash loans are a **tool**, not a business. The business is being first to a real opportunity.
- Competition on Base is fierce; sparse at-risk whales are normal. That does not mean “give up” — it means **measure** before spending on infra.
- Paid low-latency RPC / colocation is a **later** lever, only after detection is proven and we are losing races we actually entered.
- One working liquidator beats three half-built MEV toys.

---

## What we refuse

- Arb marketing bots and Remix `Launch()` “profit” contracts
- New chains, products, tokens, or SaaS wrappers before one win
- Buying speed while detection is still unproven
- Declaring the system “done” with a 0 win-rate and no live-catch verdict

See `FOCUS.md` for the operational checklist and current next action.

---

## North star

> **One profitable liquidation, proven end-to-end — then make it repeat.**

That is the path to profit.
