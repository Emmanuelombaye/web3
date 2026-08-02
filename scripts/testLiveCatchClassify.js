// Offline unit check for classifyLiveCatch (no RPC).
const { classifyLiveCatch } = require("../src/tierSnapshot");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const user = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ours = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const snap = {
  at: "2026-06-05T20:00:00.000Z",
  hot: [user],
  near: [user],
  watch: [],
};

{
  const r = classifyLiveCatch({
    ours, liquidator: ours, user, snapshot: snap, hfPrev: 0.9, totalDebtUsd: 500, minDebtUsd: 100,
  });
  assert(r.verdict === "WON", `expected WON got ${r.verdict}`);
}

{
  const r = classifyLiveCatch({
    ours, liquidator: "0xcccccccccccccccccccccccccccccccccccccccc", user, snapshot: snap, hfPrev: 0.95, totalDebtUsd: 1000, minDebtUsd: 100,
  });
  assert(r.verdict === "RACE_LOSS", `expected RACE_LOSS got ${r.verdict}`);
}

{
  const r = classifyLiveCatch({
    ours,
    liquidator: "0xcccccccccccccccccccccccccccccccccccccccc",
    user,
    snapshot: { at: "x", hot: [], near: [user], watch: [user] },
    hfPrev: 0.99,
    totalDebtUsd: 200,
    minDebtUsd: 100,
  });
  assert(r.verdict === "TIER_GAP", `expected TIER_GAP got ${r.verdict}`);
}

{
  const r = classifyLiveCatch({
    ours,
    liquidator: "0xcccccccccccccccccccccccccccccccccccccccc",
    user,
    snapshot: { at: "x", hot: [], near: [], watch: [] },
    hfPrev: 0.8,
    totalDebtUsd: 5000,
    minDebtUsd: 100,
  });
  assert(r.verdict === "DETECT_GAP", `expected DETECT_GAP got ${r.verdict}`);
}

{
  const r = classifyLiveCatch({
    ours,
    liquidator: "0xcccccccccccccccccccccccccccccccccccccccc",
    user,
    snapshot: null,
    hfPrev: 0.8,
    totalDebtUsd: 5000,
    minDebtUsd: 100,
  });
  assert(r.verdict === "INCONCLUSIVE", `expected INCONCLUSIVE got ${r.verdict}`);
}

console.log("✅ live-catch classifier OK");
