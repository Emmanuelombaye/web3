require("dotenv").config();

// SPEED BENCH — simulate the win-path timings (no broadcast).
// Measures: RPC RTT, feeData, tip compute, Multicall HF sweep, optional callStatic.
//
// Usage:
//   node scripts/speedBench.js              # CHAINS from .env
//   node scripts/speedBench.js base
//   SPEED_SAMPLE=200 node scripts/speedBench.js base

const { ethers } = require("ethers");
const { getSelectedChainConfigs, getChainConfig } = require("../src/chains");
const { createRankedProvider, resolveStallMs, getRpcUrls } = require("../src/provider");
const { getTransactionOverrides, computePriorityFeeWei, getFeeDataFast } = require("../src/gas");
const { getUserHealthFactorsBatched, resolveChainMinDebtUsd } = require("../aaveHelpers");
const store = require("../src/borrowerStore");

const SAMPLE = Math.max(20, parseInt(process.env.SPEED_SAMPLE || "200", 10) || 200);

function ms(t0) {
  return Date.now() - t0;
}

function pct(sorted, p) {
  if (!sorted.length) return null;
  const i = Math.min(sorted.length - 1, Math.max(0, Math.floor((p / 100) * (sorted.length - 1))));
  return sorted[i];
}

function resolveLiq(chainConfig) {
  const key = (chainConfig.key || "").toUpperCase();
  return process.env[`${key}_AAVE_LIQUIDATOR_ADDRESS`] || null;
}

async function timeRpcRtt(provider, n = 5) {
  const samples = [];
  for (let i = 0; i < n; i++) {
    const t0 = Date.now();
    await provider.getBlockNumber();
    samples.push(Date.now() - t0);
  }
  samples.sort((a, b) => a - b);
  return {
    n,
    min: samples[0],
    p50: pct(samples, 50),
    p95: pct(samples, 95),
    max: samples[samples.length - 1],
    avg: Math.round(samples.reduce((a, b) => a + b, 0) / samples.length),
  };
}

async function benchChain(cfg) {
  const out = { chain: cfg.key, name: cfg.name, ok: false };
  const provider = await createRankedProvider(cfg);
  out.rpcUrls = getRpcUrls(cfg).length;
  out.stallMs = resolveStallMs(cfg);

  // 1) RPC RTT
  const tRpc = Date.now();
  out.rpcRttMs = await timeRpcRtt(provider, 5);
  out.rpcBenchMs = ms(tRpc);

  // 2) Fee data + profit-guided tip (fast cached path)
  const tFee = Date.now();
  const feeData = await getFeeDataFast(provider, cfg);
  out.feeDataMs = ms(tFee);
  const tFee2 = Date.now();
  await getFeeDataFast(provider, cfg); // should hit cache
  out.feeDataCacheMs = ms(tFee2);
  const tipT0 = Date.now();
  const tip = computePriorityFeeWei(cfg, {
    baseFee: feeData.lastBaseFeePerGas || ethers.utils.parseUnits("0.01", "gwei"),
    bump: ethers.constants.Zero,
    estimatedProfitNative: ethers.utils.parseEther("0.05"),
    gasLimit: cfg.liquidationGasLimit || 2_000_000,
  });
  out.tipComputeMs = ms(tipT0);
  out.tipGwei = ethers.utils.formatUnits(tip, "gwei");
  const ovT0 = Date.now();
  const overrides = await getTransactionOverrides(provider, cfg, {
    feeData,
    estimatedProfitNative: ethers.utils.parseEther("0.05"),
    gasLimit: cfg.liquidationGasLimit || 2_000_000,
  });
  out.overridesMs = ms(ovT0);
  out.maxPriorityFeeGwei = overrides.maxPriorityFeePerGas
    ? ethers.utils.formatUnits(overrides.maxPriorityFeePerGas, "gwei")
    : null;

  // 3) Build a sample set: prefer hot → near → watch → borrowers head
  const hot = store.loadHot(cfg.key).hot;
  const near = store.loadNear(cfg.key).near;
  const watch = store.loadWatchlist(cfg.key).watch;
  const borrowers = store.loadBorrowerSet(cfg.key).borrowers;
  let sample = [];
  for (const set of [hot, near, watch, borrowers]) {
    for (const u of set) {
      sample.push(u);
      if (sample.length >= SAMPLE) break;
    }
    if (sample.length >= SAMPLE) break;
  }

  // If no local store, pull a small live Borrow-event sample so the bench still
  // measures Multicall HF speed against real addresses.
  if (sample.length === 0) {
    const tDisc = Date.now();
    try {
      const { getBorrowersFromBorrowEvents } = require("../aaveHelpers");
      const found = await getBorrowersFromBorrowEvents(provider, {
        ...cfg,
        borrowScanBlocks: parseInt(process.env.SPEED_BORROW_BLOCKS || "3000", 10),
        borrowBackfillBlocks: parseInt(process.env.SPEED_BORROW_BLOCKS || "3000", 10),
      });
      sample = found.slice(0, SAMPLE);
      out.discoveryMs = ms(tDisc);
      out.discoveryFound = found.length;
    } catch (e) {
      out.discoveryError = e.message;
    }
  }

  out.sampleSize = sample.length;
  out.hotSize = hot.size;
  out.nearSize = near.size;
  out.watchSize = watch.size;
  out.storeSize = borrowers.size;

  if (sample.length === 0) {
    out.ok = true;
    out.note = "no local tier/borrower data — RPC+tip only. Copy droplet data/ or run bot once.";
    return out;
  }

  // 4) Multicall HF sweep (the hot-path detect cost)
  const tHf = Date.now();
  const hfs = await getUserHealthFactorsBatched(sample, provider, cfg);
  out.hfSweepMs = ms(tHf);
  out.hfPerUserMs = +(out.hfSweepMs / sample.length).toFixed(2);
  const minDebt = resolveChainMinDebtUsd(cfg);
  const threshold = parseFloat(process.env.LIQUIDATION_THRESHOLD || "1.0");
  const liquidatable = hfs.filter(
    (h) => Number.isFinite(h.healthFactor) && h.healthFactor < threshold && h.totalDebtUsd >= minDebt
  );
  out.liquidatable = liquidatable.length;
  out.lowestHf = hfs
    .filter((h) => Number.isFinite(h.healthFactor) && h.healthFactor < 1e10)
    .sort((a, b) => a.healthFactor - b.healthFactor)
    .slice(0, 3)
    .map((h) => ({ user: h.user.slice(0, 10), hf: +h.healthFactor.toFixed(4), debt: +h.totalDebtUsd.toFixed(2) }));

  // 5) Liquidator contract code check (if configured)
  const liquidatorAddress = resolveLiq(cfg);
  out.liquidator = liquidatorAddress ? liquidatorAddress.slice(0, 10) + "…" : null;

  if (liquidatable.length > 0 && liquidatorAddress) {
    const tCode = Date.now();
    const code = await provider.getCode(liquidatorAddress);
    out.liquidatorCodeMs = ms(tCode);
    out.liquidatorHasCode = code && code !== "0x";
  }

  // 6) Synthetic "detect → decide prep" budget
  out.detectDecidePrepMs = (out.hfSweepMs || 0) + (out.feeDataMs || 0) + (out.overridesMs || 0);
  out.ok = true;
  return out;
}

async function main() {
  const arg = process.argv[2];
  const chains = arg ? [getChainConfig(arg)] : getSelectedChainConfigs();
  console.log(`⚡ SPEED BENCH — sample=${SAMPLE} chains=${chains.map((c) => c.key).join(",")}\n`);

  const results = [];
  for (const cfg of chains) {
    const t0 = Date.now();
    try {
      const r = await benchChain(cfg);
      r.wallMs = ms(t0);
      results.push(r);
      printResult(r);
    } catch (e) {
      console.log(`━━ ${cfg.name} ━━ ERROR: ${e.message}\n`);
      results.push({ chain: cfg.key, ok: false, error: e.message, wallMs: ms(t0) });
    }
  }

  console.log("\n—— SUMMARY ——");
  for (const r of results) {
    if (!r.ok) {
      console.log(`${r.chain}: FAIL ${r.error || ""}`);
      continue;
    }
    console.log(
      `${r.chain}: rpc p50=${r.rpcRttMs?.p50}ms | fee=${r.feeDataMs}ms | hfSweep=${r.hfSweepMs ?? "n/a"}ms` +
        ` (n=${r.sampleSize}) | detect+prep≈${r.detectDecidePrepMs ?? "n/a"}ms | tip=${r.tipGwei} gwei | wall=${r.wallMs}ms`
    );
  }
}

function printResult(r) {
  console.log(`━━ ${r.name} (${r.chain}) ━━`);
  console.log(`  RPCs: ${r.rpcUrls}  stall=${r.stallMs}ms`);
  console.log(`  RPC RTT: min=${r.rpcRttMs.min} p50=${r.rpcRttMs.p50} p95=${r.rpcRttMs.p95} max=${r.rpcRttMs.max} avg=${r.rpcRttMs.avg} ms`);
  console.log(`  feeData: ${r.feeDataMs}ms (cache hit ${r.feeDataCacheMs}ms) | tipCompute: ${r.tipComputeMs}ms | overrides: ${r.overridesMs}ms | tip=${r.tipGwei} gwei`);
  console.log(`  store: hot=${r.hotSize} near=${r.nearSize} watch=${r.watchSize} borrowers=${r.storeSize} sample=${r.sampleSize}`);
  if (r.note) console.log(`  note: ${r.note}`);
  if (r.hfSweepMs != null) {
    console.log(`  HF multicall: ${r.hfSweepMs}ms total (${r.hfPerUserMs} ms/user) | liquidatable now: ${r.liquidatable}`);
    if (r.lowestHf?.length) console.log(`  lowest HF: ${JSON.stringify(r.lowestHf)}`);
  }
  if (r.liquidator) console.log(`  liquidator: ${r.liquidator} hasCode=${r.liquidatorHasCode} (${r.liquidatorCodeMs}ms)`);
  console.log(`  wall: ${r.wallMs}ms\n`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
