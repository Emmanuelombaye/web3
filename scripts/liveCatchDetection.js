require("dotenv").config();

// LIVE-CATCH DETECTION VERDICT
//
// Answers the open question that gates all further work:
//   When someone else liquidates a user, were they already in our hot/near/watch
//   tiers *before* that block?
//
// Requires the bot to be writing tier snapshots (default ON via
// src/tierSnapshot.js hooked from aaveHelpers). Run this alongside the bot
// (ideally on the droplet next to data/):
//
//   node scripts/liveCatchDetection.js base
//   CHAINS=base POLL_MS=8000 node scripts/liveCatchDetection.js
//
// Verdicts:
//   WON         — we liquidated
//   RACE_LOSS   — in HOT before event → detection OK, buy speed later
//   TIER_GAP    — in NEAR/WATCH but not HOT → fix promotion/triggers
//   DETECT_GAP  — not in tiers (and often HF<1 at prior block) → fix detection
//   INCONCLUSIVE — ring empty; keep running until snapshots exist

const { ethers } = require("ethers");
const { getSelectedChainConfigs, getChainConfig } = require("../src/chains");
const { createProvider } = require("../src/provider");
const { resolveChainMinDebtUsd } = require("../aaveHelpers");
const {
  findSnapshotBefore,
  classifyLiveCatch,
  loadIndex,
  enabled: snapshotsEnabled,
} = require("../src/tierSnapshot");

const LIQUIDATION_ABI = [
  "event LiquidationCall(address indexed collateralAsset, address indexed debtAsset, address indexed user, uint256 debtToCover, uint256 liquidatedCollateralAmount, address liquidator, bool receiveAToken)",
];

const POOL_ABI = [
  "function getUserAccountData(address user) view returns (uint256 totalCollateralBase, uint256 totalDebtBase, uint256 availableBorrowsBase, uint256 currentLiquidationThreshold, uint256 ltv, uint256 healthFactor)",
];

const POLL_MS = Math.max(2000, parseInt(process.env.POLL_MS || "8000", 10) || 8000);
const LOOKBACK_BLOCKS = Math.max(50, parseInt(process.env.LOOKBACK_BLOCKS || "500", 10) || 500);

function ourWallet() {
  try { return new ethers.Wallet(process.env.PRIVATE_KEY).address.toLowerCase(); }
  catch (_) { return null; }
}

async function hfAtBlock(pool, user, blockTag, baseCurrencyUnit) {
  try {
    const d = await pool.getUserAccountData(user, { blockTag });
    const hf = parseFloat(ethers.utils.formatUnits(d.healthFactor, 18));
    const totalDebtUsd = parseFloat(d.totalDebtBase.toString()) / baseCurrencyUnit;
    return { hf, totalDebtUsd };
  } catch (_) {
    return { hf: NaN, totalDebtUsd: NaN };
  }
}

function baseCurrencyUnit(chainConfig) {
  // Aave base currency is typically 1e8 USD. Some helpers already encode this;
  // keep a safe default matching the rest of the bot.
  return Number(chainConfig.baseCurrencyUnit) || 1e8;
}

async function processEvent(cfg, provider, pool, ours, ev, seen) {
  const key = `${ev.blockNumber}-${ev.transactionHash}-${ev.logIndex}`;
  if (seen.has(key)) return null;
  seen.add(key);

  const user = ev.args.user.toLowerCase();
  const liquidator = ev.args.liquidator.toLowerCase();
  const block = await provider.getBlock(ev.blockNumber);
  const eventMs = (block?.timestamp || 0) * 1000;
  const snapshot = findSnapshotBefore(cfg.key, eventMs > 0 ? eventMs : Date.now());
  const prior = await hfAtBlock(pool, user, Math.max(ev.blockNumber - 1, 0), baseCurrencyUnit(cfg));
  const minDebtUsd = resolveChainMinDebtUsd(cfg);

  const result = classifyLiveCatch({
    ours,
    liquidator,
    user,
    snapshot,
    hfPrev: prior.hf,
    totalDebtUsd: prior.totalDebtUsd,
    minDebtUsd,
  });

  return {
    chain: cfg.key,
    block: ev.blockNumber,
    tx: ev.transactionHash,
    user,
    liquidator,
    debtToCover: ev.args.debtToCover.toString(),
    snapshotAt: snapshot?.at || null,
    hfPrev: prior.hf,
    totalDebtUsdPrev: prior.totalDebtUsd,
    ...result,
  };
}

function printCatch(r) {
  const m = r.membership || {};
  console.log(
    `\n━━ LIVE CATCH ${r.chain} @${r.block} ━━\n` +
    `  verdict: ${r.verdict}\n` +
    `  reason:  ${r.reason}\n` +
    `  user:    ${r.user}\n` +
    `  liq by:  ${r.liquidator}\n` +
    `  tx:      ${r.tx}\n` +
    `  HF@B-1:  ${Number.isFinite(r.hfPrev) ? r.hfPrev.toFixed(6) : "n/a"}  debt≈$${Number.isFinite(r.totalDebtUsdPrev) ? r.totalDebtUsdPrev.toFixed(2) : "n/a"}\n` +
    `  tiers:   hot=${!!m.hot} near=${!!m.near} watch=${!!m.watch}  snapshot=${r.snapshotAt || "none"}\n`
  );
}

async function pollChain(cfg, ours, state) {
  const provider = state.provider || createProvider(cfg);
  state.provider = provider;
  const pool = state.pool || new ethers.Contract(cfg.pool, [...LIQUIDATION_ABI, ...POOL_ABI], provider);
  state.pool = pool;

  const current = await provider.getBlockNumber();
  if (!state.fromBlock) {
    state.fromBlock = Math.max(current - LOOKBACK_BLOCKS, 0);
    console.log(`📡 ${cfg.name}: live-catch watching from block ${state.fromBlock} (head ${current})`);
    const idx = loadIndex(cfg.key);
    console.log(`   tier snapshots on disk: ${idx.entries?.length || 0} (keep ${idx.keep || "?"})`);
  }

  const toBlock = current;
  if (toBlock < state.fromBlock) return;

  const chunk = cfg.borrowScanChunkSize || 2000;
  for (let start = state.fromBlock; start <= toBlock; start += chunk) {
    const end = Math.min(start + chunk - 1, toBlock);
    let logs = [];
    try {
      logs = await pool.queryFilter(pool.filters.LiquidationCall(), start, end);
    } catch (e) {
      console.warn(`⚠️ ${cfg.name}: log query ${start}-${end} failed: ${e.message}`);
      break;
    }
    for (const ev of logs) {
      const r = await processEvent(cfg, provider, pool, ours, ev, state.seen);
      if (r) {
        state.counts[r.verdict] = (state.counts[r.verdict] || 0) + 1;
        printCatch(r);
        printSummary(cfg.key, state.counts);
      }
    }
  }
  state.fromBlock = toBlock + 1;
}

function printSummary(chainKey, counts) {
  const parts = Object.keys(counts).sort().map((k) => `${k}=${counts[k]}`);
  console.log(`📊 ${chainKey} tallies: ${parts.join("  ") || "(none yet)"}`);
  console.log(
    "Interpretation: DETECT_GAP → fix detection. TIER_GAP → fix promotion/HOT. " +
    "RACE_LOSS → detection OK, then consider speed/infra. Do not buy paid RPC on INCONCLUSIVE."
  );
}

async function main() {
  if (!snapshotsEnabled()) {
    console.warn("⚠️ TIER_SNAPSHOT is disabled — live-catch will stay INCONCLUSIVE. Unset TIER_SNAPSHOT or set true.");
  }

  const arg = process.argv[2];
  const chains = arg ? [getChainConfig(arg)] : getSelectedChainConfigs();
  const ours = ourWallet();
  console.log("LIVE-CATCH DETECTION — waiting for LiquidationCall events");
  console.log(`Our wallet: ${ours || "(no PRIVATE_KEY — WON bucket unavailable)"}`);
  console.log(`Poll every ${POLL_MS}ms | lookback ${LOOKBACK_BLOCKS} blocks\n`);

  const states = new Map();
  for (const cfg of chains) {
    states.set(cfg.key, { seen: new Set(), counts: {}, fromBlock: null });
  }

  while (true) {
    for (const cfg of chains) {
      try {
        await pollChain(cfg, ours, states.get(cfg.key));
      } catch (e) {
        console.error(`❌ ${cfg.name}: ${e.message}`);
      }
    }
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
