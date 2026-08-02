const fs = require("fs");
const path = require("path");
const { DATA_DIR } = require("./borrowerStore");

// Ring-buffer of pre-liquidation tier membership. Needed to answer the open
// detection question: when someone else liquidates a user, were they already
// in our hot/near/watch tiers *before* that block?
//
// Enable/disable: TIER_SNAPSHOT=false turns writes off.
// Retention: TIER_SNAPSHOT_KEEP (default 120 ≈ 30 min at 15s cycles).

const DEFAULT_KEEP = 120;

function enabled() {
  const v = (process.env.TIER_SNAPSHOT || "true").toLowerCase();
  return v !== "0" && v !== "false" && v !== "off" && v !== "no";
}

function keepCount() {
  const n = parseInt(process.env.TIER_SNAPSHOT_KEEP || String(DEFAULT_KEEP), 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_KEEP;
}

function dirFor(chainKey) {
  return path.join(DATA_DIR, "tier-snapshots", chainKey);
}

function indexPath(chainKey) {
  return path.join(dirFor(chainKey), "index.json");
}

function ensureDir(chainKey) {
  const dir = dirFor(chainKey);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function setToSorted(setLike) {
  if (!setLike) return [];
  const arr = setLike instanceof Set ? Array.from(setLike) : Array.from(setLike);
  return arr.map((a) => String(a).toLowerCase()).sort();
}

function loadIndex(chainKey) {
  try {
    return JSON.parse(fs.readFileSync(indexPath(chainKey), "utf-8"));
  } catch (_) {
    return { keep: keepCount(), entries: [] };
  }
}

function saveIndex(chainKey, index) {
  ensureDir(chainKey);
  const finalPath = indexPath(chainKey);
  const tmpPath = `${finalPath}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(index));
  fs.renameSync(tmpPath, finalPath);
}

/**
 * Persist a compact snapshot of tier membership right after a sweep persist.
 * Safe to call every cycle; no-ops when TIER_SNAPSHOT=false.
 */
function recordTierSnapshot(chainKey, { hot, near, watch, active, sweepType, blockNumber } = {}) {
  if (!enabled() || !chainKey) return null;

  const at = new Date().toISOString();
  const id = `${at.replace(/[:.]/g, "-")}_${process.hrtime.bigint().toString(36)}`;
  const dir = ensureDir(chainKey);
  const file = path.join(dir, `${id}.json`);

  const payload = {
    chainKey,
    at,
    sweepType: sweepType || null,
    blockNumber: Number.isFinite(blockNumber) ? blockNumber : null,
    hot: setToSorted(hot),
    near: setToSorted(near),
    watch: setToSorted(watch),
    // active can be large; store only when explicitly provided and smallish,
    // otherwise skip to keep disk/CPU cheap. Live-catch mainly needs hot/near/watch.
    activeCount: active ? (active.size != null ? active.size : active.length) : null,
  };

  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(payload));
  fs.renameSync(tmp, file);

  const index = loadIndex(chainKey);
  index.keep = keepCount();
  index.entries.push({ id, at, file: path.basename(file), hot: payload.hot.length, near: payload.near.length, watch: payload.watch.length });

  while (index.entries.length > index.keep) {
    const old = index.entries.shift();
    try { fs.unlinkSync(path.join(dir, old.file)); } catch (_) {}
  }
  saveIndex(chainKey, index);
  return payload;
}

function readSnapshotFile(chainKey, basename) {
  const file = path.join(dirFor(chainKey), basename);
  return JSON.parse(fs.readFileSync(file, "utf-8"));
}

/**
 * Find the newest snapshot with `at` strictly before `beforeIso` (or before
 * `beforeMs`). Returns null if none.
 */
function findSnapshotBefore(chainKey, beforeIsoOrMs) {
  const beforeMs = typeof beforeIsoOrMs === "number"
    ? beforeIsoOrMs
    : Date.parse(beforeIsoOrMs);
  if (!Number.isFinite(beforeMs)) return null;

  const index = loadIndex(chainKey);
  for (let i = index.entries.length - 1; i >= 0; i--) {
    const entry = index.entries[i];
    const t = Date.parse(entry.at);
    if (Number.isFinite(t) && t < beforeMs) {
      try {
        return readSnapshotFile(chainKey, entry.file);
      } catch (_) {
        continue;
      }
    }
  }
  return null;
}

function membership(snapshot, user) {
  const u = String(user).toLowerCase();
  if (!snapshot) {
    return { hot: false, near: false, watch: false, any: false };
  }
  const hot = (snapshot.hot || []).includes(u);
  const near = (snapshot.near || []).includes(u);
  const watch = (snapshot.watch || []).includes(u);
  return { hot, near, watch, any: hot || near || watch };
}

/**
 * Classify a competitor liquidation against a pre-event snapshot + optional
 * pre-block health factor.
 *
 *   WON          — we were the liquidator
 *   RACE_LOSS    — in HOT before the event; we saw them, lost speed
 *   TIER_GAP     — in NEAR/WATCH but not HOT — promotion/trigger gap
 *   DETECT_GAP   — not in hot/near/watch before the event
 *   INCONCLUSIVE — no snapshot available yet
 */
function classifyLiveCatch({
  ours,
  liquidator,
  user,
  snapshot,
  hfPrev,
  totalDebtUsd,
  minDebtUsd = 100,
}) {
  if (ours && liquidator && String(liquidator).toLowerCase() === String(ours).toLowerCase()) {
    return { verdict: "WON", reason: "our wallet is liquidator", membership: membership(snapshot, user) };
  }
  if (!snapshot) {
    return {
      verdict: "INCONCLUSIVE",
      reason: "no tier snapshot before event — leave live-catch running to fill the ring",
      membership: membership(null, user),
    };
  }

  const m = membership(snapshot, user);
  const wasLiquidatable =
    Number.isFinite(hfPrev) &&
    hfPrev < 1.0 &&
    Number.isFinite(totalDebtUsd) &&
    totalDebtUsd >= minDebtUsd;

  if (m.hot) {
    return {
      verdict: "RACE_LOSS",
      reason: wasLiquidatable
        ? "in HOT before event + HF<1 at prior block — detection OK, lost the race"
        : "in HOT before event — detection OK, lost the race (HF at prior block inconclusive)",
      membership: m,
      wasLiquidatable,
    };
  }
  if (m.near || m.watch) {
    return {
      verdict: "TIER_GAP",
      reason: `in ${m.near ? "NEAR" : ""}${m.near && m.watch ? "+" : ""}${m.watch ? "WATCH" : ""} but not HOT before event — promotion/trigger gap`,
      membership: m,
      wasLiquidatable,
    };
  }
  if (wasLiquidatable) {
    return {
      verdict: "DETECT_GAP",
      reason: "HF<1 + debt≥floor at prior block but NOT in hot/near/watch — detection still broken",
      membership: m,
      wasLiquidatable,
    };
  }
  return {
    verdict: "DETECT_GAP",
    reason: "not in hot/near/watch before event (prior-block HF not liquidatable or unavailable)",
    membership: m,
    wasLiquidatable,
  };
}

module.exports = {
  enabled,
  recordTierSnapshot,
  findSnapshotBefore,
  membership,
  classifyLiveCatch,
  dirFor,
  loadIndex,
};
