const { ethers } = require("ethers");

// In-memory RTT ranking so createProvider can put the fastest URL first after warm.
const rankedUrlCache = new Map(); // chainKey -> { urls, at }

function createProvider(chainConfig) {
  const stallTimeout = resolveStallMs(chainConfig);
  let urls = getRpcUrls(chainConfig);

  // Prefer previously probed order (from createRankedProvider / warmRpcRankings).
  const cached = rankedUrlCache.get(chainConfig.key);
  if (cached && cached.urls?.length) {
    const set = new Set(urls);
    const preferred = cached.urls.filter((u) => set.has(u));
    const rest = urls.filter((u) => !preferred.includes(u));
    urls = [...preferred, ...rest];
  }

  if (urls.length === 1) {
    return new ethers.providers.StaticJsonRpcProvider(urls[0], chainConfig.chainId);
  }

  // Fast primary + stall failover (replaces ethers FallbackProvider, which was
  // adding multi-hundred-ms latency under load). Primary must answer within
  // stallTimeout; then remaining URLs are raced.
  return new FastFailoverProvider(urls, chainConfig.chainId, stallTimeout);
}

/**
 * Probe all RPC URLs in parallel, rank by RTT, cache order, return provider.
 * Call this from the live bot / speed bench so the hot path uses the fastest endpoint.
 */
async function createRankedProvider(chainConfig) {
  const urls = getRpcUrls(chainConfig);
  if (urls.length > 1 && (process.env.RPC_RANK || "true") !== "false") {
    const ranked = await rankRpcUrls(urls, chainConfig.chainId);
    rankedUrlCache.set(chainConfig.key, { urls: ranked.urls, at: Date.now(), times: ranked.times });
    if (ranked.times.length) {
      const summary = ranked.times
        .map((t) => `${shortUrl(t.url)}=${t.ms == null ? "fail" : t.ms + "ms"}`)
        .join(" ");
      console.log(`⚡ ${chainConfig.name || chainConfig.key}: RPC rank ${summary}`);
    }
    return createProvider({ ...chainConfig, rpcUrls: ranked.urls, rpcUrl: ranked.urls[0] });
  }
  return createProvider(chainConfig);
}

async function warmRpcRankings(chainConfigs) {
  await Promise.all(chainConfigs.map((c) => createRankedProvider(c).catch(() => null)));
}

async function rankRpcUrls(urls, chainId, timeoutMs = 1500) {
  const times = await Promise.all(
    urls.map(async (url) => {
      const t0 = Date.now();
      const p = new ethers.providers.StaticJsonRpcProvider(url, chainId);
      try {
        await promiseWithTimeout(p.getBlockNumber(), timeoutMs);
        return { url, ms: Date.now() - t0 };
      } catch (_) {
        return { url, ms: null };
      }
    })
  );
  const ok = times.filter((t) => t.ms != null).sort((a, b) => a.ms - b.ms);
  const bad = times.filter((t) => t.ms == null);
  return { urls: [...ok.map((t) => t.url), ...bad.map((t) => t.url)], times };
}

function shortUrl(url) {
  try {
    const u = new URL(url);
    return u.host;
  } catch (_) {
    return String(url).slice(0, 24);
  }
}

function promiseWithTimeout(promise, ms) {
  let timer;
  return Promise.race([
    Promise.resolve(promise).finally(() => clearTimeout(timer)),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`RPC stall ${ms}ms`)), ms);
    }),
  ]);
}

/**
 * StaticJsonRpcProvider that fails over on ERROR or true hang:
 * 1) try primary with a generous hang timeout (default max(stall, 2500))
 * 2) race remaining backups — first success wins
 *
 * Note: stall must be ABOVE normal RTT for heavy calls (getBlock/multicall).
 * Too-low values (e.g. 400ms on a 300ms public RPC) abort healthy primaries
 * and make everything slower.
 */
class FastFailoverProvider extends ethers.providers.StaticJsonRpcProvider {
  constructor(urls, chainId, stallMs) {
    super(urls[0], chainId);
    this._failoverUrls = urls;
    // Hang timeout: never below 2s for full JSON-RPC payloads on public endpoints.
    this._stallMs = Math.max(stallMs || 400, 2000);
    this._spares = urls.slice(1).map((url) => new ethers.providers.StaticJsonRpcProvider(url, chainId));
  }

  async send(method, params) {
    try {
      return await promiseWithTimeout(super.send(method, params), this._stallMs);
    } catch (primaryErr) {
      if (this._spares.length === 0) throw primaryErr;
      try {
        return await raceFirstSuccess(this._spares.map((p) => p.send(method, params)));
      } catch (_) {
        throw primaryErr;
      }
    }
  }
}

function raceFirstSuccess(promises) {
  if (typeof Promise.any === "function") return Promise.any(promises);
  return new Promise((resolve, reject) => {
    let left = promises.length;
    const errors = [];
    if (left === 0) {
      reject(new Error("no backups"));
      return;
    }
    for (const p of promises) {
      Promise.resolve(p).then(resolve, (err) => {
        errors.push(err);
        if (--left === 0) reject(errors[0] || new Error("all backups failed"));
      });
    }
  });
}

function resolveStallMs(chainConfig = {}) {
  const key = String(chainConfig.key || "").toUpperCase();
  const raw =
    (key && process.env[`${key}_RPC_STALL_MS`]) ||
    process.env.RPC_STALL_MS ||
    "800"; // used for ranking preference; hang failover floor is max(this, 2000)
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : 400;
}

function createBlockProvider(chainConfig) {
  const wsUrl = getWsUrl(chainConfig);
  if (!wsUrl) return null;
  return new ethers.providers.WebSocketProvider(wsUrl, chainConfig.chainId);
}

function getWsUrl(chainConfig = {}) {
  const upperKey = String(chainConfig.key || "").toUpperCase();
  const explicitGlobalChain = process.env.CHAIN
    ? String(process.env.CHAIN).toLowerCase().replace(/[\s_-]+/g, "")
    : null;
  const globalOverridesEnabled = !process.env.CHAINS && explicitGlobalChain === chainConfig.key;

  const url =
    (upperKey && process.env[`${upperKey}_WS_URL`]) ||
    (globalOverridesEnabled && process.env.WS_URL) ||
    chainConfig.wsUrl ||
    "";

  const trimmed = String(url).trim();
  return trimmed || null;
}

function getRpcUrls(chainConfig) {
  const urls = chainConfig.rpcUrls || [chainConfig.rpcUrl];
  return [...new Set(urls.map((url) => String(url).trim()).filter(Boolean))];
}

module.exports = {
  createProvider,
  createRankedProvider,
  warmRpcRankings,
  rankRpcUrls,
  createBlockProvider,
  getRpcUrls,
  getWsUrl,
  resolveStallMs,
  FastFailoverProvider,
};
