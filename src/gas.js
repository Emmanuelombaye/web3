const { ethers } = require("ethers");

// Per-chain numeric env resolver: <CHAIN>_<NAME> → <NAME> → fallback.
function resolveChainNum(chainConfig, name, fallback) {
  const key = (chainConfig.key || "").toUpperCase();
  const candidates = [key ? process.env[`${key}_${name}`] : undefined, process.env[name]];
  for (const raw of candidates) {
    if (raw === undefined || raw === null || raw === "") continue;
    const v = parseFloat(raw);
    if (Number.isFinite(v) && v >= 0) return v;
  }
  return fallback;
}

function gweiToWei(gwei) {
  const fixed = Number(gwei).toFixed(9);
  return ethers.utils.parseUnits(fixed, "gwei");
}

async function safeFeeData(provider) {
  try {
    return await getFeeDataFast(provider);
  } catch (_) {
    return null;
  }
}

// Fast fee read: ONE getBlock("latest") for baseFee (+ block number) and
// gasPrice in parallel. ethers.provider.getFeeData() often issues 2–3 sequential
// RPCs (~900ms on public endpoints). Cache briefly so burst attempts reuse it.
const feeDataCache = new Map(); // key -> { at, feeData, blockNumber }

function feeCacheKey(provider, chainConfig) {
  return (chainConfig && chainConfig.key) || provider?.connection?.url || "default";
}

function feeDataTtlMs() {
  const n = parseInt(process.env.FEE_DATA_TTL_MS || "1500", 10);
  return Number.isFinite(n) && n >= 0 ? n : 1500;
}

async function getFeeDataFast(provider, chainConfig = {}) {
  const key = feeCacheKey(provider, chainConfig);
  const ttl = feeDataTtlMs();
  const hit = feeDataCache.get(key);
  if (ttl > 0 && hit && Date.now() - hit.at < ttl) {
    return hit.feeData;
  }

  // ONE RPC: latest block carries baseFeePerGas on all our L2s. Skipping a
  // parallel eth_gasPrice halves the fee-read latency (was ~900ms on public RPCs).
  const block = await provider.getBlock("latest");
  const baseFee = block && block.baseFeePerGas ? block.baseFeePerGas : null;
  const feeData = {
    gasPrice: baseFee || null,
    lastBaseFeePerGas: baseFee,
    maxFeePerGas: baseFee ? baseFee.mul(2) : null,
    maxPriorityFeePerGas: null,
  };

  feeDataCache.set(key, {
    at: Date.now(),
    feeData,
    blockNumber: block && block.number != null ? block.number : null,
  });
  return feeData;
}

function getCachedBlockNumber(provider, chainConfig = {}) {
  const hit = feeDataCache.get(feeCacheKey(provider, chainConfig));
  return hit && hit.blockNumber != null ? hit.blockNumber : null;
}

/**
 * Compute EIP-1559 priority fee (per gas) for a liquidation.
 *
 * Winning contested L2 races needs BOTH:
 *   - congestion tip (scales with base fee)
 *   - opportunity tip (spend a fraction of estimated profit on inclusion)
 *
 * Opportunity tip is a TOTAL tip budget (profit × fraction), converted to
 * per-gas: budget / gasLimit. Treating profit×fraction as per-gas would
 * overpay by ~gasLimit× and bankrupt the wallet.
 *
 * priorityFee = max(congestionTip, opportunityTipPerGas)
 * then clamped so total tip (fee × gasLimit) never exceeds
 * profit × PROFIT_TIP_MAX_FRACTION (default 10%).
 */
function computePriorityFeeWei(chainConfig, {
  baseFee,
  bump,
  estimatedProfitNative,
  gasLimit,
} = {}) {
  const priorityMult = resolveChainNum(chainConfig, "PRIORITY_FEE_MULTIPLE", 3);
  const floorTip = gweiToWei(resolveChainNum(chainConfig, "MIN_PRIORITY_FEE_GWEI", 0.05));
  const staticCap = gweiToWei(resolveChainNum(chainConfig, "MAX_PRIORITY_FEE_GWEI", 2.25));
  const bumpWei = bump && ethers.BigNumber.isBigNumber(bump) ? bump : ethers.constants.Zero;
  const gasLimitBn = ethers.BigNumber.from(
    gasLimit || chainConfig.liquidationGasLimit || 2_000_000
  );

  // Congestion component: baseFee × multiple, floored.
  let congestionTip = baseFee.mul(Math.round(priorityMult * 100)).div(100);
  if (congestionTip.lt(floorTip)) congestionTip = floorTip;

  // Opportunity component: bid a fraction of estimated profit as TOTAL tip budget,
  // then convert to per-gas for the EIP-1559 field.
  const profitGuidedEnabled = (process.env.PROFIT_GUIDED_TIP || "true") !== "false";
  let opportunityTip = ethers.constants.Zero;
  let profitWei = null;

  if (profitGuidedEnabled && estimatedProfitNative) {
    try {
      profitWei = ethers.BigNumber.isBigNumber(estimatedProfitNative)
        ? estimatedProfitNative
        : ethers.utils.parseEther(String(estimatedProfitNative));
      if (profitWei.gt(0) && gasLimitBn.gt(0)) {
        const tipFractionX1000 = Math.round(
          parseFloat(process.env.PROFIT_TIP_FRACTION || "0.01") * 1000
        ); // default 1% of estimated bonus as TOTAL tip budget
        const tipBudget = profitWei.mul(Math.max(tipFractionX1000, 0)).div(1000);
        opportunityTip = tipBudget.div(gasLimitBn);
      }
    } catch (_) {
      profitWei = null;
      opportunityTip = ethers.constants.Zero;
    }
  }

  // Take the stronger of congestion vs opportunity (per gas).
  let priorityFee = congestionTip.gte(opportunityTip) ? congestionTip : opportunityTip;

  // Hard ceiling on TOTAL tip paid ≈ priorityFee × gasLimit.
  if (profitWei && profitWei.gt(0) && gasLimitBn.gt(0)) {
    const maxFracX1000 = Math.round(
      parseFloat(process.env.PROFIT_TIP_MAX_FRACTION || "0.10") * 1000
    ); // default 10% of profit
    const maxBudget = profitWei.mul(Math.max(maxFracX1000, 0)).div(1000);
    const profitPerGasCap = maxBudget.div(gasLimitBn);
    // Allow opportunity bidding above the static gwei cap, but never above the
    // profit-derived per-gas cap. Also never below needing the static cap when
    // profit signal is tiny.
    const effectiveCap = profitPerGasCap.gt(staticCap) ? profitPerGasCap : staticCap;
    if (priorityFee.gt(effectiveCap)) priorityFee = effectiveCap;
    if (priorityFee.gt(profitPerGasCap) && profitPerGasCap.gt(0)) {
      priorityFee = profitPerGasCap;
    }
  } else if (staticCap.gt(0) && priorityFee.gt(staticCap)) {
    priorityFee = staticCap;
  }

  if (priorityFee.lt(floorTip)) priorityFee = floorTip;
  return priorityFee.add(bumpWei);
}

// Builds the tx overrides for a liquidation. Prefers EIP-1559.
async function getTransactionOverrides(provider, chainConfig, options = {}) {
  const gasLimit = options.gasLimit || chainConfig.liquidationGasLimit;
  const bumpGwei = chainConfig.gasPriceBumpGwei || "0";
  const bump = ethers.utils.parseUnits(bumpGwei, "gwei");

  const forceLegacy = options.legacy === true || chainConfig.forceLegacyGas === true;
  const feeData =
    options.feeData ||
    (forceLegacy ? null : await getFeeDataFast(provider, chainConfig));

  const baseFee = feeData && feeData.lastBaseFeePerGas;
  const supports1559 = !forceLegacy && feeData && feeData.maxFeePerGas && baseFee;

  if (supports1559) {
    const baseMultX100 = Math.round(resolveChainNum(chainConfig, "BASE_FEE_MULTIPLE", 2) * 100);
    const priorityFee = computePriorityFeeWei(chainConfig, {
      baseFee,
      bump,
      estimatedProfitNative: options.estimatedProfitNative,
      gasLimit,
    });
    const maxFeePerGas = baseFee.mul(baseMultX100).div(100).add(priorityFee);

    return {
      gasLimit,
      maxFeePerGas,
      maxPriorityFeePerGas: priorityFee,
    };
  }

  const gasPrice = options.gasPrice || (feeData && feeData.gasPrice) || (await provider.getGasPrice());
  return {
    gasLimit,
    gasPrice: gasPrice.add(bump),
  };
}

async function getGasSnapshot(provider, chainConfig) {
  const [network, blockNumber, gasPrice, feeData] = await Promise.all([
    provider.getNetwork(),
    provider.getBlockNumber(),
    provider.getGasPrice(),
    provider.getFeeData(),
  ]);

  const liquidationGasLimit = ethers.BigNumber.from(chainConfig.liquidationGasLimit);
  const deployGasLimit = ethers.BigNumber.from(1_600_000);

  return {
    chainId: network.chainId,
    blockNumber,
    nativeToken: chainConfig.nativeToken,
    gasPrice,
    gasPriceGwei: ethers.utils.formatUnits(gasPrice, "gwei"),
    feeData: {
      gasPrice: feeData.gasPrice ? ethers.utils.formatUnits(feeData.gasPrice, "gwei") : null,
      maxFeePerGas: feeData.maxFeePerGas ? ethers.utils.formatUnits(feeData.maxFeePerGas, "gwei") : null,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas
        ? ethers.utils.formatUnits(feeData.maxPriorityFeePerGas, "gwei")
        : null,
    },
    estimates: {
      deployGasLimit: deployGasLimit.toString(),
      deployCostNative: ethers.utils.formatEther(deployGasLimit.mul(gasPrice)),
      liquidationGasLimit: liquidationGasLimit.toString(),
      liquidationCostNative: ethers.utils.formatEther(liquidationGasLimit.mul(gasPrice)),
    },
  };
}

module.exports = {
  getGasSnapshot,
  getTransactionOverrides,
  computePriorityFeeWei,
  resolveChainNum,
  getFeeDataFast,
  getCachedBlockNumber,
};
