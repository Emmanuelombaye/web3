const { ethers } = require("ethers");

// Mirror the helpers in bot.js (kept local so this test stays offline).
function basketDebtScore(basket) {
  const decimals = Number.isFinite(basket.debtDecimals) ? basket.debtDecimals : 18;
  return parseFloat(ethers.utils.formatUnits(basket.debtAmount, decimals));
}

function sortBasketsByDebtDesc(baskets) {
  return [...baskets].sort((a, b) => basketDebtScore(b) - basketDebtScore(a));
}

function sortLiquidationTargets(items) {
  return [...items].sort((a, b) => {
    const debtA = Number.isFinite(a.totalDebtUsd) ? a.totalDebtUsd : 0;
    const debtB = Number.isFinite(b.totalDebtUsd) ? b.totalDebtUsd : 0;
    if (debtB !== debtA) return debtB - debtA;
    const hfA = Number.isFinite(a.healthFactor) ? a.healthFactor : Infinity;
    const hfB = Number.isFinite(b.healthFactor) ? b.healthFactor : Infinity;
    return hfA - hfB;
  });
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

// Mixed decimals: 5000 USDC (6dp) must rank above 1 WETH (18dp) when comparing
// human units (raw BigNumber would wrongly prefer 1e18 > 5e9).
{
  const baskets = [
    { debtSymbol: "WETH", debtDecimals: 18, debtAmount: ethers.utils.parseUnits("1", 18) },
    { debtSymbol: "USDC", debtDecimals: 6, debtAmount: ethers.utils.parseUnits("5000", 6) },
  ];
  const sorted = sortBasketsByDebtDesc(baskets);
  assert(sorted[0].debtSymbol === "USDC", `expected USDC first, got ${sorted[0].debtSymbol}`);
}

{
  const targets = [
    { user: "a", totalDebtUsd: 200, healthFactor: 0.99 },
    { user: "b", totalDebtUsd: 5000, healthFactor: 0.98 },
    { user: "c", totalDebtUsd: 5000, healthFactor: 0.90 },
  ];
  const sorted = sortLiquidationTargets(targets);
  assert(sorted[0].user === "c", "same debt → lower HF first");
  assert(sorted[1].user === "b", "same debt → higher HF second");
  assert(sorted[2].user === "a", "smallest debt last");
}

console.log("✅ basket/target sort OK");
