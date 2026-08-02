const { ethers } = require("ethers");
const { computePriorityFeeWei } = require("../src/gas");

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const chain = { key: "base", liquidationGasLimit: 2_000_000 };
const baseFee = ethers.utils.parseUnits("0.01", "gwei"); // quiet L2
const bump = ethers.constants.Zero;
const gasLimit = 2_000_000;

// Quiet chain + large profit → opportunity tip (per gas) = budget/gasLimit.
{
  process.env.PROFIT_GUIDED_TIP = "true";
  process.env.PROFIT_TIP_FRACTION = "0.01";
  process.env.PROFIT_TIP_MAX_FRACTION = "0.10";
  process.env.MIN_PRIORITY_FEE_GWEI = "0.05";
  process.env.MAX_PRIORITY_FEE_GWEI = "0.2";
  process.env.PRIORITY_FEE_MULTIPLE = "3";

  const profit = ethers.utils.parseEther("0.5"); // 0.5 ETH est bonus
  const tip = computePriorityFeeWei(chain, {
    baseFee, bump, estimatedProfitNative: profit, gasLimit,
  });
  const budget = profit.mul(10).div(1000); // 1% = 0.005 ETH total tip budget
  const expectedPerGas = budget.div(gasLimit);
  // Tip should be near opportunity per-gas (above tiny congestion floor).
  assert(tip.gte(expectedPerGas), `expected tip >= ${expectedPerGas}, got ${tip}`);
  const totalTip = tip.mul(gasLimit);
  assert(totalTip.lte(profit.mul(100).div(1000)), "total tip must be <= 10% of profit");
  assert(parseFloat(ethers.utils.formatUnits(tip, "gwei")) < 100, "per-gas tip must stay sane (<100 gwei)");
  console.log("  quiet+big profit tip:", ethers.utils.formatUnits(tip, "gwei"), "gwei/gas",
    `| total≈${ethers.utils.formatEther(totalTip)} ETH`);
}

// No profit signal → tip stays within static cap.
{
  const tip = computePriorityFeeWei(chain, {
    baseFee, bump, estimatedProfitNative: null, gasLimit,
  });
  const staticCap = ethers.utils.parseUnits("0.2", "gwei");
  assert(tip.lte(staticCap.add(bump)), `expected tip <= static cap, got ${tip}`);
  console.log("  no-profit tip:", ethers.utils.formatUnits(tip, "gwei"), "gwei");
}

// Never tip away more than max fraction of profit (as TOTAL cost).
{
  process.env.PROFIT_TIP_FRACTION = "0.50";
  process.env.PROFIT_TIP_MAX_FRACTION = "0.10";
  process.env.MAX_PRIORITY_FEE_GWEI = "100";
  const profit = ethers.utils.parseEther("0.1");
  const tip = computePriorityFeeWei(chain, {
    baseFee: ethers.utils.parseUnits("5", "gwei"),
    bump,
    estimatedProfitNative: profit,
    gasLimit,
  });
  const totalTip = tip.mul(gasLimit);
  const maxBudget = profit.mul(100).div(1000); // 10%
  assert(totalTip.lte(maxBudget.add(1)), `total tip ${totalTip} exceeded max budget ${maxBudget}`);
  console.log("  max-fraction total tip:", ethers.utils.formatEther(totalTip), "ETH");
}

console.log("✅ profit-guided gas tip OK");
