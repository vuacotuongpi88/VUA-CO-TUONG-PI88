const admin = require("../_firebaseAdmin.js");

const ADMIN_WALLET_KEY = "pi_admin_master";
const PMC_PER_PI = 1000;

function safeWalletKey(walletKey) {
  return String(walletKey || "").replace(/[.#$\[\]\/]/g, "_");
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "method_not_allowed"
    });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body || "{}")
        : (req.body || {});

    const pmcAmount = Math.max(0, Math.floor(Number(body.pmcAmount || 0) || 0));

    if (!pmcAmount || pmcAmount <= 0) {
      return res.status(400).json({
        ok: false,
        error: "invalid_pmcAmount"
      });
    }

    if (pmcAmount % PMC_PER_PI !== 0) {
      return res.status(400).json({
        ok: false,
        error: `pmcAmount_phai_chia_het_cho_${PMC_PER_PI}`
      });
    }

    const db = admin.database();
    const adminWalletRef = db.ref("wallets/" + safeWalletKey(ADMIN_WALLET_KEY));

    let convertResult = null;

    const txResult = await adminWalletRef.transaction(current => {
      const safeCurrent = current && typeof current === "object" ? current : {};

      const currentPmc = Math.floor(Number(safeCurrent.pmcBalance || 0) || 0);
      const currentPi = Number(safeCurrent.piBalance || 0) || 0;

      if (currentPmc < pmcAmount) {
        return;
      }

      const piAmount = pmcAmount / PMC_PER_PI;
      const newPmcBalance = currentPmc - pmcAmount;
      const newPiBalance = currentPi + piAmount;

      convertResult = {
        walletKey: ADMIN_WALLET_KEY,
        pmcAmount,
        piAmount,
        oldPmcBalance: currentPmc,
        oldPiBalance: currentPi,
        newPmcBalance,
        newPiBalance
      };

      return {
        ...safeCurrent,
        pmcBalance: newPmcBalance,
        piBalance: newPiBalance,
        role: safeCurrent.role || "admin",
        name: safeCurrent.name || "Ví phí hệ thống",
        updatedAt: Date.now()
      };
    });

    if (!txResult.committed || !convertResult) {
      return res.status(400).json({
        ok: false,
        error: "pmc_admin_khong_du_de_doi"
      });
    }

    await db.ref("adminTreasuryConversions").push({
      type: "admin_fee_pmc_to_internal_pi",
      walletKey: ADMIN_WALLET_KEY,
      pmcAmount: convertResult.pmcAmount,
      piAmount: convertResult.piAmount,
      rate: PMC_PER_PI,
      oldPmcBalance: convertResult.oldPmcBalance,
      oldPiBalance: convertResult.oldPiBalance,
      newPmcBalance: convertResult.newPmcBalance,
      newPiBalance: convertResult.newPiBalance,
      createdAt: Date.now(),
      status: "done"
    });

    await db.ref("walletTransactions").push({
      type: "admin_fee_pmc_to_internal_pi",
      walletKey: ADMIN_WALLET_KEY,
      pmcAmount: convertResult.pmcAmount,
      piAmount: convertResult.piAmount,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done"
    });

    return res.status(200).json({
      ok: true,
      walletKey: ADMIN_WALLET_KEY,
      pmcAmount: convertResult.pmcAmount,
      piAmount: convertResult.piAmount,
      newPmcBalance: convertResult.newPmcBalance,
      newPiBalance: convertResult.newPiBalance
    });
  } catch (err) {
    console.error("convert admin fee pmc -> pi error =", err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "server_error"
    });
  }
};