const PMC_PER_PI = 1000;
const ADMIN_WALLET_KEY = "pi_admin_master";
const ALLOW_FEE_FRACTION_TEST = true; // test tạm, xong nhớ đổi lại false

module.exports = async function handler(req, res) {
  let stage = "start";

  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({
      ok: false,
      error: "Method not allowed"
    });
  }

  let getDatabase;
  let adminApp;

  try {
    const adminBundle = require("./_firebaseAdmin.js");
    ({ getDatabase } = require("firebase-admin/database"));
    adminApp = adminBundle.app;
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: "load _firebaseAdmin failed: " + (e?.message || String(e))
    });
  }

  try {
    stage = "read-env";
    const allowedPiKeys = String(process.env.ADMIN_ALLOWED_PI_KEYS || "")
      .split(",")
      .map(s => s.trim().toLowerCase())
      .filter(Boolean);

    const secretFromEnv = String(process.env.ADMIN_FEE_SECRET || "").trim();

    if (!allowedPiKeys.length) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu ADMIN_ALLOWED_PI_KEYS trên Vercel."
      });
    }

    if (!secretFromEnv) {
      return res.status(500).json({
        ok: false,
        error: "Thiếu ADMIN_FEE_SECRET trên Vercel."
      });
    }

    stage = "read-wallet-key";
    const clientWalletKey = String(req.headers["x-wallet-key"] || "").trim().toLowerCase();

    if (!clientWalletKey) {
      return res.status(401).json({
        ok: false,
        error: "Thiếu định danh ví Pi admin."
      });
    }

    if (!allowedPiKeys.includes(clientWalletKey)) {
      return res.status(403).json({
        ok: false,
        error: "Ví Pi này không có quyền thao tác ví phí hệ thống."
      });
    }

    stage = "get-db";
    const db = getDatabase(adminApp);

    stage = "build-wallet-path";
    const safeWalletKey = String(ADMIN_WALLET_KEY).replace(/[.#$\[\]\/]/g, "_");
    const walletRef = db.ref("wallets/" + safeWalletKey);

    if (req.method === "GET") {
      stage = "read-wallet";
      const snap = await walletRef.once("value");
      const data = snap.val() && typeof snap.val() === "object" ? snap.val() : {};

      const pmcBalance = Math.floor(Number(data.pmcBalance ?? 0) || 0);
      const piBalance =
        Number(
          data.balance != null
            ? data.balance
            : (data.piBalance != null ? data.piBalance : 0)
        ) || 0;

      return res.status(200).json({
        ok: true,
        walletKey: safeWalletKey,
        pmcBalance,
        piBalance,
        rate: PMC_PER_PI
      });
    }

    stage = "read-body";
    const { pmcAmount, adminSecret } = req.body || {};
    const safePmc = Math.max(0, Math.floor(Number(pmcAmount || 0) || 0));
    const safeAdminSecret = String(adminSecret || "").trim();

    if (!safeAdminSecret) {
      return res.status(401).json({
        ok: false,
        error: "Thiếu mã bí mật admin."
      });
    }

    if (safeAdminSecret !== secretFromEnv) {
      return res.status(403).json({
        ok: false,
        error: "Mã bí mật admin không đúng."
      });
    }

    if (!safePmc || safePmc <= 0) {
      return res.status(400).json({
        ok: false,
        error: "Nhập số PMC muốn đổi trước đã."
      });
    }

    if (!ALLOW_FEE_FRACTION_TEST && safePmc % PMC_PER_PI !== 0) {
  return res.status(400).json({
    ok: false,
    error: `PMC phải chia hết cho ${PMC_PER_PI}.`
  });
}

    stage = "pre-read-wallet";
    const preSnap = await walletRef.once("value");
    const preRead = preSnap.val();

    stage = "transaction";
    let exchangeResult = null;

    const txResult = await walletRef.transaction(current => {
      const baseCurrent =
        current && typeof current === "object"
          ? current
          : (preRead && typeof preRead === "object" ? preRead : {});

      const currentPi =
        Number(
          baseCurrent.balance != null
            ? baseCurrent.balance
            : (baseCurrent.piBalance != null ? baseCurrent.piBalance : 0)
        ) || 0;

      const currentPmc = Math.floor(Number(baseCurrent.pmcBalance ?? 0) || 0);

      if (currentPmc < safePmc) {
        return;
      }

      const piAmount = safePmc / PMC_PER_PI;
      const newPmcBalance = currentPmc - safePmc;
      const newPiBalance = currentPi + piAmount;

      exchangeResult = {
        piAmount,
        newPmcBalance,
        newPiBalance,
        oldPmcBalance: currentPmc,
        oldPiBalance: currentPi
      };

      return {
        ...baseCurrent,
        balance: newPiBalance,
        piBalance: newPiBalance,
        pmcBalance: newPmcBalance,
        role: baseCurrent.role || "admin",
        name: baseCurrent.name || "Ví phí hệ thống",
        photo: baseCurrent.photo || "images/do_tuong.png",
        updatedAt: Date.now()
      };
    });

    if (!exchangeResult || !txResult?.committed) {
      return res.status(400).json({
        ok: false,
        error: "PMC phí hệ thống không đủ hoặc giao dịch không hợp lệ."
      });
    }

    stage = "write-history";
    await db.ref("walletTransactions").push({
      type: "admin_fee_pmc_to_pi",
      walletKey: safeWalletKey,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done",
      byWalletKey: clientWalletKey
    });

    await db.ref("adminTreasuryConversions").push({
      type: "admin_fee_pmc_to_pi",
      walletKey: safeWalletKey,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      oldPmcBalance: exchangeResult.oldPmcBalance,
      oldPiBalance: exchangeResult.oldPiBalance,
      newPmcBalance: exchangeResult.newPmcBalance,
      newPiBalance: exchangeResult.newPiBalance,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done",
      byWalletKey: clientWalletKey
    });

    stage = "done";
    return res.status(200).json({
      ok: true,
      walletKey: safeWalletKey,
      pmcAmount: safePmc,
      piAmount: exchangeResult.piAmount,
      newPmcBalance: exchangeResult.newPmcBalance,
      newPiBalance: exchangeResult.newPiBalance
    });
  } catch (err) {
    console.error("convert-fee-pmc-to-pi crash stage =", stage, err);

    return res.status(500).json({
      ok: false,
      error: err?.message || "Server error"
    });
  }
};