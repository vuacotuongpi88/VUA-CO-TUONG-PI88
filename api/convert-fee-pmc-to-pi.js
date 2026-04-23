const PMC_PER_PI = 500;
const ADMIN_WALLET_KEY = "pi_admin_master";
const MIN_ADMIN_FEE_PMC_WITHDRAW = 1000;

function safeKey(value) {
  return String(value || "").replace(/[.#$\[\]\/]/g, "_");
}

function readPiBalance(obj) {
  return Number(
    obj && obj.balance != null
      ? obj.balance
      : (obj && obj.piBalance != null ? obj.piBalance : 0)
  ) || 0;
}

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
  adminApp = adminBundle.app || adminBundle;
} catch (e) {
  return res.status(500).json({
    ok: false,
    error: "load_firebaseAdmin failed. " + (e?.message || String(e))
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
    const clientWalletKeyRaw = String(req.headers["x-wallet-key"] || "").trim().toLowerCase();

    if (!clientWalletKeyRaw) {
      return res.status(401).json({
        ok: false,
        error: "Thiếu định danh ví Pi admin."
      });
    }

    if (!allowedPiKeys.includes(clientWalletKeyRaw)) {
      return res.status(403).json({
        ok: false,
        error: "Ví Pi này không có quyền thao tác ví phí hệ thống."
      });
    }

    stage = "get-db";
    const db = getDatabase(adminApp);

    const treasuryWalletKey = safeKey(ADMIN_WALLET_KEY);
    const targetWalletKey = safeKey(clientWalletKeyRaw);

    const treasuryRef = db.ref("wallets/" + treasuryWalletKey);
    const targetRef = db.ref("wallets/" + targetWalletKey);

  if (req.method === "GET") {
  stage = "get-balances";

  const systemWalletKey = safeKey(ADMIN_WALLET_KEY);
  const currentAdminWalletKey = safeKey(
    req.headers["x-wallet-key"] || ADMIN_WALLET_KEY
  );

  const [systemSnap, adminSnap] = await Promise.all([
    db.ref("wallets/" + systemWalletKey).once("value"),
    db.ref("wallets/" + currentAdminWalletKey).once("value")
  ]);

  const systemWallet = systemSnap.val() || {};
  const adminWallet = adminSnap.val() || {};

  return res.status(200).json({
    ok: true,
    systemWalletKey: ADMIN_WALLET_KEY,
    currentAdminWalletKey,
    pmcBalance: Math.max(
      0,
      Math.floor(Number(systemWallet.pmcBalance || 0) || 0)
    ),
    piBalance: Number(
      adminWallet.balance != null
        ? adminWallet.balance
        : (adminWallet.piBalance || 0)
    ) || 0
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

    if (safePmc < MIN_ADMIN_FEE_PMC_WITHDRAW) {
  return res.status(400).json({
    ok: false,
    error: `Mức rút tối thiểu là ${MIN_ADMIN_FEE_PMC_WITHDRAW.toLocaleString("vi-VN")} PMC.`
  });
}

    stage = "read-treasury-before";
    const treasuryPreSnap = await treasuryRef.once("value");
    const treasuryPreRead =
      treasuryPreSnap.val() && typeof treasuryPreSnap.val() === "object"
        ? treasuryPreSnap.val()
        : {};

    stage = "debit-treasury-pmc";
    let treasuryResult = null;

    const treasuryTx = await treasuryRef.transaction(current => {
      const baseCurrent =
        current && typeof current === "object" ? current : treasuryPreRead;

      const currentPmc = Math.floor(Number(baseCurrent.pmcBalance ?? 0) || 0);
      if (currentPmc < safePmc) {
        return;
      }

      const newTreasuryPmcBalance = currentPmc - safePmc;
      const piAmount = safePmc / PMC_PER_PI;

      treasuryResult = {
        piAmount,
        oldTreasuryPmcBalance: currentPmc,
        newTreasuryPmcBalance
      };

      return {
        ...baseCurrent,
        pmcBalance: newTreasuryPmcBalance,
        updatedAt: Date.now()
      };
    });

    if (!treasuryResult || !treasuryTx?.committed) {
      return res.status(400).json({
        ok: false,
        error: "PMC phí hệ thống không đủ hoặc giao dịch không hợp lệ."
      });
    }

    stage = "read-target-before";
    const targetPreSnap = await targetRef.once("value");
    const targetPreRead =
      targetPreSnap.val() && typeof targetPreSnap.val() === "object"
        ? targetPreSnap.val()
        : {};

    stage = "credit-player-pi";
    let playerResult = null;

    const playerTx = await targetRef.transaction(current => {
      const baseCurrent =
        current && typeof current === "object" ? current : targetPreRead;

      const currentPi = readPiBalance(baseCurrent);
      const newPlayerPiBalance = currentPi + treasuryResult.piAmount;

      playerResult = {
        oldPlayerPiBalance: currentPi,
        newPlayerPiBalance
      };

      return {
        ...baseCurrent,
        balance: newPlayerPiBalance,
        piBalance: newPlayerPiBalance,
        updatedAt: Date.now()
      };
    });

    if (!playerResult || !playerTx?.committed) {
      stage = "rollback-treasury";
      await treasuryRef.transaction(current => {
        const baseCurrent =
          current && typeof current === "object" ? current : treasuryPreRead;

        const currentPmc = Math.floor(Number(baseCurrent.pmcBalance ?? 0) || 0);

        return {
          ...baseCurrent,
          pmcBalance: currentPmc + safePmc,
          updatedAt: Date.now()
        };
      });

      return res.status(500).json({
        ok: false,
        error: "Cộng Pi vào tài khoản admin thất bại, đã hoàn PMC về ví phí."
      });
    }

    stage = "write-history";
    await db.ref("walletTransactions").push({
      type: "admin_fee_pmc_to_player_pi",
      treasuryWalletKey,
      targetWalletKey,
      pmcAmount: safePmc,
      piAmount: treasuryResult.piAmount,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done",
      byWalletKey: clientWalletKeyRaw
    });

    await db.ref("adminTreasuryConversions").push({
      type: "admin_fee_pmc_to_player_pi",
      treasuryWalletKey,
      targetWalletKey,
      pmcAmount: safePmc,
      piAmount: treasuryResult.piAmount,
      oldTreasuryPmcBalance: treasuryResult.oldTreasuryPmcBalance,
      newTreasuryPmcBalance: treasuryResult.newTreasuryPmcBalance,
      oldPlayerPiBalance: playerResult.oldPlayerPiBalance,
      newPlayerPiBalance: playerResult.newPlayerPiBalance,
      rate: PMC_PER_PI,
      createdAt: Date.now(),
      status: "done",
      byWalletKey: clientWalletKeyRaw
    });

    stage = "done";
    return res.status(200).json({
      ok: true,
      treasuryWalletKey,
      targetWalletKey,
      pmcAmount: safePmc,
      piAmount: treasuryResult.piAmount,
      newTreasuryPmcBalance: treasuryResult.newTreasuryPmcBalance,
      newPlayerPiBalance: playerResult.newPlayerPiBalance
    });
  } catch (err) {
    console.error("convert-fee-pmc-to-pi crash stage =", stage, err);

    return res.status(500).json({
      ok: false,
      error: err?.message || "Server error"
    });
  }
};